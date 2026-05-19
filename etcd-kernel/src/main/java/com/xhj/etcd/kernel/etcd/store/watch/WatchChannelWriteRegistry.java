package com.xhj.etcd.kernel.etcd.store.watch;

import com.xhj.etcd.rpc.RpcMessage;
import com.xhj.etcd.rpc.RpcMessageType;
import com.xhj.etcd.serializer.Serializer;
import io.netty.channel.Channel;
import io.netty.channel.ChannelId;

import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * WatchChannelWriteRegistry
 *
 * @author XJks
 * @description Watch 消息发送中介队列。
 *
 * <p>
 * TODO:
 *  同一 channel 上的 watch subscribe RESPONSE 与后续 STREAM 都通过该写队列串行发送，
 *  以应用层方式收敛写顺序，避免多个业务线程直接并发 writeAndFlush 造成顺序语义不稳定。
 * </p>
 */
public class WatchChannelWriteRegistry {

    /**
     * 单次 drain 最大发送条数。
     *
     * <p>
     * TODO:
     *  分批 drain，避免同一连接上的多路 watch 交错消息在短时间内大量堆积后一次性长循环发送，
     *  进而长时间占用该 channel 的 eventLoop，导致同一连接上的其他 IO 事件得不到及时调度。
     * </p>
     */
    private static final int MAX_MESSAGES_PER_DRAIN = 256;

    /**
     * RPC 序列化器。
     */
    private final Serializer serializer;

    /**
     * channelId -> 单 channel 写路由状态。
     */
    private final ConcurrentMap<ChannelId, WatchChannelWriteQueueState> watchChannelWriteQueueStateByChannelId = new ConcurrentHashMap<>();

    public WatchChannelWriteRegistry(Serializer serializer) {
        if (serializer == null) {
            throw new IllegalArgumentException("serializer must not be null");
        }
        this.serializer = serializer;
    }

    /**
     * 入队一条 watch 消息。
     *
     * @param channel      目标连接
     * @param rpcMessageId RPC 消息 ID
     * @param messageType  消息类型
     * @param payload      负载对象
     * @return true 表示入队成功
     */
    public boolean enqueue(Channel channel, String rpcMessageId, RpcMessageType messageType, Object payload) {
        if (channel == null || !channel.isActive()) {
            return false;
        }
        if (rpcMessageId == null || rpcMessageId.trim().isEmpty()) {
            return false;
        }
        if (messageType == null || payload == null) {
            return false;
        }

        ChannelId channelId = channel.id();
        WatchChannelWriteQueueState state = watchChannelWriteQueueStateByChannelId.computeIfAbsent(channelId, id -> createWatchChannelWriteQueueState(channel));
        if (state.getChannel() != channel) {
            return false;
        }

        RpcMessage rpcMessage = new RpcMessage();
        rpcMessage.setType(messageType);
        rpcMessage.setRpcMessageId(rpcMessageId);
        rpcMessage.setData(serializer.serialize(payload));
        state.getOutboundQueue().offer(rpcMessage);

        /**
         * TODO:
         *  生产者线程只负责入队和调度，不直接做 write。
         *  当前阶段主要是两类线程会调用 enqueue：
         *  1) EtcdNode 的 apply/event-loop 线程：apply 成功后推送 watch STREAM。
         *  2) RpcRequestExecutor 线程池线程：处理 watch subscribe RPC 时入队首帧 RESPONSE。
         *  两类线程都可能并发入队，因此发送必须统一收敛到 channel.eventLoop 单写者模型。
         */
        scheduleDrain(state);
        return true;
    }

    /**
     * 清理全部连接发送状态。
     */
    public void clear() {
        watchChannelWriteQueueStateByChannelId.clear();
    }

    private WatchChannelWriteQueueState createWatchChannelWriteQueueState(Channel channel) {
        WatchChannelWriteQueueState state = new WatchChannelWriteQueueState();
        state.setChannel(channel);
        channel.closeFuture().addListener(future -> watchChannelWriteQueueStateByChannelId.remove(channel.id(), state));
        return state;
    }

    /**
     * 调度当前 channel 的串行出队发送任务。
     *
     * <p>
     * TODO:
     *  scheduleDrain 的含义是“调度一次 drain（排空队列）任务”。
     *  通过 draining CAS 保证同一 channel 同时最多只有一个 drain 在执行。
     * </p>
     *
     * <p>
     * TODO:为什么在生产者线程先做 CAS？
     *  因为上层有“apply/event-loop 线程 + RpcRequestExecutor 线程池线程”并发 enqueue。
     *  如果不先 CAS，会向 eventLoop 重复提交大量 drain 任务。
     *  CAS 成功者负责“提交一次任务”，其余生产者直接返回，避免任务风暴。
     * </p>
     *
     * <p>
     * TODO:为什么还要显式切到 eventLoop，而不是在外线程直接 writeAndFlush？
     *  虽然 Netty 允许外线程 writeAndFlush，但会把每次写都拆成 eventLoop 任务。
     *  这里统一在 eventLoop 内批量 write/flush，可减少跨线程任务数量并保持稳定顺序。
     * </p>
     */
    private void scheduleDrain(WatchChannelWriteQueueState state) {
        if (state == null || state.getChannel() == null) {
            return;
        }
        if (!state.getDraining().compareAndSet(false, true)) {
            return;
        }

        try {
            state.getChannel().eventLoop().execute(() -> drainInEventLoop(state));
        } catch (Throwable throwable) {
            // eventLoop 拒绝任务时必须释放 draining，避免后续消息永远无法再调度。
            state.getDraining().set(false);
        }
    }

    /**
     * 在同一 event-loop 内分批顺序发送当前 channel 的待发消息。
     */
    private void drainInEventLoop(WatchChannelWriteQueueState state) {
        Channel channel = state.getChannel();
        Queue<RpcMessage> outboundQueue = state.getOutboundQueue();
        int drainedCount = 0;

        while (drainedCount < MAX_MESSAGES_PER_DRAIN) {
            if (channel == null || !channel.isActive()) {
                // 连接已关闭时清空积压消息，防止该 channel 写状态长期堆积无效数据。
                outboundQueue.clear();
                break;
            }

            // TODO:poll 是非阻塞调用；队列为空会立即返回 null，不会阻塞 eventLoop。
            RpcMessage rpcMessage = outboundQueue.poll();
            if (rpcMessage == null) {
                break;
            }

            if (!channel.isWritable()) {
                // 网络侧背压：本轮先让出 eventLoop，等待下一次调度继续发送。
                outboundQueue.offer(rpcMessage);
                break;
            }

            // 分批写：先 write，轮末统一 flush，减少每条消息一次 flush 的调度开销。
            channel.write(rpcMessage);
            drainedCount++;
        }

        if (drainedCount > 0 && channel != null && channel.isActive()) {
            channel.flush();
        }

        state.getDraining().set(false);
        if (!outboundQueue.isEmpty()) {
            // TODO:本轮达到批量上限或被背压中断时，重新调度下一轮。这样既保持顺序，又不会单次长循环阻塞 eventLoop。
            scheduleDrain(state);
        }
    }

    /**
     * WatchChannelWriteQueueState
     *
     * @author XJks
     * @description 单个 channel 的发送状态。
     */
    private static class WatchChannelWriteQueueState {

        /**
         * 绑定连接。
         */
        private Channel channel;

        /**
         * 待发送队列。
         */
        private final Queue<RpcMessage> outboundQueue = new ConcurrentLinkedQueue<>();

        /**
         * 是否正在发送。
         */
        private final AtomicBoolean draining = new AtomicBoolean(false);

        public Channel getChannel() {
            return channel;
        }

        public void setChannel(Channel channel) {
            this.channel = channel;
        }

        public Queue<RpcMessage> getOutboundQueue() {
            return outboundQueue;
        }

        public AtomicBoolean getDraining() {
            return draining;
        }
    }
}
