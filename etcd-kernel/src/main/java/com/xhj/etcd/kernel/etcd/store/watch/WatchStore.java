package com.xhj.etcd.kernel.etcd.store.watch;

import com.xhj.etcd.kernel.etcd.etcdrpc.WatchNotification;
import com.xhj.etcd.rpc.RpcMessageType;
import com.xhj.etcd.serializer.Serializer;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelId;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * WatchStore
 *
 * @author XJks
 * @description Watch 会话状态机。
 *
 * <p>当前阶段会话状态只保存在内存，不进入快照持久化。</p>
 */
public class WatchStore {

    /**
     * watchId -> watch 会话。
     */
    private final ConcurrentMap<Long, WatchSession> sessionByWatchId = new ConcurrentHashMap<>();

    /**
     * channelId -> watchId 集合。
     */
    private final ConcurrentMap<ChannelId, Set<Long>> watchIdsByChannelId = new ConcurrentHashMap<>();

    /**
     * 下一次分配的 watchId。
     */
    private final AtomicLong nextWatchId = new AtomicLong(0L);

    /**
     * Watch channel 写路由注册表。
     */
    private final WatchChannelWriteRegistry watchChannelWriteRegistry;

    public WatchStore(Serializer serializer) {
        if (serializer == null) {
            throw new IllegalArgumentException("serializer must not be null");
        }
        this.watchChannelWriteRegistry = new WatchChannelWriteRegistry(serializer);
    }

    /**
     * 创建 watch 订阅会话。
     *
     * <p>TODO:
     * create 只做“会话登记”，不做 channel 绑定，也不立即允许推送。
     * 这样可以把“会话创建”和“首帧 subscribe 响应已进入发送队列”两个时点拆开，
     * 后续由 enableNotificationPush(...) 做明确门控，避免先推事件后回响应的时序竞态。
     * </p>
     */
    public WatchSession create(String startKey, String endKeyExclusive, boolean prefixMatch, long nextRevision) {
        WatchSession session = new WatchSession();
        session.setStartKey(startKey);
        session.setEndKeyExclusive(endKeyExclusive);
        session.setPrefixMatch(prefixMatch);
        session.setNextRevision(nextRevision);
        session.setNotificationPushEnabled(false);

        // TODO: watchId 只由服务端分配，客户端 subscribe 请求不再携带 watchId，避免跨节点/跨连接手工指定 ID 引发冲突。
        long watchId = resolveNextWatchId();
        session.setWatchId(watchId);
        sessionByWatchId.put(watchId, copy(session));
        return copy(session);
    }

    /**
     * 绑定 watch 会话到 channel。
     *
     * <p>TODO: 一个 channel（即一条 TCP 连接）可以绑定多个 watchId。
     * 这里的映射是 channelId -> Set&lt;watchId&gt;，用于表达“同一客户端在同一 TCP 上开多个 watch”。
     * 连接关闭时通过 closeFuture 一次性回收该 channel 下所有 watch，会话不会泄漏。</p>
     */
    public WatchSession bindWatchChannel(long watchId, Channel channel, String rpcMessageId) {
        if (watchId <= 0L || channel == null) {
            return null;
        }
        WatchSession session = sessionByWatchId.get(watchId);
        if (session == null) {
            return null;
        }
        session.setChannel(channel);
        session.setRpcMessageId(rpcMessageId);

        final ChannelId channelId = channel.id();
        Set<Long> watchIds = watchIdsByChannelId.get(channelId);
        if (watchIds == null) {
            Set<Long> createdWatchIds = ConcurrentHashMap.newKeySet();
            Set<Long> previousWatchIds = watchIdsByChannelId.putIfAbsent(channelId, createdWatchIds);
            watchIds = previousWatchIds == null ? createdWatchIds : previousWatchIds;
            if (previousWatchIds == null) {
                channel.closeFuture().addListener(new ChannelFutureListener() {
                    @Override
                    public void operationComplete(io.netty.channel.ChannelFuture future) {
                        // TCP 断开后，统一清理该连接上绑定的全部 watchId。
                        cancelByChannelId(channelId);
                    }
                });
            }
        }
        watchIds.add(watchId);
        return copy(session);
    }

    /**
     * 启用 watch 通知推送。
     *
     * <p>
     * TODO:
     *  watch 会话在 subscribe 响应写回成功前不允许推送 STREAM。
     *  只有 notificationPushEnabled=true 后 publish 才会开始发送增量通知，
     *  保证服务端先发 subscribe 响应再发事件流。
     * </p>
     *
     * @param watchId watch 会话 ID
     * @return true 表示启用成功
     */
    public boolean enableNotificationPush(long watchId) {
        WatchSession session = sessionByWatchId.get(watchId);
        if (session == null) {
            return false;
        }
        session.setNotificationPushEnabled(true);
        return true;
    }

    /**
     * 入队 watch subscribe/cancel 等控制面响应。
     *
     * @param channel      目标连接
     * @param rpcMessageId 路由消息 ID
     * @param response     响应体
     * @return true 表示成功入队
     */
    public boolean enqueueWatchResponse(Channel channel, String rpcMessageId, Object response) {
        // TODO: 控制面（RESPONSE）与数据面（STREAM）统一进同一个 channel 写注册表，
        //  由 channel.eventLoop 串行 drain，可稳定保证该 channel 的发送顺序。
        return watchChannelWriteRegistry.enqueue(channel, rpcMessageId, RpcMessageType.RESPONSE, response);
    }

    /**
     * 入队 watch 事件通知。
     *
     * @param channel      目标连接
     * @param rpcMessageId 路由消息 ID
     * @param notification 通知体
     * @return true 表示成功入队
     */
    public boolean enqueueWatchNotification(Channel channel, String rpcMessageId, WatchNotification notification) {
        // TODO: 不直接 writeAndFlush，统一入队；与 subscribe 首帧响应共用发送通道，避免跨线程写导致顺序抖动。
        return watchChannelWriteRegistry.enqueue(channel, rpcMessageId, RpcMessageType.STREAM, notification);
    }

    /**
     * 清理全部 channel 写状态。
     */
    public void clearWatchChannelWriteQueueStates() {
        watchChannelWriteRegistry.clear();
    }

    /**
     * 获取 watch 会话。
     */
    public WatchSession get(long watchId) {
        WatchSession session = sessionByWatchId.get(watchId);
        return session == null ? null : copy(session);
    }

    /**
     * 列出当前全部 watch 会话快照。
     */
    public List<WatchSession> listAll() {
        List<WatchSession> sessions = new ArrayList<>();
        for (WatchSession session : sessionByWatchId.values()) {
            sessions.add(copy(session));
        }
        return sessions;
    }

    /**
     * 获取当前 watch 会话数量。
     *
     * @return watch 数量
     */
    public int size() {
        return sessionByWatchId.size();
    }

    /**
     * 更新 watch 会话下一次事件读取起始 revision。
     *
     * <p>nextRevision 是每个 watch 会话独立游标，不影响全局 KV currentRevision。</p>
     */
    public void updateNextRevision(long watchId, long nextRevision) {
        WatchSession session = sessionByWatchId.get(watchId);
        if (session == null) {
            return;
        }
        session.setNextRevision(nextRevision);
    }

    /**
     * 取消 watch 会话。
     *
     * <p>会话取消包含两部分：</p>
     * <ol>
     *     <li>从 watchId 主表移除。</li>
     *     <li>从 channelId 反向索引移除，避免连接关闭时重复处理。</li>
     * </ol>
     */
    public boolean cancel(long watchId) {
        WatchSession session = sessionByWatchId.remove(watchId);
        if (session == null) {
            return false;
        }
        if (session.getChannel() != null) {
            unbindWatchIdFromChannel(session.getChannel().id(), watchId);
        }
        return true;
    }

    /**
     * 取消指定 channel 绑定的全部 watch 会话。
     *
     * <p>连接级回收入口：当 TCP 断开时，一次性回收该连接挂载的所有 watch。</p>
     */
    public void cancelByChannelId(ChannelId channelId) {
        if (channelId == null) {
            return;
        }
        Set<Long> watchIds = watchIdsByChannelId.remove(channelId);
        if (watchIds == null || watchIds.isEmpty()) {
            return;
        }
        for (Long watchId : new ArrayList<>(watchIds)) {
            if (watchId != null) {
                sessionByWatchId.remove(watchId);
            }
        }
    }

    /**
     * 解析 watchId。
     */
    private long resolveNextWatchId() {
        long nextId = nextWatchId.incrementAndGet();
        return nextId <= 0L ? Math.abs(nextId) + 1L : nextId;
    }

    /**
     * 解绑 channel 下的单个 watchId。
     *
     * <p>TODO: 这里只用 ChannelId + Set<WatchId> 维护关系，保持结构精简清晰。</p>
     */
    private void unbindWatchIdFromChannel(ChannelId channelId, long watchId) {
        if (channelId == null) {
            return;
        }
        Set<Long> watchIds = watchIdsByChannelId.get(channelId);
        if (watchIds == null) {
            return;
        }
        watchIds.remove(watchId);
        if (watchIds.isEmpty()) {
            watchIdsByChannelId.remove(channelId);
        }
    }

    /**
     * 复制会话对象。
     */
    private WatchSession copy(WatchSession source) {
        WatchSession target = new WatchSession();
        target.setWatchId(source.getWatchId());
        target.setStartKey(source.getStartKey());
        target.setEndKeyExclusive(source.getEndKeyExclusive());
        target.setPrefixMatch(source.isPrefixMatch());
        target.setNextRevision(source.getNextRevision());
        target.setChannel(source.getChannel());
        target.setRpcMessageId(source.getRpcMessageId());
        target.setNotificationPushEnabled(source.isNotificationPushEnabled());
        return target;
    }
}
