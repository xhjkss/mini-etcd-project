package com.xhj.etcd.kernel.etcd.store.watch;

import com.xhj.etcd.kernel.etcd.etcdrpc.WatchNotification;
import com.xhj.etcd.rpc.RpcMessageType;
import com.xhj.etcd.serializer.Serializer;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;

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
    private final ConcurrentMap<String, Set<Long>> watchIdsByChannelId = new ConcurrentHashMap<>();

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
     */
    public WatchSession create(long requestedWatchId, String startKey, String endKeyExclusive, boolean prefixMatch, long nextRevision) {
        if (requestedWatchId > 0L && sessionByWatchId.containsKey(requestedWatchId)) {
            throw new IllegalStateException("duplicate active watchId, watchId=" + requestedWatchId);
        }

        WatchSession session = new WatchSession();
        session.setStartKey(startKey);
        session.setEndKeyExclusive(endKeyExclusive);
        session.setPrefixMatch(prefixMatch);
        session.setNextRevision(nextRevision);
        session.setNotificationPushEnabled(false);

        while (true) {
            long watchId = resolveWatchId(requestedWatchId);
            session.setWatchId(watchId);
            WatchSession previous = sessionByWatchId.putIfAbsent(watchId, copy(session));
            if (previous == null) {
                return copy(session);
            }
            if (requestedWatchId > 0L) {
                throw new IllegalStateException("duplicate active watchId, watchId=" + requestedWatchId);
            }
            requestedWatchId = 0L;
        }
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

        final String channelId = watchChannelId(channel);
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
     */
    public boolean cancel(long watchId) {
        WatchSession session = sessionByWatchId.remove(watchId);
        if (session == null) {
            return false;
        }
        if (session.getChannel() != null) {
            String channelId = watchChannelId(session.getChannel());
            Set<Long> watchIds = watchIdsByChannelId.get(channelId);
            if (watchIds != null) {
                watchIds.remove(watchId);
                if (watchIds.isEmpty()) {
                    watchIdsByChannelId.remove(channelId);
                }
            }
        }
        return true;
    }

    /**
     * 取消指定 channel 绑定的全部 watch 会话。
     */
    public void cancelByChannelId(String channelId) {
        if (channelId == null || channelId.trim().isEmpty()) {
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
    private long resolveWatchId(long requestedWatchId) {
        if (requestedWatchId > 0L) {
            nextWatchId.updateAndGet(current -> Math.max(current, requestedWatchId));
            return requestedWatchId;
        }
        long nextId = nextWatchId.incrementAndGet();
        return nextId <= 0L ? Math.abs(nextId) + 1L : nextId;
    }

    /**
     * 构造 channelId。
     */
    private String watchChannelId(Channel channel) {
        return channel.id().asLongText();
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
