package com.xhj.etcd.sdk.client.watch;

import com.xhj.etcd.rpc.RpcMessage;
import com.xhj.etcd.rpc.RpcMessageHandler;
import com.xhj.etcd.rpc.RpcMessageHandlerRegistration;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * WatchSubscriptionRegistry
 *
 * @author XJks
 * @description Watch 订阅注册表与消息分发器。
 *
 * <p>职责边界：</p>
 * <ul>
 *     <li>维护 watchId 和 rpcMessageId 到 {@link WatchSubscription} 的映射。</li>
 *     <li>把入站 RESPONSE / STREAM / ERROR 消息分发给对应订阅对象。</li>
 *     <li>在连接关闭、取消或失败时统一清理注册关系。</li>
 * </ul>
 */
public class WatchSubscriptionRegistry implements RpcMessageHandler {

    /**
     * watchId -> WatchSubscription。
     */
    private final Map<Long, WatchSubscription> subscriptionByWatchId = new ConcurrentHashMap<>();

    /**
     * rpcMessageId -> WatchSubscription。
     */
    private final Map<String, WatchSubscription> subscriptionByRpcMessageId = new ConcurrentHashMap<>();

    /**
     * 注册新的订阅。
     *
     * @param subscription 订阅对象
     */
    public void register(WatchSubscription subscription) {
        if (subscription == null) {
            throw new IllegalArgumentException("watch subscription must not be null");
        }

        // 1) 先以 watchId 去重，保证同一个客户端实例内不存在重复活跃会话。
        if (subscriptionByWatchId.putIfAbsent(subscription.getWatchId(), subscription) != null) {
            throw new IllegalStateException("duplicate active watchId, watchId=" + subscription.getWatchId());
        }

        // 2) 再以 rpcMessageId 去重，保证同一 TCP 连接多路消息分发不会串线。
        WatchSubscription previous = subscriptionByRpcMessageId.putIfAbsent(subscription.getRpcMessageId(), subscription);
        if (previous != null) {
            // 3) 第二步失败时回滚第一步，保持双索引一致性。
            subscriptionByWatchId.remove(subscription.getWatchId());
            throw new IllegalStateException("duplicate watch rpcMessageId, rpcMessageId=" + subscription.getRpcMessageId());
        }
    }

    /**
     * 按 watch 对象移除注册。
     *
     * @param subscription 订阅对象
     */
    public void remove(WatchSubscription subscription) {
        if (subscription == null) {
            return;
        }
        // TODO: 双 map 按“值匹配”移除，避免误删已被复用的新订阅对象。
        subscriptionByWatchId.remove(subscription.getWatchId(), subscription);
        subscriptionByRpcMessageId.remove(subscription.getRpcMessageId(), subscription);
    }

    @Override
    public void handle(RpcMessage message, RpcMessageHandlerRegistration registration) {
        if (message == null || message.getRpcMessageId() == null) {
            return;
        }

        // 1) 根据 rpcMessageId 找到目标订阅；找不到直接忽略，避免无效消息影响其他会话。
        WatchSubscription subscription = subscriptionByRpcMessageId.get(message.getRpcMessageId());
        if (subscription == null) {
            return;
        }

        // 2) 把消息交给单订阅对象处理（阶段校验、ACK 回填、事件回调都在订阅对象内完成）。
        subscription.handleMessage(message);
    }

    @Override
    public void handleConnectionClosed(Throwable cause, RpcMessageHandlerRegistration registration) {
        if (registration == null || registration.getRpcMessageId() == null) {
            return;
        }

        // 1) 先按 rpcMessageId 删除索引，避免连接关闭期间重复回调。
        WatchSubscription subscription = subscriptionByRpcMessageId.remove(registration.getRpcMessageId());
        if (subscription == null) {
            registration.remove();
            return;
        }

        // 2) 再同步删除 watchId 索引，并把关闭事件下发给订阅对象。
        subscriptionByWatchId.remove(subscription.getWatchId(), subscription);
        subscription.handleConnectionClosed(cause);

        // 3) 最后移除 rpc 框架侧 handler 注册。
        registration.remove();
    }
}
