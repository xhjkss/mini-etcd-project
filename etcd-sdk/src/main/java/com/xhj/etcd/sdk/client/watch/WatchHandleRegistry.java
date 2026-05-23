package com.xhj.etcd.sdk.client.watch;

import com.xhj.etcd.rpc.RpcMessage;
import com.xhj.etcd.rpc.RpcMessageHandler;
import com.xhj.etcd.rpc.RpcMessageHandlerRegistration;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * WatchHandleRegistry
 *
 * @author XJks
 * @description WatchHandle 注册表与消息分发器。
 *
 * <p>职责边界：</p>
 * <ul>
 *     <li>维护 watchId 和 rpcMessageId 到 {@link DefaultWatchHandle} 的映射。</li>
 *     <li>把入站 RESPONSE / STREAM / ERROR 消息分发给对应 watchHandle。</li>
 *     <li>在连接关闭、取消或失败时统一清理注册关系。</li>
 * </ul>
 */
public class WatchHandleRegistry implements RpcMessageHandler {

    /**
     * watchId -> DefaultWatchHandle。
     *
     * <p>注意：只有 subscribe ACK 成功后才会写入这张表。</p>
     */
    private final Map<Long, DefaultWatchHandle> watchHandleByWatchId = new ConcurrentHashMap<>();

    /**
     * rpcMessageId -> DefaultWatchHandle。
     *
     * <p>注意：创建本地 watchHandle 时就先写入这张表，保证首帧 subscribe 响应不会丢路由。</p>
     */
    private final Map<String, DefaultWatchHandle> watchHandleByRpcMessageId = new ConcurrentHashMap<>();

    /**
     * 注册 watchHandle（只注册 rpcMessageId 路由）。
     *
     * <p>
     * TODO:
     *  这里故意不提前绑定 watchId，因为 watchId 由服务端 subscribe ACK 分配。
     *  若在 ACK 前尝试绑定 watchId，会引入“本地猜测 ID”和服务端真实 ID 不一致的问题。
     * </p>
     */
    public void register(DefaultWatchHandle watchHandle) {
        if (watchHandle == null) {
            throw new IllegalArgumentException("watch handle must not be null");
        }

        // SUBSCRIBING 阶段仅按 rpcMessageId 注册路由，watchId 需等待服务端 subscribe ACK 返回后再绑定。
        DefaultWatchHandle previous = watchHandleByRpcMessageId.putIfAbsent(watchHandle.getRpcMessageId(), watchHandle);
        if (previous != null) {
            throw new IllegalStateException("duplicate watch rpcMessageId, rpcMessageId=" + watchHandle.getRpcMessageId());
        }
    }

    /**
     * 在 subscribe 成功后绑定服务端分配的 watchId。
     *
     * @param watchHandle watchHandle
     * @param watchId     服务端分配的 watchId
     */
    public void bindWatchId(DefaultWatchHandle watchHandle, long watchId) {
        if (watchHandle == null || watchId <= 0L) {
            return;
        }
        DefaultWatchHandle previous = watchHandleByWatchId.putIfAbsent(watchId, watchHandle);
        if (previous != null && previous != watchHandle) {
            throw new IllegalStateException("duplicate active watchId, watchId=" + watchId);
        }
    }

    /**
     * 按 watchHandle 移除注册。
     *
     * @param watchHandle watchHandle
     */
    public void remove(DefaultWatchHandle watchHandle) {
        if (watchHandle == null) {
            return;
        }
        // TODO: 双 map 按“值匹配”移除，避免误删已被复用的新 watchHandle。
        long watchId = watchHandle.getWatchId();
        if (watchId > 0L) {
            watchHandleByWatchId.remove(watchId, watchHandle);
        }
        watchHandleByRpcMessageId.remove(watchHandle.getRpcMessageId(), watchHandle);
    }

    @Override
    public void handle(RpcMessage message, RpcMessageHandlerRegistration registration) {
        if (message == null || message.getRpcMessageId() == null) {
            return;
        }

        // 1) 根据 rpcMessageId 找到目标 watchHandle；找不到直接忽略，避免无效消息影响其他会话。
        DefaultWatchHandle watchHandle = watchHandleByRpcMessageId.get(message.getRpcMessageId());
        if (watchHandle == null) {
            return;
        }

        // 2) 把消息交给单 watchHandle 处理（阶段校验、ACK 回填、事件回调都在句柄内完成）。
        watchHandle.handleMessage(message);
    }

    @Override
    public void handleConnectionClosed(Throwable cause, RpcMessageHandlerRegistration registration) {
        if (registration == null || registration.getRpcMessageId() == null) {
            return;
        }

        // 1) 先按 rpcMessageId 删除索引，避免连接关闭期间重复回调。
        DefaultWatchHandle watchHandle = watchHandleByRpcMessageId.remove(registration.getRpcMessageId());
        if (watchHandle == null) {
            registration.remove();
            return;
        }

        // 2) 再同步删除 watchId 索引，并把关闭事件下发给 watchHandle。
        long watchId = watchHandle.getWatchId();
        if (watchId > 0L) {
            watchHandleByWatchId.remove(watchId, watchHandle);
        }
        watchHandle.handleConnectionClosed(cause);

        // 3) 最后移除 rpc 框架侧 handler 注册。
        registration.remove();
    }
}
