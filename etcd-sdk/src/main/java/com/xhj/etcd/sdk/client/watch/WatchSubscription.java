package com.xhj.etcd.sdk.client.watch;

import com.xhj.etcd.kernel.etcd.etcdrpc.EtcdRpcResponse;
import com.xhj.etcd.kernel.etcd.etcdrpc.WatchCancelRequest;
import com.xhj.etcd.kernel.etcd.etcdrpc.WatchCancelResponse;
import com.xhj.etcd.kernel.etcd.etcdrpc.WatchEventView;
import com.xhj.etcd.kernel.etcd.etcdrpc.WatchNotification;
import com.xhj.etcd.kernel.etcd.etcdrpc.WatchSubscribeRequest;
import com.xhj.etcd.kernel.etcd.etcdrpc.WatchSubscribeResponse;
import com.xhj.etcd.kernel.etcd.node.EtcdNode;
import com.xhj.etcd.rpc.NodeEndpoint;
import com.xhj.etcd.rpc.RpcClient;
import com.xhj.etcd.rpc.RpcMessage;
import com.xhj.etcd.serializer.Serializer;
import com.xhj.etcd.serializer.SerializerRegistry;
import lombok.Getter;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * WatchSubscription
 *
 * @author XJks
 * @description 单个 watch 订阅的内聚状态对象。
 *
 * <p>职责边界：</p>
 * <ul>
 *     <li>本类只维护单个 watch 的生命周期、控制面请求和回调状态。</li>
 *     <li>订阅注册、消息路由和连接清理由 {@link WatchSubscriptionRegistry} 负责。</li>
 *     <li>对外只实现最小句柄能力，不暴露 watchId、endpoint 等实现细节。</li>
 * </ul>
 */
@Getter
public class WatchSubscription implements WatchHandle {

    /**
     * Watch 生命周期阶段。
     */
    public enum WatchPhase {

        /**
         * 正在发送订阅请求，等待首帧响应。
         */
        SUBSCRIBING,

        /**
         * 订阅成功，正在接收事件推送。
         */
        ACTIVE,

        /**
         * 已发送取消请求，等待取消响应。
         */
        CANCELLING,

        /**
         * 已完成关闭，后续消息全部忽略。
         */
        CLOSED
    }

    /**
     * 默认序列化器。
     */
    private final Serializer serializer = SerializerRegistry.getDefaultSerializer();

    /**
     * RPC 客户端。
     */
    private final RpcClient rpcClient;

    /**
     * 订阅注册表。
     */
    private final WatchSubscriptionRegistry registry;

    /**
     * 当前 watchId。
     */
    private final long watchId;

    /**
     * 当前订阅对应的 rpcMessageId。
     */
    private final String rpcMessageId;

    /**
     * 当前订阅绑定的目标节点。
     */
    private final NodeEndpoint endpoint;

    /**
     * 当前订阅监听器。
     */
    private final WatchListener listener;

    /**
     * 一次订阅/取消控制请求的等待超时时间，单位：毫秒。
     */
    private final long controlTimeoutMillis;

    /**
     * 首帧订阅响应 future。
     */
    private final CompletableFuture<EtcdRpcResponse<WatchSubscribeResponse>> subscribeAckFuture = new CompletableFuture<>();

    /**
     * 取消响应 future。
     */
    private final CompletableFuture<EtcdRpcResponse<WatchCancelResponse>> cancelAckFuture = new CompletableFuture<>();

    /**
     * 当前订阅阶段。
     */
    private final AtomicReference<WatchPhase> phase = new AtomicReference<>(WatchPhase.SUBSCRIBING);

    public WatchSubscription(RpcClient rpcClient, WatchSubscriptionRegistry registry, long watchId, String rpcMessageId, NodeEndpoint endpoint, WatchListener listener, long controlTimeoutMillis) {
        if (rpcClient == null) {
            throw new IllegalArgumentException("rpcClient must not be null");
        }
        if (registry == null) {
            throw new IllegalArgumentException("registry must not be null");
        }
        if (endpoint == null) {
            throw new IllegalArgumentException("endpoint must not be null");
        }
        if (listener == null) {
            throw new IllegalArgumentException("listener must not be null");
        }
        if (rpcMessageId == null || rpcMessageId.trim().isEmpty()) {
            throw new IllegalArgumentException("rpcMessageId must not be empty");
        }

        this.rpcClient = rpcClient;
        this.registry = registry;
        this.watchId = watchId;
        this.rpcMessageId = rpcMessageId;
        this.endpoint = endpoint;
        this.listener = listener;
        this.controlTimeoutMillis = controlTimeoutMillis;
    }

    /**
     * 发送订阅请求并等待首帧响应。
     *
     * <p>
     * TODO:
     *  订阅请求和首帧响应仍然是同一个 rpcMessageId 的一元控制闭环；
     *  后续服务端主动推送的 STREAM 消息也继续复用该 rpcMessageId 路由到当前订阅。
     * </p>
     *
     * @param request 订阅请求
     * @return 首帧订阅响应
     */
    public EtcdRpcResponse<WatchSubscribeResponse> subscribe(WatchSubscribeRequest request) {
        try {
            // 1) 发送 subscribe 一元控制请求（REQUEST）。
            rpcClient.sendRequestWithRpcMessageId(
                    endpoint,
                    EtcdNode.RPC_SERVICE_NAME,
                    EtcdNode.HANDLE_ETCD_RPC_WATCH_SUBSCRIBE_REQUEST_METHOD_NAME,
                    request,
                    rpcMessageId,
                    registry);
            // 2) 阻塞等待首帧 RESPONSE，由 handleResponse(...) 回填 subscribeAckFuture。
            return subscribeAckFuture.get(controlTimeoutMillis, TimeUnit.MILLISECONDS);
        } catch (Exception exception) {
            // 3) 握手失败时统一关闭订阅，清理路由和 handler 注册，避免悬挂会话。
            close();
            throw new IllegalStateException("watch subscribe failed, watchId=" + watchId, exception);
        }
    }

    @Override
    public void cancel() {
        /**
         * TODO:
         *  cancel 只允许 ACTIVE -> CANCELLING 的单向推进，避免重复发送取消请求。
         *  如果已经在 CANCELLING，则复用已有 future 等待同一份取消响应结果。
         */
        if (!phase.compareAndSet(WatchPhase.ACTIVE, WatchPhase.CANCELLING)) {
            if (phase.get() == WatchPhase.CLOSED) {
                return;
            }
            if (phase.get() == WatchPhase.CANCELLING) {
                EtcdRpcResponse<WatchCancelResponse> response = awaitCancelResponse();
                if (response != null && response.getHeader() != null && !response.getHeader().isSuccess()) {
                    throw new IllegalStateException(response.getHeader().getMessage());
                }
                /**
                 * TODO:
                 *  BUG现象：并发重复调用 cancel() 时，当前线程可能只等到了 cancel ACK，但另一个回调线程尚未执行 terminate，导致 cancel() 返回后短时间内 isClosed() 仍可能为 false。
                 *  原因：cancel ACK 回填（future.complete）与 terminate 不是同一条执行路径内的原子动作，两者存在微小时序窗口。
                 *  修复：这里再次调用 close() 主动收敛本地状态；close/terminate 是幂等实现，若另一线程已关闭则不会重复副作用，未关闭则由当前线程补齐 CLOSED 收敛。
                 */
                close();
                return;
            }
            return;
        }

        try {
            // 1) 发送 cancel 一元控制请求（沿用同一个 rpcMessageId）。
            rpcClient.sendRequestWithRpcMessageId(
                    endpoint,
                    EtcdNode.RPC_SERVICE_NAME,
                    EtcdNode.HANDLE_ETCD_RPC_WATCH_CANCEL_REQUEST_METHOD_NAME,
                    new WatchCancelRequest(watchId),
                    rpcMessageId,
                    registry);
            // 2) 阻塞等待 cancel ACK；失败则抛异常并触发关闭。
            EtcdRpcResponse<WatchCancelResponse> response = awaitCancelResponse();
            if (response != null && response.getHeader() != null && !response.getHeader().isSuccess()) {
                throw new IllegalStateException(response.getHeader().getMessage());
            }
            /**
             * TODO:
             *  BUG现象：主取消路径中，awaitCancelResponse() 返回后，可能先于 handleCancelResponse(...) 中的 terminate 执行完成，出现“cancel() 已返回成功，但本地句柄尚未 CLOSED”的窗口。
             *  原因：cancel ACK 的等待线程与 RPC 入站消息处理线程并发执行，ACK 达成不等价于本地状态已完成关闭。
             *  修复：收到成功 ACK 后立即 close()，强制把 cancel() 的返回语义收敛为“返回即本地关闭已完成或已幂等完成”。
             *  相关路径：{@link #awaitCancelResponse()} -> {@link #handleCancelResponse(EtcdRpcResponse, WatchCancelResponse)} -> {@link #terminate(Throwable, boolean)}。
             */
            close();
        } catch (Exception exception) {
            close();
            throw new IllegalStateException("watch cancel failed, watchId=" + watchId, exception);
        }
    }

    @Override
    public void close() {
        terminate(null, false);
    }

    @Override
    public boolean isClosed() {
        return phase.get() == WatchPhase.CLOSED;
    }

    /**
     * 处理入站 RPC 消息。
     *
     * <p>该方法只负责当前订阅的协议语义判断，不负责注册表查找。</p>
     *
     * @param message 入站消息
     */
    void handleMessage(RpcMessage message) {
        /**
         * TODO:
         *  竞态场景 A（可容忍）：
         *  terminate() 可能已把 phase 切为 CLOSED，但 registry/rpc handler 还未完全移除。
         *  这时消息若仍被路由到当前订阅，会在下面 isClosed() 处被快速丢弃，不会进入业务回调。
         *  后续若要根治“关闭后绝对零消息进入 handleMessage”：
         *  需要引入更强屏障（例如单线程串行执行器或连接级顺序栅栏），仅靠本地状态位无法绝对消除并发窗口。
         */
        if (message == null || isClosed()) {
            return;
        }
        if (message.getType() == null) {
            terminate(new IllegalStateException("watch rpc message type is null"), true);
            return;
        }

        try {
            switch (message.getType()) {
                case RESPONSE:
                    // REQUEST 对应的一元响应（subscribe/cancel）走 RESPONSE 分支。
                    handleResponse(message);
                    return;
                case STREAM:
                    // 服务端主动事件推送走 STREAM 分支。
                    handleStream(message);
                    return;
                case ERROR:
                    // 协议错误统一走 ERROR 分支并终止当前 watch。
                    handleError(message.getErrorMessage());
                    return;
                default:
                    throw new IllegalStateException("unsupported watch rpc message type: " + message.getType());
            }
        } catch (Throwable throwable) {
            // TODO: 客户端 watch 收到消息后如果处理失败，当前阶段直接关闭该 watch，会话不做自动重连。
            terminate(new IllegalStateException("watch message handling failed, watchId=" + watchId, throwable), true);
        }
    }

    /**
     * 连接关闭时回调。
     *
     * @param cause 关闭原因
     */
    void handleConnectionClosed(Throwable cause) {
        terminate(cause == null ? new IllegalStateException("watch connection closed") : cause, true);
    }

    /**
     * 处理订阅响应。
     *
     * <p>流程：</p>
     * <ol>
     *     <li>先把首帧响应回填到 future，避免调用方超时等待。</li>
     *     <li>再根据 header 判断是否订阅成功或需要重试 leader。</li>
     *     <li>成功时推进到 ACTIVE，并把历史回放和订阅成功回调交给监听器。</li>
     * </ol>
     *
     * @param message 订阅响应消息
     */
    private void handleResponse(RpcMessage message) {
        EtcdRpcResponse<?> response = serializer.deserialize(message.getData(), EtcdRpcResponse.class);
        if (response == null) {
            throw new IllegalStateException("watch response is null");
        }

        Object body = response.getBody();
        WatchPhase currentPhase = phase.get();
        switch (currentPhase) {
            case SUBSCRIBING:
                // SUBSCRIBING 阶段只接受 WatchSubscribeResponse。
                if (!(body instanceof WatchSubscribeResponse)) {
                    throw new IllegalStateException("unexpected watch subscribe response body type: " + (body == null ? "null" : body.getClass().getName()));
                }
                handleSubscribeResponse(response, (WatchSubscribeResponse) body);
                return;
            case CANCELLING:
                // CANCELLING 阶段只接受 WatchCancelResponse。
                if (!(body instanceof WatchCancelResponse)) {
                    throw new IllegalStateException("unexpected watch cancel response body type: " + (body == null ? "null" : body.getClass().getName()));
                }
                handleCancelResponse(response, (WatchCancelResponse) body);
                return;
            default:
                throw new IllegalStateException("unexpected watch response phase: " + currentPhase);
        }
    }

    /**
     * 处理服务端主动推送。
     *
     * @param message 推送消息
     */
    private void handleStream(RpcMessage message) {
        WatchPhase currentPhase = phase.get();
        if (currentPhase != WatchPhase.ACTIVE && currentPhase != WatchPhase.CANCELLING) {
            throw new IllegalStateException("watch stream received in invalid phase: " + currentPhase);
        }

        // 1) 反序列化推送体并校验 watchId 归属，防止同一 TCP 多 watch 场景下的消息串线。
        WatchNotification notification = serializer.deserialize(message.getData(), WatchNotification.class);
        if (notification == null) {
            throw new IllegalStateException("watch stream notification is null");
        }
        if (notification.getWatchId() != watchId) {
            throw new IllegalStateException("watch stream watchId mismatch, expected=" + watchId + ", actual=" + notification.getWatchId());
        }

        // 2) 把事件回调给业务监听器；监听器异常会在上层 catch 中触发终止。
        listener.onNotification(notification);
        // 3) 服务端标记 canceled 时，客户端同步关闭本地订阅。
        if (notification.isCanceled()) {
            terminate(null, false);
        }
    }

    /**
     * 处理协议错误消息。
     *
     * @param errorMessage 错误信息
     */
    private void handleError(String errorMessage) {
        throw new IllegalStateException(errorMessage == null ? "watch error" : errorMessage);
    }

    /**
     * 处理订阅响应。
     *
     * @param response 订阅响应
     * @param body     订阅响应体
     */
    private void handleSubscribeResponse(EtcdRpcResponse<?> response, WatchSubscribeResponse body) {
        if (body == null) {
            throw new IllegalStateException("watch subscribe response body is null");
        }
        if (body.getWatchId() <= 0L) {
            body.setWatchId(watchId);
        }

        // 1) 先回填首帧 ACK，唤醒 watch(...) 调用线程。
        subscribeAckFuture.complete(EtcdRpcResponse.of(response.getHeader(), body));

        if (response.getHeader() != null && response.getHeader().isSuccess()) {
            // 2) 再推进阶段并触发订阅成功回调。
            phase.set(WatchPhase.ACTIVE);
            listener.onSubscribed(body);

            // 3) 最后把首帧附带的历史事件批量回放给监听器。
            WatchNotification replayNotification = buildReplayNotification(body);
            if (replayNotification != null
                    && replayNotification.getEvents() != null
                    && !replayNotification.getEvents().isEmpty()) {
                listener.onNotification(replayNotification);
            }
            return;
        }

        listener.onError(new IllegalStateException(response.getHeader() == null ? "watch subscribe failed" : response.getHeader().getMessage()));
        terminate(null, false);
    }

    /**
     * 处理取消响应。
     *
     * @param response 取消响应
     * @param body     取消响应体
     */
    private void handleCancelResponse(EtcdRpcResponse<?> response, WatchCancelResponse body) {
        if (body == null) {
            throw new IllegalStateException("watch cancel response body is null");
        }
        if (body.getWatchId() <= 0L) {
            body.setWatchId(watchId);
        }

        // 1) 先回填取消 ACK，唤醒 cancel() 等待线程。
        cancelAckFuture.complete(EtcdRpcResponse.of(response.getHeader(), body));

        if (response.getHeader() != null && response.getHeader().isSuccess()) {
            // 2) 取消成功：先通知监听器，再统一关闭本地订阅。
            listener.onCanceled(body);
            terminate(null, false);
            return;
        }

        listener.onError(new IllegalStateException(response.getHeader() == null ? "watch cancel failed" : response.getHeader().getMessage()));
        terminate(null, false);
    }

    /**
     * 等待取消响应。
     */
    private EtcdRpcResponse<WatchCancelResponse> awaitCancelResponse() {
        try {
            return cancelAckFuture.get(controlTimeoutMillis, TimeUnit.MILLISECONDS);
        } catch (Exception exception) {
            // cancel 等待超时或异常时，防止订阅悬挂，直接关闭本地会话。
            close();
            throw new IllegalStateException("watch cancel failed, watchId=" + watchId, exception);
        }
    }

    /**
     * 终止当前订阅。
     *
     * <p>
     * TODO:
     *  统一承接“正常关闭”和“异常终止”两类路径，避免 close/abort 多入口造成语义分散。
     *  关闭只做本地清理，不会向服务端发送取消请求。
     * </p>
     *
     * @param cause          终止原因；为 null 时使用默认关闭语义
     * @param notifyListener 是否回调错误监听器
     */
    private void terminate(Throwable cause, boolean notifyListener) {
        if (phase.getAndSet(WatchPhase.CLOSED) == WatchPhase.CLOSED) {
            return;
        }

        // 1) 统一失败原因，并终止所有等待中的控制面 future。
        Throwable actualCause = cause == null ? new IllegalStateException("watch closed") : cause;
        subscribeAckFuture.completeExceptionally(actualCause);
        cancelAckFuture.completeExceptionally(actualCause);

        /**
         * TODO:
         *  竞态场景 B（可容忍）：
         *  future 回填与路由移除不是同一个原子动作，二者之间存在微小时序窗口。
         *  窗口内可能出现：
         *  1) 消息仍被分发到该订阅（但通常会被 handleMessage 的 isClosed() 快速丢弃）；
         *  2) 极少量 in-flight 消息已越过前置检查并继续执行一次回调。
         *  当前实现选择“尽量压缩窗口 + 最终收敛正确”。
         *  若未来要根治“close/cancel 后绝对零回调”，需要协议/执行模型升级（如 ACK、序列屏障、单线程串行化）。
         */
        // 2) 再清理注册表与 rpcMessageId handler，防止后续消息继续路由到已关闭订阅。
        registry.remove(this);
        rpcClient.removeRpcMessageHandler(rpcMessageId);

        // 3) 最后按需通知业务层错误回调。
        if (notifyListener) {
            try {
                listener.onError(actualCause);
            } catch (Throwable ignore) {
            }
        }
    }

    /**
     * 构造订阅成功后的历史回放通知。
     *
     * @param subscribeResponse 订阅响应
     * @return 历史回放通知，若无事件则返回 null
     */
    private WatchNotification buildReplayNotification(WatchSubscribeResponse subscribeResponse) {
        List<WatchEventView> events = subscribeResponse.getEvents();
        if (events == null || events.isEmpty()) {
            return null;
        }
        return WatchNotification.of(
                subscribeResponse.getWatchId(),
                subscribeResponse.getCurrentRevision(),
                subscribeResponse.getNextRevision(),
                events);
    }
}
