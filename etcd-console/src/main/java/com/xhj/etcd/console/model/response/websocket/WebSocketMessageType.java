package com.xhj.etcd.console.model.response.websocket;

/**
 * WebSocketMessageType
 *
 * @author XJks
 * @description 控制台 WebSocket 消息类型枚举。
 */
public enum WebSocketMessageType {
    /**
     * 节点状态推送。
     *
     * <p>payload:
     * {@link com.xhj.etcd.kernel.etcd.etcdrpc.NodeStatusResponse}。</p>
     *
     * <p>来源:
     * {@link com.xhj.etcd.kernel.etcd.etcdrpc.NodeStatusRequest}
     * -> {@link com.xhj.etcd.kernel.etcd.etcdrpc.NodeStatusResponse}。</p>
     */
    NODE_STATUS,

    /**
     * watch 创建完成通知。
     *
     * <p>payload: {@link com.xhj.etcd.console.model.response.watch.WatchSessionResponse}。</p>
     *
     * <p>来源:
     * {@link com.xhj.etcd.kernel.etcd.etcdrpc.WatchSubscribeRequest}
     * -> {@link com.xhj.etcd.kernel.etcd.etcdrpc.WatchSubscribeResponse}。</p>
     */
    WATCH_CREATED,

    /**
     * watch 事件推送。
     *
     * <p>payload: {@link WatchNotificationPayload}。</p>
     *
     * <p>来源: {@link com.xhj.etcd.kernel.etcd.etcdrpc.WatchNotification}。</p>
     */
    WATCH_EVENT,

    /**
     * watch 取消完成通知。
     *
     * <p>payload: {@link com.xhj.etcd.console.model.response.watch.WatchSessionResponse}。</p>
     *
     * <p>来源:
     * {@link com.xhj.etcd.kernel.etcd.etcdrpc.WatchCancelRequest}
     * -> {@link com.xhj.etcd.kernel.etcd.etcdrpc.WatchCancelResponse}。</p>
     */
    WATCH_CANCELED,

    /**
     * watch 运行时错误通知。
     *
     * <p>payload: {@link java.lang.String}。</p>
     */
    WATCH_ERROR,

    /**
     * WebSocket 框架层错误通知。
     *
     * <p>payload: {@link java.lang.String}。</p>
     */
    CONSOLE_ERROR,

    /**
     * 当前连接列表推送。
     *
     * <p>payload:
     * {@link java.util.List}&lt;{@link com.xhj.etcd.rpc.NodeEndpoint}&gt;。</p>
     */
    CONNECTIONS,

    /**
     * KV 变更通知。
     *
     * <p>payload: {@link KeyValueChangedPayload}。</p>
     *
     * <p>operationType:
     * {@link KeyValueChangeOperationType}。</p>
     *
     * <p>来源:
     * {@link com.xhj.etcd.kernel.etcd.etcdrpc.PutRequest} /
     * {@link com.xhj.etcd.kernel.etcd.etcdrpc.DeleteRequest} /
     * {@link com.xhj.etcd.kernel.etcd.etcdrpc.DeleteRangeRequest} /
     * {@link com.xhj.etcd.kernel.etcd.etcdrpc.WatchNotification}。</p>
     */
    KV_CHANGED
}
