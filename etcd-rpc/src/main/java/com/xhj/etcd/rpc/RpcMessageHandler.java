package com.xhj.etcd.rpc;

/**
 * RpcMessageHandler
 *
 * @author XJks
 * @description RPC 消息处理器接口，用于消费服务端或客户端收到的异步消息。
 */
public interface RpcMessageHandler {

    /**
     * 处理收到的 RPC 消息。
     */
    void handle(RpcMessage message, RpcMessageHandlerRegistration registration);

    /**
     * 连接关闭时回调，用于清理和该连接相关的等待请求或 stream。
     */
    void handleConnectionClosed(Throwable cause, RpcMessageHandlerRegistration registration);
}
