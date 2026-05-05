package com.xhj.etcd.rpc;

/**
 * RpcMessageHandlerRegistration
 *
 * @author XJks
 * @description RPC 消息处理器注册关系，作为调用方持有的注销句柄。
 */
public class RpcMessageHandlerRegistration {

    /**
     * RPC 消息 ID。
     *
     * <p>用于标识当前注册关系归属的请求、响应或流式通道。</p>
     */
    private final String rpcMessageId;

    /**
     * 当前注册关系绑定的消息处理器。
     */
    private final RpcMessageHandler handler;

    /**
     * 消息处理器注册表。
     *
     * <p>Registration 不直接维护注册容器，只通过 registry 完成精确注销。</p>
     */
    private final RpcMessageHandlerRegistry registry;

    public RpcMessageHandlerRegistration(String rpcMessageId, RpcMessageHandler handler, RpcMessageHandlerRegistry registry) {
        this.rpcMessageId = rpcMessageId;
        this.handler = handler;
        this.registry = registry;
    }

    public String getRpcMessageId() {
        return rpcMessageId;
    }

    public RpcMessageHandler getHandler() {
        return handler;
    }

    /**
     * 注销当前注册关系。
     *
     * <p>TODO: 注销时会校验注册表中当前保存的 registration 是否就是当前对象，避免同一个 rpcMessageId 被覆盖注册后，旧 registration 误删新的注册关系。</p>
     */
    public void remove() {
        registry.removeIfMatch(rpcMessageId, this);
    }
}