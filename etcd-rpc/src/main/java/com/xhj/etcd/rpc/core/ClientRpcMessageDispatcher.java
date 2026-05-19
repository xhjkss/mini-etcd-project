package com.xhj.etcd.rpc.core;

import com.xhj.etcd.rpc.RpcMessage;
import com.xhj.etcd.rpc.RpcMessageHandlerRegistration;
import com.xhj.etcd.rpc.RpcMessageHandlerRegistry;
import com.xhj.etcd.rpc.RpcMessageType;
import io.netty.channel.ChannelId;

import java.util.EnumSet;
import java.util.List;
import java.util.Set;

/**
 * ClientRpcMessageDispatcher
 *
 * @author XJks
 * @description 客户端 RPC 消息分发器，负责把服务端返回的消息路由到等待中的调用方。
 *
 * <p>客户端发送请求时，会先按 rpcMessageId 注册对应的 RpcMessageHandler；
 * 当网络线程收到响应、流式数据、心跳或异常消息后，本类再根据 rpcMessageId 找回处理器并完成回调。</p>
 */
public class ClientRpcMessageDispatcher {

    /**
     * 客户端允许接收的 RPC 消息类型。
     *
     * <p>客户端入站方向只应该处理服务端返回的响应、流式数据、心跳和错误消息；
     * 如果收到 REQUEST 等服务端入站类型，说明协议方向异常，会被转换为 ERROR 消息交给原 handler 处理。</p>
     */
    private static final Set<RpcMessageType> CLIENT_INBOUND_TYPES = EnumSet.of(
            RpcMessageType.RESPONSE,
            RpcMessageType.STREAM,
            RpcMessageType.HEARTBEAT,
            RpcMessageType.ERROR
    );

    /**
     * RPC 消息处理器注册表。
     *
     * <p>用于根据 rpcMessageId 查找当前仍在等待响应或流式数据的处理器。</p>
     */
    private final RpcMessageHandlerRegistry handlerRegistry;

    public ClientRpcMessageDispatcher(RpcMessageHandlerRegistry handlerRegistry) {
        this.handlerRegistry = handlerRegistry;
    }

    /**
     * 分发客户端入站 RPC 消息。
     *
     * <p>TODO: 客户端侧不会直接按 serviceName/methodName 分发响应，而是通过 rpcMessageId 找回发起请求时注册的 handler；serviceName/methodName 主要用于服务端处理 REQUEST 时路由服务方法。</p>
     *
     * @param message 客户端收到的 RPC 消息
     */
    public void dispatch(RpcMessage message) {
        if (message == null || message.getRpcMessageId() == null) {
            return;
        }

        // 1) 根据 rpcMessageId 找到发起请求时注册的 handler。
        RpcMessageHandlerRegistration registration = handlerRegistry.get(message.getRpcMessageId());
        if (registration == null) {
            return;
        }

        // 2) 客户端入站方向只接受服务端返回类消息，协议方向异常时统一转成 ERROR 回调。
        if (!CLIENT_INBOUND_TYPES.contains(message.getType())) {
            registration.getHandler().handle(buildUnsupportedMessage(message), registration);
            return;
        }

        // 3) 将正常响应、流式数据、心跳或错误消息回调给等待中的 handler。
        registration.getHandler().handle(message, registration);
    }

    /**
     * 构建客户端不支持的入站消息类型错误。
     *
     * <p>该方法不会直接关闭连接，只把协议方向异常转换为框架层 ERROR 消息，交由当前 rpcMessageId 对应的 handler 按统一异常路径处理。</p>
     *
     * @param message 原始 RPC 消息
     * @return 框架层 ERROR 消息
     */
    private RpcMessage buildUnsupportedMessage(RpcMessage message) {
        RpcMessage error = new RpcMessage();
        error.setType(RpcMessageType.ERROR);
        error.setRpcMessageId(message.getRpcMessageId());
        error.setErrorMessage("unsupported client inbound rpc message type: " + message.getType() + ", supported types: " + CLIENT_INBOUND_TYPES);
        return error;
    }

    /**
     * 分发连接关闭事件。
     *
     * <p>连接关闭时，可能仍有普通请求等待响应，也可能仍有流式调用等待后续数据；
     * 因此需要遍历当前注册表快照，将关闭事件通知给所有未完成的 handler。</p>
     *
     * @param cause 连接关闭原因
     */
    public void dispatchConnectionClosed(Throwable cause, ChannelId closedChannelId) {
        // TODO:修复 bug：过去连接断开会通知注册表全部 handler，导致“某一条连接断开误伤其他连接上的调用”。现在按 closedChannelId 精确过滤，只通知同一连接上的 handler。
        // listRegistrations 返回快照，避免遍历过程中 handler 自己 remove 导致并发修改问题。
        List<RpcMessageHandlerRegistration> registrations = handlerRegistry.listRegistrations();
        for (RpcMessageHandlerRegistration registration : registrations) {
            if (closedChannelId != null && !closedChannelId.equals(registration.getChannelId())) {
                continue;
            }
            registration.getHandler().handleConnectionClosed(cause, registration);
        }
    }
}