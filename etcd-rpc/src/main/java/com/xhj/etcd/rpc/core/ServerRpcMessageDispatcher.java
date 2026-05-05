package com.xhj.etcd.rpc.core;

import com.xhj.etcd.rpc.RpcMessage;
import com.xhj.etcd.rpc.RpcMessageType;
import com.xhj.etcd.serializer.Serializer;
import io.netty.channel.Channel;

import java.util.EnumSet;
import java.util.Set;

/**
 * ServerRpcMessageDispatcher
 *
 * @author XJks
 * @description 服务端 RPC 消息分发器，负责校验服务端入站消息类型，并将普通请求转交给请求执行器。
 *
 * <p>TODO: 服务端分发器只处理协议方向判断和轻量级心跳响应；真正的服务路由、方法反射调用和业务线程切换由 RpcRequestExecutor 负责。</p>
 */
public class ServerRpcMessageDispatcher {

    /**
     * 服务端允许接收的 RPC 消息类型。
     *
     * <p>服务端入站方向只应该接收客户端请求和心跳消息；
     * 如果收到 RESPONSE、STREAM、ERROR 等客户端入站类型，说明协议方向异常，会直接返回框架层 ERROR。</p>
     */
    private static final Set<RpcMessageType> SERVER_INBOUND_TYPES = EnumSet.of(
            RpcMessageType.REQUEST,
            RpcMessageType.HEARTBEAT
    );

    /**
     * 序列化器。
     *
     * <p>当前主要用于构建心跳响应载荷。</p>
     */
    private final Serializer serializer;

    /**
     * RPC 请求执行器。
     *
     * <p>普通 REQUEST 消息会转交给 requestExecutor，由其完成服务查找、方法调用和响应写回。</p>
     */
    private final RpcRequestExecutor requestExecutor;

    public ServerRpcMessageDispatcher(Serializer serializer, RpcRequestExecutor requestExecutor) {
        this.serializer = serializer;
        this.requestExecutor = requestExecutor;
    }

    /**
     * 分发服务端入站 RPC 消息。
     *
     * <p>处理流程：</p>
     * <p>1) 忽略空连接或空消息；</p>
     * <p>2) 校验消息类型是否允许进入服务端；</p>
     * <p>3) 心跳消息直接在当前分发层响应；</p>
     * <p>4) 普通请求提交给 RpcRequestExecutor 执行。</p>
     *
     * @param channel 当前请求所在连接
     * @param message 服务端收到的 RPC 消息
     */
    public void dispatch(Channel channel, RpcMessage message) {
        if (channel == null || message == null) {
            return;
        }

        // 1) 服务端只接受 REQUEST 和 HEARTBEAT，其他方向的消息统一转为框架层 ERROR。
        if (!SERVER_INBOUND_TYPES.contains(message.getType())) {
            channel.writeAndFlush(buildError(message, "unsupported server inbound rpc message type: " + message.getType() + ", supported types: " + SERVER_INBOUND_TYPES));
            return;
        }

        // 2) 心跳属于轻量级协议消息，直接在 IO 分发层响应，不进入业务线程池。
        if (message.getType() == RpcMessageType.HEARTBEAT) {
            channel.writeAndFlush(buildHeartbeatResponse(message));
            return;
        }

        // 3) 普通请求可能触发反序列化、反射调用和业务逻辑执行，需要交给请求执行器处理。
        requestExecutor.submit(channel, message);
    }

    /**
     * 构建心跳响应消息。
     *
     * <p>心跳响应复用请求的 rpcMessageId，方便客户端将响应归属到本次心跳检测。</p>
     *
     * @param requestMessage 心跳请求消息
     * @return 心跳响应消息
     */
    private RpcMessage buildHeartbeatResponse(RpcMessage requestMessage) {
        RpcMessage response = new RpcMessage();
        response.setType(RpcMessageType.HEARTBEAT);
        response.setRpcMessageId(requestMessage.getRpcMessageId());
        response.setData(serializer.serialize("OK"));
        return response;
    }

    /**
     * 构建服务端框架层错误消息。
     *
     * <p>该方法用于表达协议方向异常、消息类型不支持等 RPC 框架层错误；
     * 业务方法执行失败由 RpcRequestExecutor 在请求执行阶段转换为 ERROR。</p>
     *
     * @param requestMessage 原始请求消息
     * @param errorMessage   错误信息
     * @return RPC 错误消息
     */
    private RpcMessage buildError(RpcMessage requestMessage, String errorMessage) {
        RpcMessage response = new RpcMessage();
        response.setType(RpcMessageType.ERROR);
        response.setRpcMessageId(requestMessage.getRpcMessageId());
        response.setErrorMessage(errorMessage);
        return response;
    }
}