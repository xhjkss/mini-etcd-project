package com.xhj.etcd.rpc.netty;

import com.xhj.etcd.rpc.RpcMessage;
import com.xhj.etcd.rpc.core.ClientRpcMessageDispatcher;
import io.netty.channel.ChannelId;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

/**
 * NettyRpcClientHandler
 *
 * @author XJks
 * @description Netty 客户端 ChannelHandler，负责接收服务端响应消息，并将连接关闭事件通知给等待中的调用方。
 *
 * <p>TODO: 该类只负责 Netty pipeline 入站事件到 RPC 客户端分发器的转发，不直接处理业务响应，也不根据 serviceName/methodName 做服务路由。</p>
 */
public class NettyRpcClientHandler extends SimpleChannelInboundHandler<RpcMessage> {

    /**
     * 客户端 RPC 消息分发器。
     *
     * <p>负责根据 RpcMessage.rpcMessageId 找到请求发送前注册的 handler，
     * 并将响应、流式数据、心跳或错误消息回调给等待中的调用方。</p>
     */
    private final ClientRpcMessageDispatcher messageDispatcher;

    public NettyRpcClientHandler(ClientRpcMessageDispatcher messageDispatcher) {
        this.messageDispatcher = messageDispatcher;
    }

    /**
     * 处理服务端返回的 RPC 消息。
     *
     * <p>Netty 解码得到 RpcMessage 后，本方法不直接处理响应内容，
     * 只把消息交给 ClientRpcMessageDispatcher 按 rpcMessageId 分发。</p>
     *
     * @param ctx     Netty Channel 上下文
     * @param message 服务端返回的 RPC 消息
     */
    @Override
    protected void channelRead0(ChannelHandlerContext ctx, RpcMessage message) {
        messageDispatcher.dispatch(message);
    }

    /**
     * 处理客户端连接断开事件。
     *
     * <p>连接断开后，当前 Channel 上未完成的一元调用或流式调用都不可能继续收到响应，
     * 因此需要通知注册表中仍在等待的 handler，让调用方尽快异常结束。</p>
     *
     * @param ctx Netty Channel 上下文
     */
    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        ChannelId closedChannelId = ctx.channel() == null ? null : ctx.channel().id();
        messageDispatcher.dispatchConnectionClosed(new IllegalStateException("netty rpc channel inactive"), closedChannelId);
    }

    /**
     * 处理 Netty pipeline 异常。
     *
     * <p>发生编解码异常、网络异常或其他 pipeline 异常时，主动关闭当前连接；
     * 连接关闭后会触发 channelInactive，并由 channelInactive 统一通知等待中的 handler。</p>
     *
     * @param ctx   Netty Channel 上下文
     * @param cause 异常原因
     */
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        ctx.close();
    }
}