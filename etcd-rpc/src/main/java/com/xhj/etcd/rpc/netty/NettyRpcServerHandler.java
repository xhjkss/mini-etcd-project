package com.xhj.etcd.rpc.netty;

import com.xhj.etcd.rpc.RpcMessage;
import com.xhj.etcd.rpc.core.ServerRpcMessageDispatcher;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

/**
 * NettyRpcServerHandler
 *
 * @author XJks
 * @description Netty 服务端 ChannelHandler，负责接收客户端 RPC 消息，并转交给服务端消息分发器。
 *
 * <p>TODO: 该类只负责 Netty pipeline 入站事件到 RPC 服务端分发器的转发；不直接处理服务路由、方法反射调用和业务线程切换。</p>
 */
public class NettyRpcServerHandler extends SimpleChannelInboundHandler<RpcMessage> {

    /**
     * 服务端 RPC 消息分发器。
     *
     * <p>负责校验服务端入站消息类型，处理心跳消息，并将普通 REQUEST 消息转交给 RpcRequestExecutor。</p>
     */
    private final ServerRpcMessageDispatcher messageDispatcher;

    public NettyRpcServerHandler(ServerRpcMessageDispatcher messageDispatcher) {
        this.messageDispatcher = messageDispatcher;
    }

    /**
     * 处理客户端入站 RPC 消息。
     *
     * <p>RpcMessageCodec 解码得到 RpcMessage 后，本方法只把消息和当前 Channel 交给 ServerRpcMessageDispatcher；后续服务查找、方法调用和响应写回由下游组件完成。</p>
     *
     * @param ctx     Netty Channel 上下文
     * @param message 客户端发送的 RPC 消息
     */
    @Override
    protected void channelRead0(ChannelHandlerContext ctx, RpcMessage message) {
        messageDispatcher.dispatch(ctx.channel(), message);
    }

    /**
     * 处理 Netty pipeline 异常。
     *
     * <p>发生编解码异常、网络异常或其他 pipeline 异常时，关闭当前连接；
     * 对于客户端正在等待的调用，连接关闭会由客户端侧 NettyRpcClientHandler 感知并通知等待方。</p>
     *
     * @param ctx   Netty Channel 上下文
     * @param cause 异常原因
     */
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        ctx.close();
    }
}