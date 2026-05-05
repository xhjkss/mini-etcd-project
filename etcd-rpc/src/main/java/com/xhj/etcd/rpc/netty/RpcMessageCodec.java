package com.xhj.etcd.rpc.netty;

import com.xhj.etcd.rpc.RpcMessage;
import com.xhj.etcd.serializer.Serializer;
import com.xhj.etcd.serializer.SerializerRegistry;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageCodec;

import java.util.List;

/**
 * RpcMessageCodec
 *
 * @author XJks
 * @description RPC 消息编解码器，负责在 Netty ByteBuf 和 RpcMessage 之间转换。
 *
 * <p>TODO: 该编解码器只负责单个 RpcMessage 的序列化和反序列化，不负责 TCP 粘包/半包处理；消息边界由 pipeline 中的长度字段编解码器保证。</p>
 */
public class RpcMessageCodec extends MessageToMessageCodec<ByteBuf, RpcMessage> {

    /**
     * RpcMessage 序列化器。
     *
     * <p>RpcMessage 外层信封和 RpcMessage.data 里的业务载荷，当前默认都使用 etcd-serializer 模块提供的默认 Serializer。</p>
     *
     * <p>TODO: SPI 加载和默认 Serializer 选择只属于 etcd-serializer 模块；etcd-rpc 不再自建 envelope serializer。</p>
     */
    private final Serializer serializer;

    public RpcMessageCodec() {
        this(SerializerRegistry.getDefaultSerializer());
    }

    public RpcMessageCodec(Serializer serializer) {
        this.serializer = serializer;
    }

    // ==================== Encode RpcMessage ====================

    /**
     * 编码 RPC 消息。
     *
     * <p>将 RpcMessage 序列化为字节数组后包装成 ByteBuf。
     * 出站消息的长度字段由 pipeline 中的 LengthFieldPrepender 负责追加。</p>
     *
     * @param ctx     Netty Channel 上下文
     * @param message RPC 消息
     * @param out     编码结果输出列表
     */
    @Override
    protected void encode(ChannelHandlerContext ctx, RpcMessage message, List<Object> out) {
        byte[] data = serializer.serialize(message);
        out.add(Unpooled.wrappedBuffer(data));
    }

    // ==================== Decode RpcMessage ====================

    /**
     * 解码 RPC 消息。
     *
     * <p>入站 ByteBuf 已经由 LengthFieldBasedFrameDecoder 按消息长度切分完成，
     * 因此这里可以直接读取完整字节数组并反序列化为 RpcMessage。</p>
     *
     * @param ctx     Netty Channel 上下文
     * @param byteBuf 单个完整 RpcMessage 对应的字节缓冲区
     * @param out     解码结果输出列表
     */
    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf byteBuf, List<Object> out) {
        byte[] data = new byte[byteBuf.readableBytes()];
        byteBuf.readBytes(data);
        out.add(serializer.deserialize(data, RpcMessage.class));
    }
}