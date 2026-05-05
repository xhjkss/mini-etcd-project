package com.xhj.etcd.rpc.netty;

import com.xhj.etcd.rpc.RpcMessage;
import com.xhj.etcd.rpc.RpcMessageType;
import com.xhj.etcd.rpc.fixture.EchoRequest;
import com.xhj.etcd.serializer.impl.JdkSerializer;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import org.junit.Assert;
import org.junit.Test;

/**
 * RpcMessageCodecTest
 *
 * @author XJks
 * @description RPC 消息编解码器测试，验证 RpcMessage 与 ByteBuf 的互转以及长度字段拆包流程。
 */
public class RpcMessageCodecTest {

    @Test
    public void shouldEncodeAndDecodeRpcMessage() {
        JdkSerializer serializer = new JdkSerializer();
        EmbeddedChannel channel = new EmbeddedChannel(new RpcMessageCodec(serializer));
        RpcMessage request = request(serializer, "rpc-1", "hello");
        ByteBuf encoded = null;
        try {
            Assert.assertTrue(channel.writeOutbound(request));
            encoded = channel.readOutbound();
            Assert.assertNotNull(encoded);

            Assert.assertTrue(channel.writeInbound(encoded.retain()));
            RpcMessage decoded = channel.readInbound();
            Assert.assertNotNull(decoded);

            EchoRequest body = serializer.deserialize(decoded.getData(), EchoRequest.class);
            Assert.assertEquals(RpcMessageType.REQUEST, decoded.getType());
            Assert.assertEquals("rpc-1", decoded.getRpcMessageId());
            Assert.assertEquals("EchoService", decoded.getServiceName());
            Assert.assertEquals("echo", decoded.getMethodName());
            Assert.assertEquals("hello", body.getMessage());
            Assert.assertNull(channel.readInbound());
        } finally {
            if (encoded != null) {
                encoded.release();
            }
            channel.finishAndReleaseAll();
        }
    }

    @Test
    public void shouldEncodeAndDecodeErrorMessage() {
        JdkSerializer serializer = new JdkSerializer();
        EmbeddedChannel channel = new EmbeddedChannel(new RpcMessageCodec(serializer));
        RpcMessage error = new RpcMessage();
        error.setType(RpcMessageType.ERROR);
        error.setRpcMessageId("rpc-error");
        error.setErrorMessage("boom");
        ByteBuf encoded = null;
        try {
            Assert.assertTrue(channel.writeOutbound(error));
            encoded = channel.readOutbound();
            Assert.assertNotNull(encoded);

            Assert.assertTrue(channel.writeInbound(encoded.retain()));
            RpcMessage decoded = channel.readInbound();
            Assert.assertNotNull(decoded);

            Assert.assertEquals(RpcMessageType.ERROR, decoded.getType());
            Assert.assertEquals("rpc-error", decoded.getRpcMessageId());
            Assert.assertEquals("boom", decoded.getErrorMessage());
            Assert.assertNull(channel.readInbound());
        } finally {
            if (encoded != null) {
                encoded.release();
            }
            channel.finishAndReleaseAll();
        }
    }

    @Test
    public void shouldDecodeMultipleFramedMessagesInOneByteBuf() {
        JdkSerializer serializer = new JdkSerializer();
        EmbeddedChannel channel = new EmbeddedChannel(
                new LengthFieldBasedFrameDecoder(16 * 1024 * 1024, 0, 4, 0, 4),
                new RpcMessageCodec(serializer)
        );
        ByteBuf combined = Unpooled.buffer();
        writeFramedMessage(combined, serializer, request(serializer, "rpc-1", "first"));
        writeFramedMessage(combined, serializer, request(serializer, "rpc-2", "second"));
        try {
            Assert.assertTrue(channel.writeInbound(combined.retain()));

            RpcMessage firstDecoded = channel.readInbound();
            RpcMessage secondDecoded = channel.readInbound();

            Assert.assertNotNull(firstDecoded);
            Assert.assertNotNull(secondDecoded);
            Assert.assertEquals("rpc-1", firstDecoded.getRpcMessageId());
            Assert.assertEquals("rpc-2", secondDecoded.getRpcMessageId());

            EchoRequest firstBody = serializer.deserialize(firstDecoded.getData(), EchoRequest.class);
            EchoRequest secondBody = serializer.deserialize(secondDecoded.getData(), EchoRequest.class);
            Assert.assertEquals("first", firstBody.getMessage());
            Assert.assertEquals("second", secondBody.getMessage());
            Assert.assertNull(channel.readInbound());
        } finally {
            combined.release();
            channel.finishAndReleaseAll();
        }
    }

    @Test
    public void shouldEncodeMessageWithLengthFieldAndDecodeBack() {
        JdkSerializer serializer = new JdkSerializer();
        EmbeddedChannel outbound = new EmbeddedChannel(
                new LengthFieldPrepender(4),
                new RpcMessageCodec(serializer)
        );
        EmbeddedChannel inbound = new EmbeddedChannel(
                new LengthFieldBasedFrameDecoder(16 * 1024 * 1024, 0, 4, 0, 4),
                new RpcMessageCodec(serializer)
        );
        ByteBuf frame = null;
        try {
            Assert.assertTrue(outbound.writeOutbound(request(serializer, "rpc-frame", "framed")));
            frame = readAllOutboundBuffers(outbound);
            Assert.assertTrue(inbound.writeInbound(frame.retain()));

            RpcMessage decoded = inbound.readInbound();
            Assert.assertNotNull(decoded);
            Assert.assertEquals("rpc-frame", decoded.getRpcMessageId());

            EchoRequest body = serializer.deserialize(decoded.getData(), EchoRequest.class);
            Assert.assertEquals("framed", body.getMessage());
            Assert.assertNull(inbound.readInbound());
        } finally {
            if (frame != null) {
                frame.release();
            }
            outbound.finishAndReleaseAll();
            inbound.finishAndReleaseAll();
        }
    }

    private void writeFramedMessage(ByteBuf target, JdkSerializer serializer, RpcMessage message) {
        byte[] payload = serializer.serialize(message);
        target.writeInt(payload.length);
        target.writeBytes(payload);
    }

    private ByteBuf readAllOutboundBuffers(EmbeddedChannel channel) {
        ByteBuf combined = Unpooled.buffer();
        ByteBuf current;
        while ((current = channel.readOutbound()) != null) {
            try {
                combined.writeBytes(current);
            } finally {
                current.release();
            }
        }
        return combined;
    }

    private RpcMessage request(JdkSerializer serializer, String rpcMessageId, String value) {
        EchoRequest request = new EchoRequest();
        request.setMessage(value);

        RpcMessage message = new RpcMessage();
        message.setType(RpcMessageType.REQUEST);
        message.setRpcMessageId(rpcMessageId);
        message.setServiceName("EchoService");
        message.setMethodName("echo");
        message.setData(serializer.serialize(request));
        return message;
    }
}
