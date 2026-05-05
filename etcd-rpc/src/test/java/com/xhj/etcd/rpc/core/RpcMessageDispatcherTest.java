package com.xhj.etcd.rpc.core;

import com.xhj.etcd.rpc.RpcMessage;
import com.xhj.etcd.rpc.RpcMessageHandler;
import com.xhj.etcd.rpc.RpcMessageHandlerRegistration;
import com.xhj.etcd.rpc.RpcMessageHandlerRegistry;
import com.xhj.etcd.rpc.RpcMessageType;
import com.xhj.etcd.serializer.impl.JdkSerializer;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.Assert;
import org.junit.Test;

public class RpcMessageDispatcherTest {

    @Test
    public void shouldRejectClientInboundRequestMessageType() {
        RpcMessageHandlerRegistry registry = new RpcMessageHandlerRegistry();
        ClientRpcMessageDispatcher dispatcher = new ClientRpcMessageDispatcher(registry);
        RecordingRpcMessageHandler handler = new RecordingRpcMessageHandler();
        registry.register("rpc-1", handler);

        RpcMessage message = message("rpc-1", RpcMessageType.REQUEST);
        dispatcher.dispatch(message);

        Assert.assertNotNull(handler.message);
        Assert.assertEquals(RpcMessageType.ERROR, handler.message.getType());
        Assert.assertTrue(handler.message.getErrorMessage().contains("unsupported client inbound"));
    }

    @Test
    public void shouldDispatchClientInboundResponseMessageType() {
        RpcMessageHandlerRegistry registry = new RpcMessageHandlerRegistry();
        ClientRpcMessageDispatcher dispatcher = new ClientRpcMessageDispatcher(registry);
        RecordingRpcMessageHandler handler = new RecordingRpcMessageHandler();
        registry.register("rpc-2", handler);

        RpcMessage message = message("rpc-2", RpcMessageType.RESPONSE);
        dispatcher.dispatch(message);

        Assert.assertSame(message, handler.message);
    }

    @Test
    public void shouldRejectServerInboundResponseMessageType() {
        ServerRpcMessageDispatcher dispatcher = new ServerRpcMessageDispatcher(new JdkSerializer(), null);
        EmbeddedChannel channel = new EmbeddedChannel();

        dispatcher.dispatch(channel, message("rpc-3", RpcMessageType.RESPONSE));

        RpcMessage response = channel.readOutbound();
        Assert.assertNotNull(response);
        Assert.assertEquals(RpcMessageType.ERROR, response.getType());
        Assert.assertEquals("rpc-3", response.getRpcMessageId());
        Assert.assertTrue(response.getErrorMessage().contains("unsupported server inbound"));
    }

    @Test
    public void shouldHandleServerInboundHeartbeatMessageType() {
        JdkSerializer serializer = new JdkSerializer();
        ServerRpcMessageDispatcher dispatcher = new ServerRpcMessageDispatcher(serializer, null);
        EmbeddedChannel channel = new EmbeddedChannel();

        dispatcher.dispatch(channel, message("rpc-4", RpcMessageType.HEARTBEAT));

        RpcMessage response = channel.readOutbound();
        Assert.assertNotNull(response);
        Assert.assertEquals(RpcMessageType.HEARTBEAT, response.getType());
        Assert.assertEquals("rpc-4", response.getRpcMessageId());
        Assert.assertEquals("OK", serializer.deserialize(response.getData(), String.class));
    }

    @Test
    public void shouldNotKeepUnusedCompleteMessageType() {
        RpcMessageType[] values = RpcMessageType.values();
        for (RpcMessageType value : values) {
            Assert.assertNotEquals("COMPLETE", value.name());
        }
    }

    private RpcMessage message(String rpcMessageId, RpcMessageType type) {
        RpcMessage message = new RpcMessage();
        message.setRpcMessageId(rpcMessageId);
        message.setType(type);
        return message;
    }

    private static class RecordingRpcMessageHandler implements RpcMessageHandler {
        private RpcMessage message;

        @Override
        public void handle(RpcMessage message, RpcMessageHandlerRegistration registration) {
            this.message = message;
        }

        @Override
        public void handleConnectionClosed(Throwable cause, RpcMessageHandlerRegistration registration) {
        }
    }
}
