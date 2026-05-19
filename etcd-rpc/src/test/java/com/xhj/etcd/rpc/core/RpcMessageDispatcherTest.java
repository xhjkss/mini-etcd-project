package com.xhj.etcd.rpc.core;

import com.xhj.etcd.rpc.RpcMessage;
import com.xhj.etcd.rpc.RpcMessageHandler;
import com.xhj.etcd.rpc.RpcMessageHandlerRegistration;
import com.xhj.etcd.rpc.RpcMessageHandlerRegistry;
import com.xhj.etcd.rpc.RpcMessageType;
import com.xhj.etcd.serializer.impl.JdkSerializer;
import io.netty.channel.ChannelId;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.Assert;
import org.junit.Test;

public class RpcMessageDispatcherTest {

    @Test
    public void shouldRejectClientInboundRequestMessageType() {
        RpcMessageHandlerRegistry registry = new RpcMessageHandlerRegistry();
        ClientRpcMessageDispatcher dispatcher = new ClientRpcMessageDispatcher(registry);
        RecordingRpcMessageHandler handler = new RecordingRpcMessageHandler();
        registry.register("rpc-1", handler, null);

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
        registry.register("rpc-2", handler, null);

        RpcMessage message = message("rpc-2", RpcMessageType.RESPONSE);
        dispatcher.dispatch(message);

        Assert.assertSame(message, handler.message);
    }

    @Test
    public void shouldOnlyNotifyHandlersOnClosedChannel() {
        RpcMessageHandlerRegistry registry = new RpcMessageHandlerRegistry();
        ClientRpcMessageDispatcher dispatcher = new ClientRpcMessageDispatcher(registry);

        RecordingRpcMessageHandler channelOneHandler = new RecordingRpcMessageHandler();
        RecordingRpcMessageHandler channelTwoHandler = new RecordingRpcMessageHandler();
        ChannelId channelOneId = new FakeChannelId("channel-1");
        ChannelId channelTwoId = new FakeChannelId("channel-2");
        registry.register("rpc-11", channelOneHandler, channelOneId);
        registry.register("rpc-22", channelTwoHandler, channelTwoId);

        dispatcher.dispatchConnectionClosed(new IllegalStateException("closed"), channelOneId);

        Assert.assertNotNull(channelOneHandler.cause);
        Assert.assertNull(channelTwoHandler.cause);
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
        private Throwable cause;

        @Override
        public void handle(RpcMessage message, RpcMessageHandlerRegistration registration) {
            this.message = message;
        }

        @Override
        public void handleConnectionClosed(Throwable cause, RpcMessageHandlerRegistration registration) {
            this.cause = cause;
        }
    }

    private static class FakeChannelId implements ChannelId {

        private final String value;

        private FakeChannelId(String value) {
            this.value = value;
        }

        @Override
        public String asShortText() {
            return value;
        }

        @Override
        public String asLongText() {
            return value;
        }

        @Override
        public int compareTo(ChannelId otherChannelId) {
            if (otherChannelId == null) {
                return 1;
            }
            return asLongText().compareTo(otherChannelId.asLongText());
        }

        @Override
        public boolean equals(Object object) {
            if (this == object) {
                return true;
            }
            if (!(object instanceof FakeChannelId)) {
                return false;
            }
            FakeChannelId other = (FakeChannelId) object;
            return value.equals(other.value);
        }

        @Override
        public int hashCode() {
            return value.hashCode();
        }
    }
}
