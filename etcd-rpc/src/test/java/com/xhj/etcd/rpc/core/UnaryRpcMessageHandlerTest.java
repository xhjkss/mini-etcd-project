package com.xhj.etcd.rpc.core;

import com.xhj.etcd.rpc.RpcException;
import com.xhj.etcd.rpc.RpcMessage;
import com.xhj.etcd.rpc.RpcMessageHandlerRegistration;
import com.xhj.etcd.rpc.RpcMessageHandlerRegistry;
import com.xhj.etcd.rpc.RpcMessageType;
import com.xhj.etcd.rpc.fixture.EchoResponse;
import com.xhj.etcd.serializer.impl.JdkSerializer;
import org.junit.Assert;
import org.junit.Test;

public class UnaryRpcMessageHandlerTest {

    @Test
    public void shouldCompleteFutureWhenResponseArrives() {
        JdkSerializer serializer = new JdkSerializer();
        UnaryRpcMessageHandler<EchoResponse> handler = new UnaryRpcMessageHandler<>(serializer, EchoResponse.class);
        RpcMessageHandlerRegistry registry = new RpcMessageHandlerRegistry();
        RpcMessageHandlerRegistration registration = registry.register("rpc-1", handler);

        EchoResponse response = new EchoResponse();
        response.setMessage("ok");
        handler.handle(message("rpc-1", RpcMessageType.RESPONSE, serializer.serialize(response), null), registration);

        Assert.assertEquals("ok", handler.await(1000L).getMessage());
        Assert.assertNull(registry.get("rpc-1"));
    }

    @Test
    public void shouldCompleteFutureWhenHeartbeatArrives() {
        JdkSerializer serializer = new JdkSerializer();
        UnaryRpcMessageHandler<String> handler = new UnaryRpcMessageHandler<>(serializer, String.class);
        RpcMessageHandlerRegistry registry = new RpcMessageHandlerRegistry();
        RpcMessageHandlerRegistration registration = registry.register("rpc-1", handler);

        handler.handle(message("rpc-1", RpcMessageType.HEARTBEAT, serializer.serialize("OK"), null), registration);

        Assert.assertEquals("OK", handler.await(1000L));
        Assert.assertNull(registry.get("rpc-1"));
    }

    @Test
    public void shouldFailWhenErrorMessageArrives() {
        UnaryRpcMessageHandler<EchoResponse> handler = new UnaryRpcMessageHandler<>(new JdkSerializer(), EchoResponse.class);
        RpcMessageHandlerRegistry registry = new RpcMessageHandlerRegistry();
        RpcMessageHandlerRegistration registration = registry.register("rpc-1", handler);

        handler.handle(message("rpc-1", RpcMessageType.ERROR, null, "service not found"), registration);

        assertAwaitFailed(handler);
        Assert.assertNull(registry.get("rpc-1"));
    }

    @Test
    public void shouldFailWhenUnsupportedStreamMessageArrives() {
        UnaryRpcMessageHandler<EchoResponse> handler = new UnaryRpcMessageHandler<>(new JdkSerializer(), EchoResponse.class);
        RpcMessageHandlerRegistry registry = new RpcMessageHandlerRegistry();
        RpcMessageHandlerRegistration registration = registry.register("rpc-1", handler);

        handler.handle(message("rpc-1", RpcMessageType.STREAM, null, null), registration);

        assertAwaitFailed(handler);
        Assert.assertNull(registry.get("rpc-1"));
    }

    @Test
    public void shouldFailWhenConnectionClosed() {
        UnaryRpcMessageHandler<EchoResponse> handler = new UnaryRpcMessageHandler<>(new JdkSerializer(), EchoResponse.class);
        RpcMessageHandlerRegistry registry = new RpcMessageHandlerRegistry();
        RpcMessageHandlerRegistration registration = registry.register("rpc-1", handler);

        handler.handleConnectionClosed(new IllegalStateException("closed"), registration);

        assertAwaitFailed(handler);
        Assert.assertNull(registry.get("rpc-1"));
    }

    @Test
    public void shouldRemoveRegistrationWhenDeserializeFailed() {
        UnaryRpcMessageHandler<EchoResponse> handler = new UnaryRpcMessageHandler<>(new JdkSerializer(), EchoResponse.class);
        RpcMessageHandlerRegistry registry = new RpcMessageHandlerRegistry();
        RpcMessageHandlerRegistration registration = registry.register("rpc-1", handler);

        handler.handle(message("rpc-1", RpcMessageType.RESPONSE, new byte[]{1, 2, 3}, null), registration);

        assertAwaitFailed(handler);
        Assert.assertNull(registry.get("rpc-1"));
    }

    private RpcMessage message(String rpcMessageId, RpcMessageType type, byte[] data, String errorMessage) {
        RpcMessage message = new RpcMessage();
        message.setRpcMessageId(rpcMessageId);
        message.setType(type);
        message.setData(data);
        message.setErrorMessage(errorMessage);
        return message;
    }

    private void assertAwaitFailed(UnaryRpcMessageHandler<?> handler) {
        try {
            handler.await(1000L);
            Assert.fail("unary rpc should fail");
        } catch (RpcException expected) {
            Assert.assertTrue(expected.getMessage() != null);
        }
    }
}
