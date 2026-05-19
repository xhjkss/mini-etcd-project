package com.xhj.etcd.rpc;

import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class RpcMessageHandlerRegistryTest {

    @Test
    public void shouldRegisterAndGetHandlerRegistration() {
        RpcMessageHandlerRegistry registry = new RpcMessageHandlerRegistry();
        RecordingHandler handler = new RecordingHandler();

        RpcMessageHandlerRegistration registration = registry.register("rpc-1", handler, null);

        Assert.assertSame(registration, registry.get("rpc-1"));
        Assert.assertSame(handler, registration.getHandler());
        Assert.assertEquals("rpc-1", registration.getRpcMessageId());
    }

    @Test
    public void shouldRejectInvalidRegistrationArguments() {
        RpcMessageHandlerRegistry registry = new RpcMessageHandlerRegistry();
        try {
            registry.register(" ", new RecordingHandler(), null);
            Assert.fail("empty rpcMessageId should be rejected");
        } catch (IllegalArgumentException expected) {
            Assert.assertTrue(expected.getMessage().contains("rpcMessageId"));
        }
        try {
            registry.register("rpc-1", null, null);
            Assert.fail("null handler should be rejected");
        } catch (IllegalArgumentException expected) {
            Assert.assertTrue(expected.getMessage().contains("handler"));
        }
    }

    @Test
    public void shouldRemoveRegistrationByRpcMessageId() {
        RpcMessageHandlerRegistry registry = new RpcMessageHandlerRegistry();
        registry.register("rpc-1", new RecordingHandler(), null);

        registry.remove("rpc-1");

        Assert.assertNull(registry.get("rpc-1"));
    }

    @Test
    public void shouldOnlyRemoveWhenRegistrationMatches() {
        RpcMessageHandlerRegistry registry = new RpcMessageHandlerRegistry();
        RecordingHandler oldHandler = new RecordingHandler();
        RecordingHandler newHandler = new RecordingHandler();

        RpcMessageHandlerRegistration oldRegistration = registry.register("rpc-1", oldHandler, null);
        RpcMessageHandlerRegistration newRegistration = registry.register("rpc-1", newHandler, null);

        boolean removed = registry.removeIfMatch("rpc-1", oldRegistration);

        Assert.assertFalse(removed);
        Assert.assertSame(newRegistration, registry.get("rpc-1"));
        Assert.assertSame(newHandler, registry.get("rpc-1").getHandler());
    }

    @Test
    public void shouldRemoveCurrentRegistrationWhenRegistrationMatches() {
        RpcMessageHandlerRegistry registry = new RpcMessageHandlerRegistry();
        RecordingHandler handler = new RecordingHandler();
        RpcMessageHandlerRegistration registration = registry.register("rpc-1", handler, null);

        boolean removed = registry.removeIfMatch("rpc-1", registration);

        Assert.assertTrue(removed);
        Assert.assertNull(registry.get("rpc-1"));
    }

    @Test
    public void shouldRemoveCurrentRegistrationThroughRegistrationHandle() {
        RpcMessageHandlerRegistry registry = new RpcMessageHandlerRegistry();
        RecordingHandler handler = new RecordingHandler();
        RpcMessageHandlerRegistration registration = registry.register("rpc-1", handler, null);

        registration.remove();

        Assert.assertNull(registry.get("rpc-1"));
    }

    @Test
    public void shouldReturnRegistrationSnapshotAndClearRegistry() {
        RpcMessageHandlerRegistry registry = new RpcMessageHandlerRegistry();
        registry.register("rpc-1", new RecordingHandler(), null);
        registry.register("rpc-2", new RecordingHandler(), null);

        List<RpcMessageHandlerRegistration> registrations = registry.listRegistrations();
        registry.clear();

        Assert.assertEquals(2, registrations.size());
        Assert.assertTrue(registry.listRegistrations().isEmpty());
    }

    private static class RecordingHandler implements RpcMessageHandler {
        @Override
        public void handle(RpcMessage message, RpcMessageHandlerRegistration registration) {
        }

        @Override
        public void handleConnectionClosed(Throwable cause, RpcMessageHandlerRegistration registration) {
        }
    }
}
