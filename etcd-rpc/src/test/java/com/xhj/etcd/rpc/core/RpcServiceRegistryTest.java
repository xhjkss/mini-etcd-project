package com.xhj.etcd.rpc.core;

import com.xhj.etcd.rpc.RpcException;
import com.xhj.etcd.rpc.fixture.EchoRequest;
import com.xhj.etcd.rpc.fixture.EchoResponse;
import org.junit.Assert;
import org.junit.Test;

public class RpcServiceRegistryTest {

    @Test
    public void shouldRegisterExplicitExportedMethodsOnly() {
        RpcServiceRegistry registry = new RpcServiceRegistry();
        registry.registerService("EchoService", new Service(), "echo");

        RpcServiceDefinition definition = registry.getServiceDefinition("EchoService");

        Assert.assertNotNull(definition);
        Assert.assertNotNull(definition.getMethodDefinition("echo"));
        Assert.assertNull(definition.getMethodDefinition("hidden"));
    }

    @Test
    public void shouldRejectEmptyServiceNameAndEmptyMethodNames() {
        RpcServiceRegistry registry = new RpcServiceRegistry();
        try {
            registry.registerService(" ", new Service(), "echo");
            Assert.fail("empty service name should fail");
        } catch (IllegalArgumentException expected) {
            Assert.assertTrue(expected.getMessage().contains("serviceName"));
        }
        try {
            registry.registerService("EchoService", new Service());
            Assert.fail("empty method names should fail");
        } catch (IllegalArgumentException expected) {
            Assert.assertTrue(expected.getMessage().contains("methodNames"));
        }
    }

    @Test
    public void shouldRejectMissingExportedMethod() {
        RpcServiceRegistry registry = new RpcServiceRegistry();
        try {
            registry.registerService("EchoService", new Service(), "missing");
            Assert.fail("missing exported method should fail");
        } catch (RpcException expected) {
            Assert.assertTrue(expected.getMessage().contains("not found"));
        }
    }

    @Test
    public void shouldReplaceServiceDefinitionWhenServiceNameRegisteredAgain() {
        RpcServiceRegistry registry = new RpcServiceRegistry();
        registry.registerService("EchoService", new Service(), "echo");
        RpcServiceDefinition oldDefinition = registry.getServiceDefinition("EchoService");

        registry.registerService("EchoService", new Service(), "hidden");
        RpcServiceDefinition newDefinition = registry.getServiceDefinition("EchoService");

        Assert.assertNotSame(oldDefinition, newDefinition);
        Assert.assertNull(newDefinition.getMethodDefinition("echo"));
        Assert.assertNotNull(newDefinition.getMethodDefinition("hidden"));
    }

    public static class Service {
        public EchoResponse echo(EchoRequest request) {
            return new EchoResponse();
        }

        public EchoResponse hidden(EchoRequest request) {
            return new EchoResponse();
        }
    }
}
