package com.xhj.etcd.rpc.core;

import com.xhj.etcd.rpc.fixture.EchoRequest;
import com.xhj.etcd.rpc.fixture.EchoResponse;
import io.netty.channel.Channel;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Method;

public class RpcMethodDefinitionTest {

    @Test
    public void shouldRecognizeNormalRpcMethod() throws Exception {
        Method method = Service.class.getMethod("echo", EchoRequest.class);

        Assert.assertTrue(RpcMethodDefinition.isRpcMethod(method));

        RpcMethodDefinition definition = new RpcMethodDefinition("echo", method);
        Assert.assertEquals("echo", definition.getMethodName());
        Assert.assertEquals(EchoRequest.class, definition.getRequestClass());
        Assert.assertFalse(definition.isChannelAware());
    }

    @Test
    public void shouldRecognizeChannelAwareRpcMethod() throws Exception {
        Method method = Service.class.getMethod("echoWithChannel", EchoRequest.class, Channel.class);

        Assert.assertTrue(RpcMethodDefinition.isRpcMethod(method));

        RpcMethodDefinition definition = new RpcMethodDefinition("echoWithChannel", method);
        Assert.assertEquals(EchoRequest.class, definition.getRequestClass());
        Assert.assertTrue(definition.isChannelAware());
    }

    @Test
    public void shouldRejectInvalidRpcMethodSignature() throws Exception {
        Assert.assertFalse(RpcMethodDefinition.isRpcMethod(Service.class.getMethod("noArg")));
        Assert.assertFalse(RpcMethodDefinition.isRpcMethod(Service.class.getMethod("badSecondArg", EchoRequest.class, String.class)));
    }

    public static class Service {
        public EchoResponse echo(EchoRequest request) {
            return new EchoResponse();
        }

        public EchoResponse echoWithChannel(EchoRequest request, Channel channel) {
            return new EchoResponse();
        }

        public EchoResponse noArg() {
            return new EchoResponse();
        }

        public EchoResponse badSecondArg(EchoRequest request, String ignored) {
            return new EchoResponse();
        }
    }
}
