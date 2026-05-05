package com.xhj.etcd.rpc.core;

import com.xhj.etcd.rpc.RpcException;
import com.xhj.etcd.rpc.fixture.EchoRequest;
import com.xhj.etcd.rpc.fixture.EchoResponse;
import com.xhj.etcd.serializer.impl.JdkSerializer;
import io.netty.channel.Channel;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Method;

public class RpcMethodInvokerTest {

    @Test
    public void shouldInvokeNormalRpcMethod() throws Exception {
        JdkSerializer serializer = new JdkSerializer();
        RpcMethodInvoker invoker = new RpcMethodInvoker(serializer);
        Service service = new Service();
        Method method = Service.class.getMethod("echo", EchoRequest.class);
        RpcMethodDefinition definition = new RpcMethodDefinition("echo", method);

        Object result = invoker.invoke(service, definition, serializer.serialize(request("hello")), new EmbeddedChannel());

        Assert.assertEquals("echo:hello", ((EchoResponse) result).getMessage());
    }

    @Test
    public void shouldInvokeChannelAwareRpcMethod() throws Exception {
        JdkSerializer serializer = new JdkSerializer();
        RpcMethodInvoker invoker = new RpcMethodInvoker(serializer);
        Service service = new Service();
        EmbeddedChannel channel = new EmbeddedChannel();
        Method method = Service.class.getMethod("channelEcho", EchoRequest.class, Channel.class);
        RpcMethodDefinition definition = new RpcMethodDefinition("channelEcho", method);

        Object result = invoker.invoke(service, definition, serializer.serialize(request("hello")), channel);

        Assert.assertSame(channel, service.lastChannel);
        Assert.assertEquals("channel:hello", ((EchoResponse) result).getMessage());
    }

    @Test
    public void shouldWrapServiceInvocationFailure() throws Exception {
        JdkSerializer serializer = new JdkSerializer();
        RpcMethodInvoker invoker = new RpcMethodInvoker(serializer);
        Method method = Service.class.getMethod("fail", EchoRequest.class);
        RpcMethodDefinition definition = new RpcMethodDefinition("fail", method);

        try {
            invoker.invoke(new Service(), definition, serializer.serialize(request("bad")), new EmbeddedChannel());
            Assert.fail("service failure should be wrapped as RpcException");
        } catch (RpcException expected) {
            Assert.assertTrue(expected.getMessage().contains("invoke rpc method failed"));
        }
    }

    private EchoRequest request(String message) {
        EchoRequest request = new EchoRequest();
        request.setMessage(message);
        return request;
    }

    public static class Service {
        private Channel lastChannel;

        public EchoResponse echo(EchoRequest request) {
            EchoResponse response = new EchoResponse();
            response.setMessage("echo:" + request.getMessage());
            return response;
        }

        public EchoResponse channelEcho(EchoRequest request, Channel channel) {
            lastChannel = channel;
            EchoResponse response = new EchoResponse();
            response.setMessage("channel:" + request.getMessage());
            return response;
        }

        public EchoResponse fail(EchoRequest request) {
            throw new IllegalStateException("boom");
        }
    }
}
