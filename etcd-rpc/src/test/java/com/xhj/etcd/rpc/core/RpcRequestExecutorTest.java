package com.xhj.etcd.rpc.core;

import com.xhj.etcd.rpc.RpcMessage;
import com.xhj.etcd.rpc.RpcMessageType;
import com.xhj.etcd.rpc.fixture.EchoRequest;
import com.xhj.etcd.rpc.fixture.EchoResponse;
import com.xhj.etcd.serializer.impl.JdkSerializer;
import io.netty.channel.Channel;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.TimeUnit;

public class RpcRequestExecutorTest {

    @Test
    public void shouldWriteErrorWhenServiceMissing() {
        JdkSerializer serializer = new JdkSerializer();
        RpcRequestExecutor executor = executor(serializer, new RpcServiceRegistry());
        EmbeddedChannel channel = new EmbeddedChannel();

        executor.submit(channel, requestMessage(serializer, "rpc-1", "MissingService", "echo", request("hello")));

        RpcMessage response = channel.readOutbound();
        Assert.assertEquals(RpcMessageType.ERROR, response.getType());
        Assert.assertEquals("rpc-1", response.getRpcMessageId());
        Assert.assertTrue(response.getErrorMessage().contains("service not found"));
    }

    @Test
    public void shouldWriteErrorWhenMethodNotExported() {
        JdkSerializer serializer = new JdkSerializer();
        RpcServiceRegistry registry = new RpcServiceRegistry();
        registry.registerService("EchoService", new Service(), "echo");
        RpcRequestExecutor executor = executor(serializer, registry);
        EmbeddedChannel channel = new EmbeddedChannel();

        executor.submit(channel, requestMessage(serializer, "rpc-1", "EchoService", "hidden", request("hello")));

        RpcMessage response = channel.readOutbound();
        Assert.assertEquals(RpcMessageType.ERROR, response.getType());
        Assert.assertTrue(response.getErrorMessage().contains("not exported"));
    }

    @Test
    public void shouldInvokeServiceAndWriteResponse() {
        JdkSerializer serializer = new JdkSerializer();
        RpcServiceRegistry registry = new RpcServiceRegistry();
        registry.registerService("EchoService", new Service(), "echo");
        RpcRequestExecutor executor = executor(serializer, registry);
        EmbeddedChannel channel = new EmbeddedChannel();

        executor.submit(channel, requestMessage(serializer, "rpc-1", "EchoService", "echo", request("hello")));

        RpcMessage response = channel.readOutbound();
        EchoResponse body = serializer.deserialize(response.getData(), EchoResponse.class);
        Assert.assertEquals(RpcMessageType.RESPONSE, response.getType());
        Assert.assertEquals("rpc-1", response.getRpcMessageId());
        Assert.assertEquals("echo:hello", body.getMessage());
    }

    @Test
    public void shouldInvokeChannelAwareServiceMethod() {
        JdkSerializer serializer = new JdkSerializer();
        Service service = new Service();
        RpcServiceRegistry registry = new RpcServiceRegistry();
        registry.registerService("EchoService", service, "channelEcho");
        RpcRequestExecutor executor = executor(serializer, registry);
        EmbeddedChannel channel = new EmbeddedChannel();

        executor.submit(channel, requestMessage(serializer, "rpc-1", "EchoService", "channelEcho", request("hello")));

        RpcMessage response = channel.readOutbound();
        EchoResponse body = serializer.deserialize(response.getData(), EchoResponse.class);
        Assert.assertSame(channel, service.lastChannel);
        Assert.assertEquals("channel:hello", body.getMessage());
    }

    @Test
    public void shouldWriteErrorWhenServiceMethodThrows() {
        JdkSerializer serializer = new JdkSerializer();
        RpcServiceRegistry registry = new RpcServiceRegistry();
        registry.registerService("EchoService", new Service(), "fail");
        RpcRequestExecutor executor = executor(serializer, registry);
        EmbeddedChannel channel = new EmbeddedChannel();

        executor.submit(channel, requestMessage(serializer, "rpc-1", "EchoService", "fail", request("bad")));

        RpcMessage response = channel.readOutbound();
        Assert.assertEquals(RpcMessageType.ERROR, response.getType());
        Assert.assertTrue(response.getErrorMessage() != null);
    }

    @Test
    public void shouldNotWriteResponseWhenServiceMethodReturnsNull() {
        JdkSerializer serializer = new JdkSerializer();
        RpcServiceRegistry registry = new RpcServiceRegistry();
        registry.registerService("EchoService", new Service(), "oneWay");
        RpcRequestExecutor executor = executor(serializer, registry);
        EmbeddedChannel channel = new EmbeddedChannel();

        executor.submit(channel, requestMessage(serializer, "rpc-1", "EchoService", "oneWay", request("hello")));

        Assert.assertNull(channel.readOutbound());
    }

    private RpcRequestExecutor executor(JdkSerializer serializer, RpcServiceRegistry registry) {
        return new RpcRequestExecutor(serializer, registry, new RpcMethodInvoker(serializer), new DirectExecutorService());
    }

    private RpcMessage requestMessage(JdkSerializer serializer, String rpcMessageId, String serviceName, String methodName, EchoRequest request) {
        RpcMessage message = new RpcMessage();
        message.setType(RpcMessageType.REQUEST);
        message.setRpcMessageId(rpcMessageId);
        message.setServiceName(serviceName);
        message.setMethodName(methodName);
        message.setData(serializer.serialize(request));
        return message;
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
            this.lastChannel = channel;
            EchoResponse response = new EchoResponse();
            response.setMessage("channel:" + request.getMessage());
            return response;
        }

        public EchoResponse fail(EchoRequest request) {
            throw new IllegalStateException("boom");
        }

        public EchoResponse oneWay(EchoRequest request) {
            return null;
        }
    }

    private static class DirectExecutorService extends AbstractExecutorService {
        private volatile boolean shutdown;

        @Override
        public void shutdown() {
            shutdown = true;
        }

        @Override
        public List<Runnable> shutdownNow() {
            shutdown = true;
            return Collections.emptyList();
        }

        @Override
        public boolean isShutdown() {
            return shutdown;
        }

        @Override
        public boolean isTerminated() {
            return shutdown;
        }

        @Override
        public boolean awaitTermination(long timeout, TimeUnit unit) {
            return true;
        }

        @Override
        public void execute(Runnable command) {
            command.run();
        }
    }
}
