package com.xhj.etcd.rpc.netty;

import com.xhj.etcd.rpc.NodeEndpoint;
import com.xhj.etcd.rpc.RpcMessage;
import com.xhj.etcd.rpc.RpcMessageHandler;
import com.xhj.etcd.rpc.RpcMessageHandlerRegistration;
import com.xhj.etcd.rpc.RpcMessageType;
import com.xhj.etcd.rpc.fixture.EchoEventService;
import com.xhj.etcd.rpc.fixture.EchoRequest;
import com.xhj.etcd.rpc.fixture.EchoResponse;
import com.xhj.etcd.rpc.fixture.EchoService;
import com.xhj.etcd.serializer.Serializer;
import com.xhj.etcd.serializer.impl.HessianSerializer;
import com.xhj.etcd.serializer.impl.JdkSerializer;
import com.xhj.etcd.serializer.impl.JsonSerializer;
import org.junit.Assert;
import org.junit.Test;

import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * NettyRpcIntegrationTest
 *
 * @author XJks
 * @description Netty RPC 集成测试，验证不同序列化器下的一元调用和基础流式响应。
 */
public class NettyRpcIntegrationTest {

    @Test
    public void shouldCallEchoServiceWithJdkSerializer() throws Exception {
        shouldCallEchoServiceByServiceAndMethod(new JdkSerializer());
    }

    @Test
    public void shouldCallEchoServiceWithJsonSerializer() throws Exception {
        shouldCallEchoServiceByServiceAndMethod(new JsonSerializer());
    }

    @Test
    public void shouldCallEchoServiceWithHessianSerializer() throws Exception {
        shouldCallEchoServiceByServiceAndMethod(new HessianSerializer());
    }

    @Test
    public void shouldDispatchEventMessagesWithJdkSerializer() throws Exception {
        Serializer serializer = new JdkSerializer();
        NodeEndpoint endpoint = new NodeEndpoint("node-event", "127.0.0.1", findFreePort());
        NettyRpcServer server = new NettyRpcServer(endpoint, serializer);
        NettyRpcClient client = new NettyRpcClient(serializer, 5000L);
        server.registerService("EchoEventService", new EchoEventService(serializer), "echoEvents");
        server.start();
        try {
            String eventId = "event-stream-integration";
            EchoEventHandler handler = new EchoEventHandler(serializer, 3);

            EchoRequest request = buildEchoRequest("call");
            request.setEventId(eventId);
            client.sendRequestWithRpcMessageId(
                    endpoint,
                    "EchoEventService",
                    "echoEvents",
                    request,
                    eventId,
                    handler);

            Assert.assertTrue(handler.await(5000L));
            Assert.assertEquals(3, handler.messages.size());
            Assert.assertEquals("event:first", handler.messages.get(0));
            Assert.assertEquals("event:second", handler.messages.get(1));
            Assert.assertEquals("response:call", handler.messages.get(2));
            client.removeRpcMessageHandler(eventId);
        } finally {
            client.shutdown();
            server.stop();
        }
    }

    private EchoRequest buildEchoRequest(String message) {
        EchoRequest request = new EchoRequest();
        request.setMessage(message);
        return request;
    }

    private void shouldCallEchoServiceByServiceAndMethod(Serializer serializer) throws Exception {
        NodeEndpoint endpoint = new NodeEndpoint("node-echo-" + serializer.name(), "127.0.0.1", findFreePort());
        NettyRpcServer server = new NettyRpcServer(endpoint, serializer);
        NettyRpcClient client = new NettyRpcClient(serializer, 5000L);
        server.registerService("EchoService", new EchoService(), "echo");
        server.start();
        try {
            EchoRequest request = new EchoRequest();
            request.setMessage(serializer.name());

            EchoResponse response = client.call(endpoint, "EchoService", "echo", request, EchoResponse.class);

            Assert.assertEquals("echo:" + serializer.name(), response.getMessage());
        } finally {
            client.shutdown();
            server.stop();
        }
    }

    private int findFreePort() throws Exception {
        ServerSocket serverSocket = new ServerSocket(0);
        try {
            return serverSocket.getLocalPort();
        } finally {
            serverSocket.close();
        }
    }

    private static class EchoEventHandler implements RpcMessageHandler {
        private final Serializer serializer;
        private final CountDownLatch latch;
        private final List<String> messages = Collections.synchronizedList(new ArrayList<>());

        private EchoEventHandler(Serializer serializer, int expectedMessageCount) {
            this.serializer = serializer;
            this.latch = new CountDownLatch(expectedMessageCount);
        }

        @Override
        public void handle(RpcMessage message, RpcMessageHandlerRegistration registration) {
            if (message.getType() == RpcMessageType.STREAM || message.getType() == RpcMessageType.RESPONSE) {
                EchoResponse response = serializer.deserialize(message.getData(), EchoResponse.class);
                messages.add(response.getMessage());
                latch.countDown();
            }
        }

        @Override
        public void handleConnectionClosed(Throwable cause, RpcMessageHandlerRegistration registration) {
            registration.remove();
        }

        private boolean await(long timeoutMillis) throws InterruptedException {
            return latch.await(timeoutMillis, TimeUnit.MILLISECONDS);
        }
    }
}
