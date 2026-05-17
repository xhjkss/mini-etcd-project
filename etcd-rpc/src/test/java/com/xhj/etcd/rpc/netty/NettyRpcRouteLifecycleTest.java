package com.xhj.etcd.rpc.netty;

import com.xhj.etcd.rpc.NodeEndpoint;
import com.xhj.etcd.rpc.RpcMessage;
import com.xhj.etcd.rpc.RpcMessageHandler;
import com.xhj.etcd.rpc.RpcMessageHandlerRegistration;
import com.xhj.etcd.rpc.RpcMessageType;
import com.xhj.etcd.rpc.fixture.EchoEventService;
import com.xhj.etcd.rpc.fixture.EchoRequest;
import com.xhj.etcd.rpc.fixture.EchoResponse;
import com.xhj.etcd.serializer.Serializer;
import com.xhj.etcd.serializer.impl.JdkSerializer;
import io.netty.channel.Channel;
import org.junit.Assert;
import org.junit.Test;

import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * NettyRpcRouteLifecycleTest
 *
 * @author XJks
 * @description Netty RPC 路由生命周期测试，验证多 rpcMessageId 分发、移除路由和连接关闭通知。
 */
public class NettyRpcRouteLifecycleTest {

    @Test
    public void shouldRouteMultipleRouteIdsByRpcMessageId() throws Exception {
        Serializer serializer = new JdkSerializer();
        NodeEndpoint endpoint = new NodeEndpoint("node-multi-stream", "127.0.0.1", findFreePort());
        NettyRpcServer server = new NettyRpcServer(endpoint, serializer);
        NettyRpcClient client = new NettyRpcClient(serializer, 5000L);
        server.registerService("EchoEventService", new EchoEventService(serializer), "echoEvents");
        server.start();
        try {
            RecordingStreamHandler firstHandler = new RecordingStreamHandler(serializer, 3);
            RecordingStreamHandler secondHandler = new RecordingStreamHandler(serializer, 3);

            EchoRequest firstRequest = request("first-call", "stream-1");
            EchoRequest secondRequest = request("second-call", "stream-2");

            client.sendRequestWithRpcMessageId(endpoint, "EchoEventService", "echoEvents", firstRequest, "stream-1", firstHandler);
            client.sendRequestWithRpcMessageId(endpoint, "EchoEventService", "echoEvents", secondRequest, "stream-2", secondHandler);

            Assert.assertTrue(firstHandler.await(5000L));
            Assert.assertTrue(secondHandler.await(5000L));

            Assert.assertEquals(3, firstHandler.messages.size());
            Assert.assertEquals(3, secondHandler.messages.size());
            Assert.assertEquals("event:first", firstHandler.messages.get(0));
            Assert.assertEquals("event:second", firstHandler.messages.get(1));
            Assert.assertEquals("response:first-call", firstHandler.messages.get(2));
            Assert.assertEquals("event:first", secondHandler.messages.get(0));
            Assert.assertEquals("event:second", secondHandler.messages.get(1));
            Assert.assertEquals("response:second-call", secondHandler.messages.get(2));

            client.removeRpcMessageHandler("stream-1");
            client.removeRpcMessageHandler("stream-2");
        } finally {
            client.shutdown();
            server.stop();
        }
    }

    @Test
    public void shouldStopDispatchingAfterRemovingRouteHandler() throws Exception {
        Serializer serializer = new JdkSerializer();
        NodeEndpoint endpoint = new NodeEndpoint("node-route-close", "127.0.0.1", findFreePort());
        NettyRpcServer server = new NettyRpcServer(endpoint, serializer);
        NettyRpcClient client = new NettyRpcClient(serializer, 5000L);
        server.registerService("EchoEventService", new EchoEventService(serializer), "echoEvents");
        server.start();
        try {
            RecordingStreamHandler handler = new RecordingStreamHandler(serializer, 3);
            client.sendRequestWithRpcMessageId(
                    endpoint,
                    "EchoEventService",
                    "echoEvents",
                    request("first-call", "route-close"),
                    "route-close",
                    handler);

            Assert.assertTrue(handler.await(5000L));
            Assert.assertEquals(3, handler.messages.size());

            client.removeRpcMessageHandler("route-close");
            client.sendRequestWithRpcMessageId(
                    endpoint,
                    "EchoEventService",
                    "echoEvents",
                    request("second-call", "route-close"),
                    "route-close",
                    null);
            Thread.sleep(500L);

            Assert.assertEquals(3, handler.messages.size());
        } finally {
            client.shutdown();
            server.stop();
        }
    }

    @Test
    public void shouldNotifyRouteHandlerWhenClientConnectionClosed() throws Exception {
        Serializer serializer = new JdkSerializer();
        NodeEndpoint endpoint = new NodeEndpoint("node-route-inactive", "127.0.0.1", findFreePort());
        NettyRpcServer server = new NettyRpcServer(endpoint, serializer);
        NettyRpcClient client = new NettyRpcClient(serializer, 5000L);
        CountingStreamService service = new CountingStreamService();
        server.registerService("CountingStreamService", service, "hold");
        server.start();
        try {
            CloseAwareStreamHandler handler = new CloseAwareStreamHandler();
            client.sendRequestWithRpcMessageId(endpoint,
                    "CountingStreamService",
                    "hold",
                    request("first", "stream-inactive"),
                    "stream-inactive",
                    handler);
            Assert.assertTrue(service.awaitInvocationCount(1, 5000L));

            client.shutdown();

            Assert.assertTrue(handler.awaitClosed(5000L));
            Assert.assertTrue(handler.closed.get());
        } finally {
            server.stop();
        }
    }

    private EchoRequest request(String message, String eventId) {
        EchoRequest request = new EchoRequest();
        request.setMessage(message);
        request.setEventId(eventId);
        return request;
    }

    private int findFreePort() throws Exception {
        ServerSocket serverSocket = new ServerSocket(0);
        try {
            return serverSocket.getLocalPort();
        } finally {
            serverSocket.close();
        }
    }

    private static class RecordingStreamHandler implements RpcMessageHandler {
        private final Serializer serializer;
        private final CountDownLatch latch;
        private final List<String> messages = Collections.synchronizedList(new ArrayList<>());

        private RecordingStreamHandler(Serializer serializer, int expectedMessageCount) {
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

    private static class CloseAwareStreamHandler implements RpcMessageHandler {
        private final CountDownLatch closeLatch = new CountDownLatch(1);
        private final AtomicBoolean closed = new AtomicBoolean(false);

        @Override
        public void handle(RpcMessage message, RpcMessageHandlerRegistration registration) {
        }

        @Override
        public void handleConnectionClosed(Throwable cause, RpcMessageHandlerRegistration registration) {
            closed.set(true);
            closeLatch.countDown();
            registration.remove();
        }

        private boolean awaitClosed(long timeoutMillis) throws InterruptedException {
            return closeLatch.await(timeoutMillis, TimeUnit.MILLISECONDS);
        }
    }

    public static class CountingStreamService {
        private final AtomicInteger invocationCount = new AtomicInteger(0);
        private volatile CountDownLatch invocationLatch = new CountDownLatch(1);

        public EchoResponse hold(EchoRequest request, Channel channel) {
            invocationCount.incrementAndGet();
            invocationLatch.countDown();
            return null;
        }

        private boolean awaitInvocationCount(int expectedCount, long timeoutMillis) throws InterruptedException {
            long deadlineMillis = System.currentTimeMillis() + timeoutMillis;
            while (System.currentTimeMillis() < deadlineMillis) {
                if (invocationCount.get() >= expectedCount) {
                    return true;
                }
                invocationLatch.await(Math.min(50L, Math.max(1L, deadlineMillis - System.currentTimeMillis())), TimeUnit.MILLISECONDS);
            }
            return invocationCount.get() >= expectedCount;
        }
    }
}
