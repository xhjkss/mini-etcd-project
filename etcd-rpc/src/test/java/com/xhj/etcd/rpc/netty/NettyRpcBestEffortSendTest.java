package com.xhj.etcd.rpc.netty;

import com.xhj.etcd.rpc.NodeEndpoint;
import com.xhj.etcd.rpc.fixture.EchoRequest;
import com.xhj.etcd.rpc.fixture.EchoResponse;
import com.xhj.etcd.serializer.impl.JdkSerializer;
import org.junit.Assert;
import org.junit.Test;

import java.net.ServerSocket;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * NettyRpcBestEffortSendTest
 *
 * @author XJks
 * @description Netty best-effort 单向发送测试，验证不可达端点不抛异常和可达端点能收到消息。
 */
public class NettyRpcBestEffortSendTest {

    @Test
    public void shouldNotThrowWhenBestEffortSendEndpointDoesNotSpeakRpc() throws Exception {
        ServerSocket silentSocket = new ServerSocket(0);
        NodeEndpoint endpoint = new NodeEndpoint("node-best-effort-silent", "127.0.0.1", silentSocket.getLocalPort());
        NettyRpcClient client = new NettyRpcClient(new JdkSerializer(), 300L);
        try {
            client.send(endpoint, "RecordingService", "record", request("first"));
            client.send(endpoint, "RecordingService", "record", request("second"));
        } finally {
            client.shutdown();
            silentSocket.close();
        }
    }

    @Test
    public void shouldBestEffortSendRequestWithoutWaitingResponse() throws Exception {
        NodeEndpoint endpoint = new NodeEndpoint("node-best-effort-ok", "127.0.0.1", findFreePort());
        NettyRpcServer server = new NettyRpcServer(endpoint, new JdkSerializer());
        NettyRpcClient client = new NettyRpcClient(new JdkSerializer(), 5000L);
        RecordingService service = new RecordingService();
        server.registerService("RecordingService", service, "record");
        server.start();
        try {
            // 先用心跳建立 active Channel，避免测试结果受异步建连调度影响。
            Assert.assertTrue(client.heartbeat(endpoint));

            client.send(endpoint, "RecordingService", "record", request("hello"));

            Assert.assertTrue(service.await(5000L));
            Assert.assertEquals("hello", service.message);
        } finally {
            client.shutdown();
            server.stop();
        }
    }

    private EchoRequest request(String message) {
        EchoRequest request = new EchoRequest();
        request.setMessage(message);
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

    public static class RecordingService {
        private final CountDownLatch latch = new CountDownLatch(1);
        private volatile String message;

        public EchoResponse record(EchoRequest request) {
            this.message = request.getMessage();
            latch.countDown();
            EchoResponse response = new EchoResponse();
            response.setMessage("recorded:" + request.getMessage());
            return response;
        }

        public boolean await(long timeoutMillis) throws InterruptedException {
            return latch.await(timeoutMillis, TimeUnit.MILLISECONDS);
        }
    }
}
