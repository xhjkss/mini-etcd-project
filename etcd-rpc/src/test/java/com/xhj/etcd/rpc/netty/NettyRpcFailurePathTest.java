package com.xhj.etcd.rpc.netty;

import com.xhj.etcd.rpc.NodeEndpoint;
import com.xhj.etcd.rpc.RpcException;
import com.xhj.etcd.rpc.fixture.EchoRequest;
import com.xhj.etcd.rpc.fixture.EchoResponse;
import com.xhj.etcd.serializer.impl.JdkSerializer;
import org.junit.Assert;
import org.junit.Test;

import java.net.ServerSocket;

/**
 * NettyRpcFailurePathTest
 *
 * @author XJks
 * @description Netty RPC 失败路径测试，验证心跳失败、不可用端点和服务方法异常。
 */
public class NettyRpcFailurePathTest {

    @Test
    public void shouldHeartbeatSuccessfully() throws Exception {
        NodeEndpoint endpoint = new NodeEndpoint("node-heartbeat", "127.0.0.1", findFreePort());
        NettyRpcServer server = new NettyRpcServer(endpoint, new JdkSerializer());
        NettyRpcClient client = new NettyRpcClient(new JdkSerializer(), 5000L);
        server.start();
        try {
            Assert.assertTrue(client.heartbeat(endpoint));
        } finally {
            client.shutdown();
            server.stop();
        }
    }

    @Test
    public void shouldReturnFalseWhenHeartbeatEndpointDoesNotSpeakRpc() throws Exception {
        ServerSocket silentSocket = new ServerSocket(0);
        NodeEndpoint endpoint = new NodeEndpoint("node-heartbeat-fail", "127.0.0.1", silentSocket.getLocalPort());
        NettyRpcClient client = new NettyRpcClient(new JdkSerializer(), 300L);
        try {
            Assert.assertFalse(client.heartbeat(endpoint));
        } finally {
            client.shutdown();
            silentSocket.close();
        }
    }

    @Test
    public void shouldFailWhenCallingEndpointThatDoesNotSpeakRpc() throws Exception {
        ServerSocket silentSocket = new ServerSocket(0);
        NodeEndpoint endpoint = new NodeEndpoint("node-unavailable", "127.0.0.1", silentSocket.getLocalPort());
        NettyRpcClient client = new NettyRpcClient(new JdkSerializer(), 300L);
        try {
            client.call(endpoint, "EchoService", "echo", request("hello"), EchoResponse.class);
            Assert.fail("non-rpc endpoint should fail");
        } catch (RpcException expected) {
            Assert.assertTrue(expected.getMessage() != null);
        } finally {
            client.shutdown();
            silentSocket.close();
        }
    }

    @Test
    public void shouldReturnErrorWhenServiceMethodThrowsException() throws Exception {
        NodeEndpoint endpoint = new NodeEndpoint("node-service-error", "127.0.0.1", findFreePort());
        NettyRpcServer server = new NettyRpcServer(endpoint, new JdkSerializer());
        NettyRpcClient client = new NettyRpcClient(new JdkSerializer(), 5000L);
        server.registerService("BrokenService", new BrokenService(), "fail");
        server.start();
        try {
            client.call(endpoint, "BrokenService", "fail", request("bad"), EchoResponse.class);
            Assert.fail("service method exception should fail rpc call");
        } catch (RpcException expected) {
            Assert.assertTrue(expected.getMessage() != null);
        } finally {
            client.shutdown();
            server.stop();
        }
    }

    @Test
    public void shouldFailWhenServiceIsNotRegistered() throws Exception {
        NodeEndpoint endpoint = new NodeEndpoint("node-missing-service", "127.0.0.1", findFreePort());
        NettyRpcServer server = new NettyRpcServer(endpoint, new JdkSerializer());
        NettyRpcClient client = new NettyRpcClient(new JdkSerializer(), 5000L);
        server.start();
        try {
            client.call(endpoint, "MissingService", "echo", request("hello"), EchoResponse.class);
            Assert.fail("calling unregistered service should fail");
        } catch (RpcException expected) {
            Assert.assertTrue(expected.getMessage() != null);
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

    public static class BrokenService {
        public EchoResponse fail(EchoRequest request) {
            throw new IllegalStateException("boom");
        }
    }
}
