package com.xhj.etcd.rpc.netty;

import com.xhj.etcd.rpc.NodeEndpoint;
import com.xhj.etcd.rpc.RpcException;
import com.xhj.etcd.rpc.fixture.EchoRequest;
import com.xhj.etcd.rpc.fixture.EchoResponse;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Method;
import java.net.ServerSocket;

public class NettyRpcApiBoundaryTest {

    @Test
    public void shouldNotExposeInternalClientHandlerOperationsAsPublicApi() {
        assertMethodNotPublic("nextRpcMessageId");
        assertMethodNotPublic("registerHandler");
        assertMethodNotPublic("removeHandler");
        assertMethodNotPublic("sendRequest");
        assertNoPublicOpenStreamMethod();
        assertOnlyOnePublicSendRequestWithRpcMessageIdMethod();
    }

    @Test
    public void shouldRequireExplicitServerMethodExports() throws Exception {
        NettyRpcServer server = new NettyRpcServer(new NodeEndpoint("node-export", "127.0.0.1", findFreePort()));
        try {
            server.registerService("BoundaryService", new BoundaryService());
            Assert.fail("service registration without exported methods should fail");
        } catch (IllegalArgumentException expected) {
            Assert.assertTrue(expected.getMessage().contains("methodNames"));
        }
    }

    @Test
    public void shouldRejectAmbiguousExportedMethodSignatures() throws Exception {
        NettyRpcServer server = new NettyRpcServer(new NodeEndpoint("node-ambiguous", "127.0.0.1", findFreePort()));
        try {
            server.registerService("AmbiguousService", new AmbiguousService(), "echo");
            Assert.fail("ambiguous rpc method signatures should fail fast");
        } catch (RpcException expected) {
            Assert.assertTrue(expected.getMessage().contains("ambiguous"));
        }
    }

    @Test
    public void shouldOnlyInvokeExplicitlyExportedServerMethods() throws Exception {
        NodeEndpoint endpoint = new NodeEndpoint("node-boundary", "127.0.0.1", findFreePort());
        NettyRpcServer server = new NettyRpcServer(endpoint);
        NettyRpcClient client = new NettyRpcClient();
        server.registerService("BoundaryService", new BoundaryService(), "echo");
        server.start();
        try {
            EchoResponse response = client.call(endpoint, "BoundaryService", "echo", request("ok"), EchoResponse.class);
            Assert.assertEquals("echo:ok", response.getMessage());

            try {
                client.call(endpoint, "BoundaryService", "hidden", request("bad"), EchoResponse.class);
                Assert.fail("hidden method should not be exported");
            } catch (RpcException expected) {
                Assert.assertTrue(expected.getMessage() != null);
            }
        } finally {
            client.shutdown();
            server.stop();
        }
    }

    private void assertMethodNotPublic(String methodName) {
        Method[] methods = NettyRpcClient.class.getMethods();
        for (Method method : methods) {
            if (method.getName().equals(methodName)) {
                Assert.fail(methodName + " should not be public");
            }
        }
    }

    private void assertNoPublicOpenStreamMethod() {
        int count = 0;
        Method[] methods = NettyRpcClient.class.getMethods();
        for (Method method : methods) {
            if (method.getName().equals("openStream")) {
                count++;
            }
        }
        Assert.assertEquals(0, count);
    }

    private void assertOnlyOnePublicSendRequestWithRpcMessageIdMethod() {
        int count = 0;
        Method[] methods = NettyRpcClient.class.getMethods();
        for (Method method : methods) {
            if (method.getName().equals("sendRequestWithRpcMessageId")) {
                count++;
            }
        }
        Assert.assertEquals(1, count);
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

    public static class AmbiguousService {

        public EchoResponse echo(EchoRequest request) {
            EchoResponse response = new EchoResponse();
            response.setMessage("echo:" + request.getMessage());
            return response;
        }

        public EchoResponse echo(EchoResponse request) {
            EchoResponse response = new EchoResponse();
            response.setMessage("echo:" + request.getMessage());
            return response;
        }
    }

    public static class BoundaryService {

        public EchoResponse echo(EchoRequest request) {
            EchoResponse response = new EchoResponse();
            response.setMessage("echo:" + request.getMessage());
            return response;
        }

        public EchoResponse hidden(EchoRequest request) {
            EchoResponse response = new EchoResponse();
            response.setMessage("hidden:" + request.getMessage());
            return response;
        }
    }
}
