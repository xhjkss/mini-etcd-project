package com.xhj.etcd.rpc.netty;

import com.xhj.etcd.rpc.NodeEndpoint;
import com.xhj.etcd.rpc.RpcException;
import com.xhj.etcd.rpc.fixture.EchoRequest;
import com.xhj.etcd.rpc.fixture.EchoResponse;
import com.xhj.etcd.rpc.fixture.EchoService;
import org.junit.Assert;
import org.junit.Test;

import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class NettyRpcEdgeCaseTest {

    @Test
    public void shouldReturnErrorWhenServiceMissing() throws Exception {
        NodeEndpoint endpoint = new NodeEndpoint("node-1", "127.0.0.1", findFreePort());
        NettyRpcServer server = new NettyRpcServer(endpoint);
        NettyRpcClient client = new NettyRpcClient();
        server.start();
        try {
            try {
                client.call(endpoint, "missing", "echo", buildEchoRequest("hello"), EchoResponse.class);
                Assert.fail("missing service should fail");
            } catch (RpcException expected) {
                Assert.assertTrue(expected.getMessage() != null);
            }
        } finally {
            client.shutdown();
            server.stop();
        }
    }

    @Test
    public void shouldSupportConcurrentCallsToSameEndpoint() throws Exception {
        NodeEndpoint endpoint = new NodeEndpoint("node-1", "127.0.0.1", findFreePort());
        NettyRpcServer server = new NettyRpcServer(endpoint);
        NettyRpcClient client = new NettyRpcClient();
        server.registerService("EchoService", new EchoService(), "echo");
        server.start();
        ExecutorService executor = Executors.newFixedThreadPool(4);
        try {
            List<Future<EchoResponse>> futures = new ArrayList<Future<EchoResponse>>();
            for (int i = 0; i < 12; i++) {
                final int index = i;
                futures.add(executor.submit(new Callable<EchoResponse>() {
                    @Override
                    public EchoResponse call() {
                        return client.call(endpoint, "EchoService", "echo", buildEchoRequest("value-" + index), EchoResponse.class);
                    }
                }));
            }
            for (int i = 0; i < futures.size(); i++) {
                Assert.assertEquals("echo:value-" + i, futures.get(i).get(10L, TimeUnit.SECONDS).getMessage());
            }
        } finally {
            executor.shutdownNow();
            client.shutdown();
            server.stop();
        }
    }

    private EchoRequest buildEchoRequest(String message) {
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
}
