package com.xhj.etcd.sdk.client;

import com.xhj.etcd.rpc.NodeEndpoint;
import com.xhj.etcd.rpc.RpcClient;
import com.xhj.etcd.rpc.RpcMessageHandler;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * EtcdClientSdkBehaviorTest
 *
 * @author XJks
 * @description EtcdClient 基础生命周期测试。
 */
public class EtcdClientSdkBehaviorTest {

    @Test(expected = IllegalArgumentException.class)
    public void shouldRejectNullRpcClient() {
        new EtcdClient(null, defaultEndpoints());
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldRejectEmptyEndpoints() {
        new EtcdClient(new FakeRpcClient(), new ArrayList<NodeEndpoint>());
    }

    @Test
    public void shouldNotShutdownExternalRpcClientOnClose() {
        FakeRpcClient rpcClient = new FakeRpcClient();
        EtcdClient etcdClient = new EtcdClient(rpcClient, defaultEndpoints());

        etcdClient.close();

        assertFalse(rpcClient.shutdownCalled.get());
    }

    @Test
    public void shouldShutdownOwnedRpcClientOnClose() {
        FakeRpcClient rpcClient = new FakeRpcClient();
        EtcdClient etcdClient = new EtcdClient(rpcClient, defaultEndpoints(), true);

        etcdClient.close();

        assertTrue(rpcClient.shutdownCalled.get());
    }

    private static List<NodeEndpoint> defaultEndpoints() {
        List<NodeEndpoint> endpoints = new ArrayList<>();
        endpoints.add(new NodeEndpoint("n1", "127.0.0.1", 2379));
        return endpoints;
    }

    private static class FakeRpcClient implements RpcClient {

        private final AtomicBoolean shutdownCalled = new AtomicBoolean(false);

        @Override
        public <T> T call(NodeEndpoint endpoint, String serviceName, String methodName, Object request, Class<T> responseClass) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void send(NodeEndpoint endpoint, String serviceName, String methodName, Object request) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void sendRequestWithRpcMessageId(NodeEndpoint endpoint, String serviceName, String methodName, Object request, String rpcMessageId, RpcMessageHandler handler) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void removeRpcMessageHandler(String rpcMessageId) {
        }

        @Override
        public boolean heartbeat(NodeEndpoint endpoint) {
            return false;
        }

        @Override
        public void shutdown() {
            shutdownCalled.set(true);
        }
    }
}
