package com.xhj.etcd.sdk.client;

import com.xhj.etcd.kernel.etcd.etcdrpc.EtcdResponseHeader;
import com.xhj.etcd.kernel.etcd.etcdrpc.EtcdRpcResponse;
import com.xhj.etcd.kernel.etcd.etcdrpc.LeaseKeepAliveRequest;
import com.xhj.etcd.kernel.etcd.etcdrpc.LeaseKeepAliveResponse;
import com.xhj.etcd.kernel.etcd.etcdrpc.LeaseView;
import com.xhj.etcd.kernel.etcd.node.EtcdNode;
import com.xhj.etcd.sdk.client.lease.LeaseHandle;
import com.xhj.etcd.rpc.NodeEndpoint;
import com.xhj.etcd.rpc.RpcClient;
import com.xhj.etcd.rpc.RpcMessageHandler;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertSame;
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
    public void shouldShutdownRpcClientOnClose() {
        FakeRpcClient rpcClient = new FakeRpcClient();
        EtcdClient etcdClient = new EtcdClient(rpcClient, defaultEndpoints());

        etcdClient.close();

        assertTrue(rpcClient.shutdownCalled.get());
    }

    @Test
    public void shouldKeepNewLeaseHandleRegisteredWhenReplacingSameLeaseId() throws Exception {
        FakeRpcClient rpcClient = new FakeRpcClient();
        EtcdClient etcdClient = new EtcdClient(rpcClient, defaultEndpoints());

        try {
            LeaseHandle firstLeaseHandle = etcdClient.startLeaseKeepAlive(new LeaseKeepAliveRequest(1001L));
            LeaseHandle secondLeaseHandle = etcdClient.startLeaseKeepAlive(new LeaseKeepAliveRequest(1001L));

            assertTrue(firstLeaseHandle.isClosed());
            Map<Long, ?> leaseHandleByLeaseId = leaseHandleByLeaseId(etcdClient);
            assertSame(secondLeaseHandle, leaseHandleByLeaseId.get(1001L));
        } finally {
            etcdClient.close();
        }
    }

    @SuppressWarnings("unchecked")
    private static Map<Long, ?> leaseHandleByLeaseId(EtcdClient etcdClient) throws Exception {
        java.lang.reflect.Field leaseHandleByLeaseIdField = EtcdClient.class.getDeclaredField("leaseHandleByLeaseId");
        leaseHandleByLeaseIdField.setAccessible(true);
        return (Map<Long, ?>) leaseHandleByLeaseIdField.get(etcdClient);
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
            if (EtcdNode.HANDLE_ETCD_RPC_LEASE_KEEP_ALIVE_REQUEST_METHOD_NAME.equals(methodName)) {
                LeaseKeepAliveRequest leaseKeepAliveRequest = (LeaseKeepAliveRequest) request;
                LeaseView leaseView = new LeaseView();
                leaseView.setLeaseId(leaseKeepAliveRequest.getLeaseId());
                leaseView.setTtlSeconds(30L);
                leaseView.setRemainingSeconds(30L);

                LeaseKeepAliveResponse leaseKeepAliveResponse = LeaseKeepAliveResponse.of(leaseView);
                EtcdRpcResponse<LeaseKeepAliveResponse> rpcResponse = EtcdRpcResponse.of(
                        EtcdResponseHeader.success(),
                        leaseKeepAliveResponse);
                return responseClass.cast(rpcResponse);
            }
            throw new UnsupportedOperationException("unexpected methodName=" + methodName);
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
