package com.xhj.etcd.kernel.etcd.network.client;

import com.xhj.etcd.kernel.etcd.etcdrpc.CompactRequest;
import com.xhj.etcd.kernel.etcd.etcdrpc.DeleteRangeRequest;
import com.xhj.etcd.kernel.etcd.etcdrpc.DeleteRequest;
import com.xhj.etcd.kernel.etcd.etcdrpc.GetRequest;
import com.xhj.etcd.kernel.etcd.etcdrpc.LeaseGrantRequest;
import com.xhj.etcd.kernel.etcd.etcdrpc.LeaseKeepAliveRequest;
import com.xhj.etcd.kernel.etcd.etcdrpc.LeaseTtlRequest;
import com.xhj.etcd.kernel.etcd.etcdrpc.PutRequest;
import com.xhj.etcd.kernel.etcd.etcdrpc.RangeRequest;
import com.xhj.etcd.kernel.etcd.etcdrpc.TxnCompareCondition;
import com.xhj.etcd.kernel.etcd.etcdrpc.TxnCompareOperatorType;
import com.xhj.etcd.kernel.etcd.etcdrpc.TxnRequest;
import com.xhj.etcd.kernel.etcd.etcdrpc.TxnResponse;
import com.xhj.etcd.kernel.etcd.client.EtcdClient;
import com.xhj.etcd.kernel.etcd.network.support.EtcdDistributedTestSkeleton;
import com.xhj.etcd.rpc.NodeEndpoint;
import com.xhj.etcd.rpc.RpcClient;
import com.xhj.etcd.rpc.RpcMessageHandler;
import com.xhj.etcd.rpc.RpcStream;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * EtcdClientFailurePathTest
 *
 * @author XJks
 * @description EtcdClient 失败路径测试，覆盖构造参数校验、RPC 异常传播和业务失败返回。
 */
public class EtcdClientFailurePathTest extends EtcdDistributedTestSkeleton {

    @Test(expected = IllegalArgumentException.class)
    public void shouldRejectNullRpcClient() {
        new EtcdClient(null, new NodeEndpoint("n1", "127.0.0.1", 8080));
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldRejectEmptyEndpointList() {
        new EtcdClient(buildNoopRpcClient(), new ArrayList<NodeEndpoint>());
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldRejectNullEndpointList() {
        new EtcdClient(buildNoopRpcClient(), (List<NodeEndpoint>) null);
    }

    @Test(expected = IllegalStateException.class)
    public void shouldThrowWhenRpcClientReturnsUnexpectedResponseType() {
        RpcClient fakeRpcClient = new RpcClient() {
            @Override
            public <T> T call(NodeEndpoint endpoint, String serviceName, String methodName, Object request, Class<T> responseClass) {
                @SuppressWarnings("unchecked")
                T response = (T) (Object) "unexpected-response";
                return response;
            }

            @Override
            public void send(NodeEndpoint endpoint, String serviceName, String methodName, Object request) {
            }

            @Override
            public RpcStream openStream(String streamId, NodeEndpoint endpoint, String serviceName, String methodName, Object request, RpcMessageHandler handler) {
                return null;
            }

            @Override
            public boolean heartbeat(NodeEndpoint endpoint) {
                return false;
            }

            @Override
            public void shutdown() {
            }
        };

        EtcdClient client = new EtcdClient(fakeRpcClient, new NodeEndpoint("n1", "127.0.0.1", 8080));
        client.put(new PutRequest("client/failure/key", "value"));
    }

    @Test
    public void shouldReturnNullForInvalidRequestsAndKeepLeaderRoutingStable() throws Exception {
        String leaderId = startClusterAndAwaitLeader(3, DEFAULT_BOUNDARY_TIMEOUT_MILLIS);
        String followerId = chooseFollowerId(leaderId);

        EtcdClient client = new EtcdClient(harness.getTestClient(), buildClientEndpoints(followerId));
        NodeEndpoint leaderEndpoint = requireEndpoint(leaderId);

        assertNull(client.put(new PutRequest("", "value")));
        assertEquals(leaderEndpoint.endpointKey(), client.getCurrentEndpoint().endpointKey());

        assertNull(client.get(new GetRequest("", false)));
        assertNull(client.range(buildInvalidRangeRequest(false)));
        assertNull(client.delete(new DeleteRequest("")));
        assertNull(client.deleteRange(buildInvalidDeleteRangeRequest()));
        assertNull(client.compact(new CompactRequest()));
        assertNull(client.leaseGrant(new LeaseGrantRequest(0L, 0L)));
        assertNull(client.leaseKeepAlive(new LeaseKeepAliveRequest(999999L)));
        assertNull(client.leaseTtl(new LeaseTtlRequest(999999L)));

        TxnRequest failureTxnRequest = new TxnRequest();
        failureTxnRequest.getCompareConditions().add(TxnCompareCondition.version(
                "client/failure/txn",
                TxnCompareOperatorType.EQUAL,
                1L));
        TxnResponse failureTxnResponse = client.txn(failureTxnRequest);
        assertNotNull(failureTxnResponse);
        assertFalse(failureTxnResponse.isSucceeded());
    }

    @Test
    public void shouldPreserveCurrentEndpointWhenLeaderRoutedRequestsFailBusinessValidation() throws Exception {
        String leaderId = startClusterAndAwaitLeader(3, DEFAULT_BOUNDARY_TIMEOUT_MILLIS);
        String followerId = chooseFollowerId(leaderId);

        EtcdClient client = new EtcdClient(harness.getTestClient(), buildClientEndpoints(followerId));
        NodeEndpoint leaderEndpoint = requireEndpoint(leaderId);

        CompactRequest compactRequest = new CompactRequest();
        compactRequest.setRevision(0L);
        assertNull(client.compact(compactRequest));
        assertEquals(leaderEndpoint.endpointKey(), client.getCurrentEndpoint().endpointKey());

        assertNull(client.leaseGrant(new LeaseGrantRequest(0L, 0L)));
        assertEquals(leaderEndpoint.endpointKey(), client.getCurrentEndpoint().endpointKey());

        assertNull(client.leaseKeepAlive(new LeaseKeepAliveRequest(0L)));
        assertEquals(leaderEndpoint.endpointKey(), client.getCurrentEndpoint().endpointKey());
    }

    private RpcClient buildNoopRpcClient() {
        return new RpcClient() {
            @Override
            public <T> T call(NodeEndpoint endpoint, String serviceName, String methodName, Object request, Class<T> responseClass) {
                return null;
            }

            @Override
            public void send(NodeEndpoint endpoint, String serviceName, String methodName, Object request) {
            }

            @Override
            public RpcStream openStream(String streamId, NodeEndpoint endpoint, String serviceName, String methodName, Object request, RpcMessageHandler handler) {
                return null;
            }

            @Override
            public boolean heartbeat(NodeEndpoint endpoint) {
                return false;
            }

            @Override
            public void shutdown() {
            }
        };
    }

    private List<NodeEndpoint> buildClientEndpoints(String firstNodeId) {
        List<NodeEndpoint> endpoints = new ArrayList<>();
        endpoints.add(requireEndpoint(firstNodeId));
        for (String nodeId : harness.getNodeIds()) {
            if (!firstNodeId.equals(nodeId)) {
                endpoints.add(requireEndpoint(nodeId));
            }
        }
        return endpoints;
    }

    private RangeRequest buildInvalidRangeRequest(boolean linearizableRead) {
        RangeRequest rangeRequest = new RangeRequest();
        rangeRequest.setStartKey("");
        rangeRequest.setLinearizableRead(linearizableRead);
        return rangeRequest;
    }

    private DeleteRangeRequest buildInvalidDeleteRangeRequest() {
        DeleteRangeRequest deleteRangeRequest = new DeleteRangeRequest();
        deleteRangeRequest.setStartKey("");
        return deleteRangeRequest;
    }
}
