package com.xhj.etcd.kernel.etcd.module.node;

import com.xhj.etcd.kernel.etcd.etcdrpc.GetRequest;
import com.xhj.etcd.kernel.etcd.etcdrpc.GetResponse;
import com.xhj.etcd.kernel.etcd.etcdrpc.LeaseGrantRequest;
import com.xhj.etcd.kernel.etcd.etcdrpc.LeaseGrantResponse;
import com.xhj.etcd.kernel.etcd.etcdrpc.LeaseKeepAliveRequest;
import com.xhj.etcd.kernel.etcd.etcdrpc.LeaseKeepAliveResponse;
import com.xhj.etcd.kernel.etcd.etcdrpc.LeaseListRequest;
import com.xhj.etcd.kernel.etcd.etcdrpc.LeaseListResponse;
import com.xhj.etcd.kernel.etcd.etcdrpc.LeaseRevokeRequest;
import com.xhj.etcd.kernel.etcd.etcdrpc.LeaseRevokeResponse;
import com.xhj.etcd.kernel.etcd.etcdrpc.LeaseTtlRequest;
import com.xhj.etcd.kernel.etcd.etcdrpc.LeaseTtlResponse;
import com.xhj.etcd.kernel.etcd.etcdrpc.PutRequest;
import com.xhj.etcd.kernel.etcd.etcdrpc.PutResponse;
import com.xhj.etcd.kernel.etcd.etcdrpc.RangeRequest;
import com.xhj.etcd.kernel.etcd.etcdrpc.RangeResponse;
import com.xhj.etcd.kernel.etcd.node.EtcdNode;
import com.xhj.etcd.kernel.etcd.network.support.EtcdTestSupport;
import com.xhj.etcd.kernel.raft.core.RaftConfig;
import com.xhj.etcd.kernel.raft.core.RaftRoleType;
import com.xhj.etcd.serializer.SerializerRegistry;
import com.xhj.etcd.storage.Storage;
import com.xhj.etcd.storage.memory.MemoryStorage;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * EtcdNodeLeaseServiceTest
 *
 * @author XJks
 * @description EtcdNode Lease 模块测试，验证 lease grant / keepalive / revoke / expire 与 KV 绑定关系。
 */
public class EtcdNodeLeaseServiceTest {

    private Storage storage;
    private RaftConfig raftConfig;
    private EtcdNode node;

    @Before
    public void setUp() {
        storage = new MemoryStorage();

        raftConfig = new RaftConfig();
        raftConfig.setElectionTimeoutTicks(10);
        raftConfig.setHeartbeatTimeoutTicks(3);
        raftConfig.setSnapshotTriggerLogCount(2);

        node = new EtcdNode("node1", raftConfig, storage, SerializerRegistry.getDefaultSerializer());
        node.start();
    }

    @After
    public void tearDown() {
        if (node != null) {
            node.stop();
        }
    }

    @Test
    public void shouldGrantLeaseBindKeyAndExposeLeaseMetadataOnReads() throws Exception {
        awaitLeader(node, 3000L);

        LeaseGrantRequest leaseGrantRequest = new LeaseGrantRequest(0L, 3L);
        LeaseGrantResponse leaseGrantResponse = requireSuccess(node.handleEtcdRpcLeaseGrantRequest(leaseGrantRequest)).getBody();
        assertNotNull(leaseGrantResponse);
        assertNotNull(leaseGrantResponse.getLease());
        long leaseId = leaseGrantResponse.getLease().getLeaseId();
        assertTrue(leaseId > 0L);

        PutRequest putRequest = new PutRequest("lease/node/key", "lease-value", leaseId);
        PutResponse putResponse = requireSuccess(node.handleEtcdRpcPutRequest(putRequest)).getBody();
        assertNotNull(putResponse);

        GetRequest getRequest = new GetRequest();
        getRequest.setKey("lease/node/key");
        getRequest.setLinearizableRead(false);
        GetResponse getResponse = requireSuccess(node.handleEtcdRpcGetRequest(getRequest)).getBody();
        assertNotNull(getResponse);
        assertEquals("lease-value", getResponse.getValue());
        assertEquals(leaseId, getResponse.getLeaseId());

        RangeRequest rangeRequest = new RangeRequest();
        rangeRequest.setStartKey("lease/node/");
        rangeRequest.setPrefixMatch(true);
        rangeRequest.setLinearizableRead(false);
        RangeResponse rangeResponse = requireSuccess(node.handleEtcdRpcRangeRequest(rangeRequest)).getBody();
        assertNotNull(rangeResponse);
        assertEquals(1, rangeResponse.getCount());
        assertEquals(leaseId, rangeResponse.getItems().get(0).getLeaseId());

        LeaseTtlResponse ttlResponse = requireSuccess(node.handleEtcdRpcLeaseTtlRequest(new LeaseTtlRequest(leaseId))).getBody();
        assertNotNull(ttlResponse);
        assertNotNull(ttlResponse.getLease());
        assertEquals(leaseId, ttlResponse.getLease().getLeaseId());
        assertTrue(ttlResponse.getLease().getRemainingSeconds() > 0L);

        LeaseKeepAliveResponse keepAliveResponse = requireSuccess(node.handleEtcdRpcLeaseKeepAliveRequest(new LeaseKeepAliveRequest(leaseId))).getBody();
        assertNotNull(keepAliveResponse);
        assertNotNull(keepAliveResponse.getLease());
        assertEquals(leaseId, keepAliveResponse.getLease().getLeaseId());

        LeaseListResponse listResponse = requireSuccess(node.handleEtcdRpcLeaseListRequest(new LeaseListRequest())).getBody();
        assertNotNull(listResponse);
        assertEquals(1, listResponse.getLeases().size());
        assertEquals("lease/node/key", listResponse.getLeases().get(0).getKeys().get(0));
    }

    @Test
    public void shouldRevokeLeaseAndDeleteBoundKey() throws Exception {
        awaitLeader(node, 3000L);

        LeaseGrantResponse leaseGrantResponse = requireSuccess(node.handleEtcdRpcLeaseGrantRequest(new LeaseGrantRequest(0L, 5L))).getBody();
        long leaseId = leaseGrantResponse.getLease().getLeaseId();

        assertTrue(requireSuccess(node.handleEtcdRpcPutRequest(new PutRequest("lease/revoke/key", "revoked-value", leaseId))).getHeader().isSuccess());

        LeaseRevokeResponse revokeResponse = requireSuccess(node.handleEtcdRpcLeaseRevokeRequest(new LeaseRevokeRequest(leaseId))).getBody();
        assertNotNull(revokeResponse);
        assertEquals(leaseId, revokeResponse.getLeaseId());
        assertEquals(1, revokeResponse.getDeletedCount());

        GetRequest getRequest = new GetRequest();
        getRequest.setKey("lease/revoke/key");
        getRequest.setLinearizableRead(false);
        GetResponse getResponse = requireSuccess(node.handleEtcdRpcGetRequest(getRequest)).getBody();
        assertNotNull(getResponse);
        assertNull(getResponse.getValue());

        assertFalse(node.handleEtcdRpcLeaseTtlRequest(new LeaseTtlRequest(leaseId)).getHeader().isSuccess());
    }

    @Test
    public void shouldExpireLeaseAndDeleteBoundKeyAutomatically() throws Exception {
        awaitLeader(node, 3000L);

        LeaseGrantResponse leaseGrantResponse = requireSuccess(node.handleEtcdRpcLeaseGrantRequest(new LeaseGrantRequest(0L, 1L))).getBody();
        long leaseId = leaseGrantResponse.getLease().getLeaseId();
        assertTrue(requireSuccess(node.handleEtcdRpcPutRequest(new PutRequest("lease/expire/key", "expire-value", leaseId))).getHeader().isSuccess());

        EtcdTestSupport.awaitTrue(() -> {
            GetRequest getRequest = new GetRequest();
            getRequest.setKey("lease/expire/key");
            getRequest.setLinearizableRead(false);
            GetResponse getResponse = node.handleEtcdRpcGetRequest(getRequest).getBody();
            return getResponse != null && getResponse.getValue() == null;
        }, 5000L, "lease expired key should be deleted automatically");

        assertFalse(node.handleEtcdRpcLeaseTtlRequest(new LeaseTtlRequest(leaseId)).getHeader().isSuccess());
    }

    @Test
    public void shouldRejectInvalidLeaseRequestsOnNodeRpcBoundary() throws Exception {
        awaitLeader(node, 3000L);

        assertFalse(node.handleEtcdRpcLeaseGrantRequest(new LeaseGrantRequest(0L, 0L)).getHeader().isSuccess());
        assertFalse(node.handleEtcdRpcLeaseKeepAliveRequest(new LeaseKeepAliveRequest(9999L)).getHeader().isSuccess());
        assertTrue(node.handleEtcdRpcLeaseRevokeRequest(new LeaseRevokeRequest(9999L)).getHeader().isSuccess());
        assertFalse(node.handleEtcdRpcLeaseTtlRequest(new LeaseTtlRequest(9999L)).getHeader().isSuccess());
    }

    private <T> com.xhj.etcd.kernel.etcd.etcdrpc.EtcdRpcResponse<T> requireSuccess(com.xhj.etcd.kernel.etcd.etcdrpc.EtcdRpcResponse<T> response) {
        assertNotNull(response);
        assertNotNull(response.getHeader());
        assertTrue(response.getHeader().isSuccess());
        return response;
    }

    private void awaitLeader(EtcdNode targetNode, long timeoutMillis) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (System.currentTimeMillis() < deadline) {
            if (targetNode.getRole() == RaftRoleType.LEADER) {
                return;
            }
            Thread.sleep(50L);
        }
        assertEquals(RaftRoleType.LEADER, targetNode.getRole());
    }
}
