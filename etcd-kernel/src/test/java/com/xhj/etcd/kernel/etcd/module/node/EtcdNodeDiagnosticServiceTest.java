package com.xhj.etcd.kernel.etcd.module.node;

import com.xhj.etcd.kernel.etcd.etcdrpc.KvStateHashRequest;
import com.xhj.etcd.kernel.etcd.etcdrpc.KvStateHashResponse;
import com.xhj.etcd.kernel.etcd.etcdrpc.CompactRequest;
import com.xhj.etcd.kernel.etcd.etcdrpc.CompactResponse;
import com.xhj.etcd.kernel.etcd.etcdrpc.LeaseGrantRequest;
import com.xhj.etcd.kernel.etcd.etcdrpc.LeaseGrantResponse;
import com.xhj.etcd.kernel.etcd.etcdrpc.PutRequest;
import com.xhj.etcd.kernel.etcd.etcdrpc.PutResponse;
import com.xhj.etcd.kernel.etcd.etcdrpc.NodeStatusRequest;
import com.xhj.etcd.kernel.etcd.etcdrpc.NodeStatusResponse;
import com.xhj.etcd.kernel.etcd.etcdrpc.WatchSubscribeRequest;
import com.xhj.etcd.kernel.etcd.etcdrpc.WatchSubscribeResponse;
import com.xhj.etcd.kernel.etcd.etcdrpc.EtcdRpcResponse;
import com.xhj.etcd.kernel.etcd.node.EtcdNode;
import com.xhj.etcd.kernel.raft.core.RaftConfig;
import com.xhj.etcd.kernel.raft.core.RaftRoleType;
import com.xhj.etcd.storage.Storage;
import com.xhj.etcd.storage.memory.MemoryStorage;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.Callable;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * EtcdNodeDiagnosticServiceTest
 *
 * @author XJks
 * @description EtcdNode KvStateHash 和 Status 诊断能力测试。
 */
public class EtcdNodeDiagnosticServiceTest {

    private Storage storage;
    private EtcdNode node;

    @Before
    public void setUp() {
        storage = new MemoryStorage();

        RaftConfig raftConfig = new RaftConfig();
        raftConfig.setElectionTimeoutTicks(10);
        raftConfig.setHeartbeatTimeoutTicks(3);

        node = new EtcdNode("n1", raftConfig, storage);
        node.start();
    }

    @After
    public void tearDown() {
        if (node != null) {
            node.stop();
        }
    }

    @Test
    public void shouldReturnStableKvStateHashAndStatusForCurrentNodeState() throws Exception {
        awaitTrue(new Callable<Boolean>() {
            @Override
            public Boolean call() {
                return node.getRole() == RaftRoleType.LEADER;
            }
        }, 5000L, "leader is not elected");

        EtcdRpcResponse<PutResponse> putResponse = node.handleEtcdRpcPutRequest(new PutRequest("diag/hash/status", "v1"));
        assertNotNull(putResponse);
        assertNotNull(putResponse.getHeader());
        assertTrue(putResponse.getHeader().isSuccess());

        EtcdRpcResponse<KvStateHashResponse> kvStateHashResponse = node.handleEtcdRpcKvStateHashRequest(new KvStateHashRequest(0L));
        assertNotNull(kvStateHashResponse);
        assertNotNull(kvStateHashResponse.getHeader());
        assertTrue(kvStateHashResponse.getHeader().isSuccess());
        assertNotNull(kvStateHashResponse.getBody());
        assertEquals(1L, kvStateHashResponse.getBody().getRevision());
        assertEquals(1, kvStateHashResponse.getBody().getKeyCount());

        EtcdRpcResponse<NodeStatusResponse> nodeStatusResponse = node.handleEtcdRpcNodeStatusRequest(new NodeStatusRequest());
        assertNotNull(nodeStatusResponse);
        assertNotNull(nodeStatusResponse.getHeader());
        assertTrue(nodeStatusResponse.getHeader().isSuccess());
        assertNotNull(nodeStatusResponse.getBody());
        assertEquals("n1", nodeStatusResponse.getBody().getNodeId());
        assertEquals(RaftRoleType.LEADER, nodeStatusResponse.getBody().getRole());
        assertEquals(1L, nodeStatusResponse.getBody().getCurrentRevision());
        assertEquals(1, nodeStatusResponse.getBody().getKeyCount());
        assertEquals(0, nodeStatusResponse.getBody().getLeaseCount());
        assertEquals(0, nodeStatusResponse.getBody().getWatchCount());
    }

    @Test
    public void shouldRejectKvStateHashOnCompactedRevision() throws Exception {
        awaitTrue(new Callable<Boolean>() {
            @Override
            public Boolean call() {
                return node.getRole() == RaftRoleType.LEADER;
            }
        }, 5000L, "leader is not elected");

        EtcdRpcResponse<PutResponse> firstPutResponse = node.handleEtcdRpcPutRequest(new PutRequest("diag/hash/compact/k", "v1"));
        EtcdRpcResponse<PutResponse> secondPutResponse = node.handleEtcdRpcPutRequest(new PutRequest("diag/hash/compact/k", "v2"));
        assertTrue(firstPutResponse.getHeader().isSuccess());
        assertTrue(secondPutResponse.getHeader().isSuccess());

        CompactRequest compactRequest = new CompactRequest();
        compactRequest.setRevision(secondPutResponse.getBody().getRevision());
        EtcdRpcResponse<CompactResponse> compactResponse = node.handleEtcdRpcCompactRequest(compactRequest);
        assertNotNull(compactResponse);
        assertNotNull(compactResponse.getHeader());
        assertTrue(compactResponse.getHeader().isSuccess());
        assertNotNull(compactResponse.getBody());

        EtcdRpcResponse<KvStateHashResponse> compactedHashResponse = node.handleEtcdRpcKvStateHashRequest(new KvStateHashRequest(firstPutResponse.getBody().getRevision()));
        assertNotNull(compactedHashResponse);
        assertNotNull(compactedHashResponse.getHeader());
        assertFalse(compactedHashResponse.getHeader().isSuccess());
        assertTrue(compactedHashResponse.getHeader().getMessage().contains("compacted"));
        assertNull(compactedHashResponse.getBody());
    }

    @Test
    public void shouldReflectLeaseAndWatchCountInStatus() throws Exception {
        awaitTrue(new Callable<Boolean>() {
            @Override
            public Boolean call() {
                return node.getRole() == RaftRoleType.LEADER;
            }
        }, 5000L, "leader is not elected");

        EtcdRpcResponse<LeaseGrantResponse> leaseGrantResponse = node.handleEtcdRpcLeaseGrantRequest(new LeaseGrantRequest(0L, 30L));
        assertNotNull(leaseGrantResponse);
        assertNotNull(leaseGrantResponse.getHeader());
        assertTrue(leaseGrantResponse.getHeader().isSuccess());

        WatchSubscribeRequest watchSubscribeRequest = new WatchSubscribeRequest();
        watchSubscribeRequest.setWatchId(10001L);
        watchSubscribeRequest.setStartKey("diag/status/watch/");
        watchSubscribeRequest.setPrefixMatch(true);
        EtcdRpcResponse<WatchSubscribeResponse> watchSubscribeResponse = node.handleEtcdRpcWatchSubscribeRequest(
                watchSubscribeRequest,
                null,
                "diag-status-watch-route");
        assertNotNull(watchSubscribeResponse);
        assertNotNull(watchSubscribeResponse.getHeader());
        assertTrue(watchSubscribeResponse.getHeader().isSuccess());

        EtcdRpcResponse<NodeStatusResponse> nodeStatusResponse = node.handleEtcdRpcNodeStatusRequest(new NodeStatusRequest());
        assertNotNull(nodeStatusResponse);
        assertNotNull(nodeStatusResponse.getHeader());
        assertTrue(nodeStatusResponse.getHeader().isSuccess());
        assertNotNull(nodeStatusResponse.getBody());
        assertEquals(1, nodeStatusResponse.getBody().getLeaseCount());
        assertEquals(1, nodeStatusResponse.getBody().getWatchCount());
    }

    @Test
    public void shouldReflectCompactRevisionInStatusAfterCompactApplied() throws Exception {
        awaitTrue(new Callable<Boolean>() {
            @Override
            public Boolean call() {
                return node.getRole() == RaftRoleType.LEADER;
            }
        }, 5000L, "leader is not elected");

        EtcdRpcResponse<PutResponse> firstPutResponse = node.handleEtcdRpcPutRequest(new PutRequest("diag/status/compact", "v1"));
        EtcdRpcResponse<PutResponse> secondPutResponse = node.handleEtcdRpcPutRequest(new PutRequest("diag/status/compact", "v2"));
        assertTrue(firstPutResponse.getHeader().isSuccess());
        assertTrue(secondPutResponse.getHeader().isSuccess());

        long compactRevision = secondPutResponse.getBody().getRevision();
        CompactRequest compactRequest = new CompactRequest();
        compactRequest.setRevision(compactRevision);
        EtcdRpcResponse<CompactResponse> compactResponse = node.handleEtcdRpcCompactRequest(compactRequest);
        assertNotNull(compactResponse);
        assertNotNull(compactResponse.getHeader());
        assertTrue(compactResponse.getHeader().isSuccess());

        EtcdRpcResponse<NodeStatusResponse> nodeStatusResponse = node.handleEtcdRpcNodeStatusRequest(new NodeStatusRequest());
        assertNotNull(nodeStatusResponse);
        assertNotNull(nodeStatusResponse.getHeader());
        assertTrue(nodeStatusResponse.getHeader().isSuccess());
        assertNotNull(nodeStatusResponse.getBody());
        assertEquals(compactRevision, nodeStatusResponse.getBody().getCompactRevision());
        assertTrue(nodeStatusResponse.getBody().getCurrentRevision() >= compactRevision);
    }

    @Test
    public void shouldRejectKvStateHashWhenRevisionIsFutureRevision() throws Exception {
        awaitTrue(new Callable<Boolean>() {
            @Override
            public Boolean call() {
                return node.getRole() == RaftRoleType.LEADER;
            }
        }, 5000L, "leader is not elected");

        EtcdRpcResponse<PutResponse> putResponse = node.handleEtcdRpcPutRequest(new PutRequest("diag/hash/future", "v1"));
        assertNotNull(putResponse);
        assertNotNull(putResponse.getHeader());
        assertTrue(putResponse.getHeader().isSuccess());
        assertNotNull(putResponse.getBody());

        long futureRevision = putResponse.getBody().getRevision() + 1L;
        EtcdRpcResponse<KvStateHashResponse> kvStateHashResponse = node.handleEtcdRpcKvStateHashRequest(new KvStateHashRequest(futureRevision));
        assertNotNull(kvStateHashResponse);
        assertNotNull(kvStateHashResponse.getHeader());
        assertFalse(kvStateHashResponse.getHeader().isSuccess());
        assertNull(kvStateHashResponse.getBody());
    }

    @Test
    public void shouldRejectNullDiagnosticRequests() throws Exception {
        awaitTrue(new Callable<Boolean>() {
            @Override
            public Boolean call() {
                return node.getRole() == RaftRoleType.LEADER;
            }
        }, 5000L, "leader is not elected");

        EtcdRpcResponse<KvStateHashResponse> nullKvStateHashResponse = node.handleEtcdRpcKvStateHashRequest(null);
        assertNotNull(nullKvStateHashResponse);
        assertNotNull(nullKvStateHashResponse.getHeader());
        assertFalse(nullKvStateHashResponse.getHeader().isSuccess());
        assertNull(nullKvStateHashResponse.getBody());

        EtcdRpcResponse<NodeStatusResponse> nullNodeStatusResponse = node.handleEtcdRpcNodeStatusRequest(null);
        assertNotNull(nullNodeStatusResponse);
        assertNotNull(nullNodeStatusResponse.getHeader());
        assertFalse(nullNodeStatusResponse.getHeader().isSuccess());
        assertNull(nullNodeStatusResponse.getBody());
    }

    private void awaitTrue(Callable<Boolean> predicate, long timeoutMillis, String message) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (System.currentTimeMillis() < deadline) {
            if (Boolean.TRUE.equals(predicate.call())) {
                return;
            }
            Thread.sleep(20L);
        }
        throw new AssertionError(message);
    }
}
