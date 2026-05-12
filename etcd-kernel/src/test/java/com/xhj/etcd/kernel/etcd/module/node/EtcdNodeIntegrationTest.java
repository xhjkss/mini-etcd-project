package com.xhj.etcd.kernel.etcd.module.node;

import com.xhj.etcd.kernel.etcd.node.EtcdNode;

import com.xhj.etcd.kernel.raft.core.RaftConfig;
import com.xhj.etcd.kernel.raft.core.RaftRoleType;
import com.xhj.etcd.kernel.raft.storage.RaftPersistentState;
import com.xhj.etcd.kernel.etcd.etcdrpc.DeleteRequest;
import com.xhj.etcd.kernel.etcd.etcdrpc.DeleteRangeRequest;
import com.xhj.etcd.kernel.etcd.etcdrpc.DeleteRangeResponse;
import com.xhj.etcd.kernel.etcd.etcdrpc.DeleteResponse;
import com.xhj.etcd.kernel.etcd.etcdrpc.EtcdRpcResponse;
import com.xhj.etcd.kernel.etcd.etcdrpc.GetRequest;
import com.xhj.etcd.kernel.etcd.etcdrpc.GetResponse;
import com.xhj.etcd.kernel.etcd.etcdrpc.PutRequest;
import com.xhj.etcd.kernel.etcd.etcdrpc.PutResponse;
import com.xhj.etcd.kernel.etcd.etcdrpc.RangeRequest;
import com.xhj.etcd.kernel.etcd.etcdrpc.RangeResponse;
import com.xhj.etcd.serializer.SerializerRegistry;
import com.xhj.etcd.storage.Storage;
import com.xhj.etcd.storage.memory.MemoryStorage;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * EtcdNodeIntegrationTest
 *
 * @author XJks
 * @description EtcdNode 集成测试，验证 RPC -> Raft -> apply 闭环。
 */
public class EtcdNodeIntegrationTest {

    private Storage storage;
    private RaftConfig raftConfig;
    private EtcdNode etcdNode;

    @Before
    public void setUp() {
        storage = new MemoryStorage();

        raftConfig = new RaftConfig();
        raftConfig.setElectionTimeoutTicks(10);
        raftConfig.setHeartbeatTimeoutTicks(3);
        raftConfig.setSnapshotTriggerLogCount(2);

        etcdNode = new EtcdNode("node1", raftConfig, storage);
    }

    @After
    public void tearDown() {
        if (etcdNode != null) {
            etcdNode.stop();
        }
    }

    @Test
    public void testSingleNodeBecomesLeader() throws Exception {
        etcdNode.start();
        waitForRole(etcdNode, RaftRoleType.LEADER, 3000L);
        assertEquals(RaftRoleType.LEADER, etcdNode.getRole());
    }

    @Test
    public void testPutAndGetViaRaft() throws Exception {
        etcdNode.start();
        waitForRole(etcdNode, RaftRoleType.LEADER, 3000L);

        EtcdRpcResponse<PutResponse> putResponse = etcdNode.handleEtcdRpcPutRequest(new PutRequest("key1", "value1"));
        assertTrue(putResponse.getHeader().isSuccess());

        EtcdRpcResponse<GetResponse> getResponse = etcdNode.handleEtcdRpcGetRequest(new GetRequest("key1"));
        assertTrue(getResponse.getHeader().isSuccess());
        assertNotNull(getResponse.getBody());
        assertEquals("value1", getResponse.getBody().getValue());
    }

    @Test
    public void testDeleteViaRaft() throws Exception {
        etcdNode.start();
        waitForRole(etcdNode, RaftRoleType.LEADER, 3000L);

        EtcdRpcResponse<PutResponse> putResponse = etcdNode.handleEtcdRpcPutRequest(new PutRequest("key2", "value2"));
        assertTrue(putResponse.getHeader().isSuccess());

        EtcdRpcResponse<DeleteResponse> deleteResponse = etcdNode.handleEtcdRpcDeleteRequest(new DeleteRequest("key2"));
        assertTrue(deleteResponse.getHeader().isSuccess());
        assertNotNull(deleteResponse.getBody());
        assertEquals(1, deleteResponse.getBody().getDeletedCount());

        EtcdRpcResponse<GetResponse> getResponse = etcdNode.handleEtcdRpcGetRequest(new GetRequest("key2"));
        assertTrue(getResponse.getHeader().isSuccess());
        assertNull(getResponse.getBody().getValue());
    }

    @Test
    public void testRangeViaRaft() throws Exception {
        etcdNode.start();
        waitForRole(etcdNode, RaftRoleType.LEADER, 3000L);

        assertTrue(etcdNode.handleEtcdRpcPutRequest(new PutRequest("a", "1")).getHeader().isSuccess());
        assertTrue(etcdNode.handleEtcdRpcPutRequest(new PutRequest("b", "2")).getHeader().isSuccess());

        RangeRequest rangeRequest = new RangeRequest();
        rangeRequest.setStartKey("a");
        rangeRequest.setEndKeyExclusive("z");
        EtcdRpcResponse<RangeResponse> rangeResponse = etcdNode.handleEtcdRpcRangeRequest(rangeRequest);
        assertTrue(rangeResponse.getHeader().isSuccess());
        assertNotNull(rangeResponse.getBody());
        assertEquals(2, rangeResponse.getBody().getCount());
    }

    @Test
    public void testDeleteRangeViaRaft() throws Exception {
        etcdNode.start();
        waitForRole(etcdNode, RaftRoleType.LEADER, 3000L);

        assertTrue(etcdNode.handleEtcdRpcPutRequest(new PutRequest("dr-1", "v1")).getHeader().isSuccess());
        assertTrue(etcdNode.handleEtcdRpcPutRequest(new PutRequest("dr-2", "v2")).getHeader().isSuccess());
        assertTrue(etcdNode.handleEtcdRpcPutRequest(new PutRequest("keep-1", "v3")).getHeader().isSuccess());

        DeleteRangeRequest deleteRangeRequest = new DeleteRangeRequest();
        deleteRangeRequest.setStartKey("dr-");
        deleteRangeRequest.setPrefixMatch(true);
        deleteRangeRequest.setPrevKv(true);

        EtcdRpcResponse<DeleteRangeResponse> deleteRangeResponse = etcdNode.handleEtcdRpcDeleteRangeRequest(deleteRangeRequest);
        assertTrue(deleteRangeResponse.getHeader().isSuccess());
        assertNotNull(deleteRangeResponse.getBody());
        assertEquals(2, deleteRangeResponse.getBody().getDeletedCount());
        assertEquals(2, deleteRangeResponse.getBody().getPrevItems().size());

        RangeRequest rangeRequest = new RangeRequest();
        rangeRequest.setStartKey("dr-");
        rangeRequest.setPrefixMatch(true);
        rangeRequest.setCountOnly(true);
        EtcdRpcResponse<RangeResponse> rangeResponse = etcdNode.handleEtcdRpcRangeRequest(rangeRequest);
        assertTrue(rangeResponse.getHeader().isSuccess());
        assertNotNull(rangeResponse.getBody());
        assertEquals(0, rangeResponse.getBody().getCount());
    }

    @Test
    public void testGetHistoricalRevision() throws Exception {
        etcdNode.start();
        waitForRole(etcdNode, RaftRoleType.LEADER, 3000L);

        EtcdRpcResponse<PutResponse> firstPut = etcdNode.handleEtcdRpcPutRequest(new PutRequest("mvcc-key", "v1"));
        assertTrue(firstPut.getHeader().isSuccess());
        long firstRevision = firstPut.getBody().getRevision();

        EtcdRpcResponse<PutResponse> secondPut = etcdNode.handleEtcdRpcPutRequest(new PutRequest("mvcc-key", "v2"));
        assertTrue(secondPut.getHeader().isSuccess());
        assertTrue(secondPut.getBody().getRevision() > firstRevision);

        GetRequest historyRequest = new GetRequest();
        historyRequest.setKey("mvcc-key");
        historyRequest.setRevision(firstRevision);
        historyRequest.setLinearizableRead(false);

        EtcdRpcResponse<GetResponse> historyResponse = etcdNode.handleEtcdRpcGetRequest(historyRequest);
        assertTrue(historyResponse.getHeader().isSuccess());
        assertNotNull(historyResponse.getBody());
        assertEquals("v1", historyResponse.getBody().getValue());
    }

    @Test
    public void testHardStateAndLogPersistence() throws Exception {
        etcdNode.start();
        waitForRole(etcdNode, RaftRoleType.LEADER, 3000L);

        EtcdRpcResponse<PutResponse> putResponse = etcdNode.handleEtcdRpcPutRequest(new PutRequest("persistKey", "persistValue"));
        assertTrue(putResponse.getHeader().isSuccess());

        byte[] persistentStateData = storage.get("raft", "persistent-state");
        assertNotNull(persistentStateData);
    }

    @Test
    public void testSnapshotRequestedByCommittedLogThreshold() throws Exception {
        etcdNode.start();
        waitForRole(etcdNode, RaftRoleType.LEADER, 3000L);

        assertTrue(etcdNode.handleEtcdRpcPutRequest(new PutRequest("s-key-1", "s-value-1")).getHeader().isSuccess());
        assertTrue(etcdNode.handleEtcdRpcPutRequest(new PutRequest("s-key-2", "s-value-2")).getHeader().isSuccess());

        RaftPersistentState snapshotState = waitForPersistentStateWithSnapshot(5000L);
        assertNotNull(snapshotState);
        assertNotNull(snapshotState.getSnapshot());
    }

    @Test
    public void testRestartRestoreStateMachineFromSnapshot() throws Exception {
        etcdNode.start();
        waitForRole(etcdNode, RaftRoleType.LEADER, 3000L);

        assertTrue(etcdNode.handleEtcdRpcPutRequest(new PutRequest("restart-key-1", "restart-value-1")).getHeader().isSuccess());
        assertTrue(etcdNode.handleEtcdRpcPutRequest(new PutRequest("restart-key-2", "restart-value-2")).getHeader().isSuccess());

        RaftPersistentState snapshotState = waitForPersistentStateWithSnapshot(5000L);
        assertNotNull(snapshotState);
        assertNotNull(snapshotState.getSnapshot());

        etcdNode.stop();

        EtcdNode restartedNode = new EtcdNode("node1", raftConfig, storage);
        restartedNode.start();
        try {
            waitForRole(restartedNode, RaftRoleType.LEADER, 3000L);

            EtcdRpcResponse<GetResponse> key1Response = restartedNode.handleEtcdRpcGetRequest(new GetRequest("restart-key-1"));
            assertTrue(key1Response.getHeader().isSuccess());
            assertEquals("restart-value-1", key1Response.getBody().getValue());

            EtcdRpcResponse<GetResponse> key2Response = restartedNode.handleEtcdRpcGetRequest(new GetRequest("restart-key-2"));
            assertTrue(key2Response.getHeader().isSuccess());
            assertEquals("restart-value-2", key2Response.getBody().getValue());

            assertEquals("restart-value-1", restartedNode.getLocalStateMachineValue("kv", "restart-key-1"));
            assertEquals("restart-value-2", restartedNode.getLocalStateMachineValue("kv", "restart-key-2"));
        } finally {
            restartedNode.stop();
            etcdNode = restartedNode;
        }
    }

    @Test
    public void testRestartKeepsHistoricalRevisionReadableFromSnapshot() throws Exception {
        etcdNode.start();
        waitForRole(etcdNode, RaftRoleType.LEADER, 3000L);

        EtcdRpcResponse<PutResponse> firstPut = etcdNode.handleEtcdRpcPutRequest(new PutRequest("restart-mvcc-key", "v1"));
        assertTrue(firstPut.getHeader().isSuccess());
        long firstRevision = firstPut.getBody().getRevision();

        EtcdRpcResponse<PutResponse> secondPut = etcdNode.handleEtcdRpcPutRequest(new PutRequest("restart-mvcc-key", "v2"));
        assertTrue(secondPut.getHeader().isSuccess());
        assertTrue(secondPut.getBody().getRevision() > firstRevision);

        RaftPersistentState snapshotState = waitForPersistentStateWithSnapshot(5000L);
        assertNotNull(snapshotState);
        assertNotNull(snapshotState.getSnapshot());

        etcdNode.stop();

        EtcdNode restartedNode = new EtcdNode("node1", raftConfig, storage);
        restartedNode.start();
        try {
            waitForRole(restartedNode, RaftRoleType.LEADER, 3000L);

            GetRequest historyRequest = new GetRequest();
            historyRequest.setKey("restart-mvcc-key");
            historyRequest.setRevision(firstRevision);
            historyRequest.setLinearizableRead(false);
            EtcdRpcResponse<GetResponse> historyResponse = restartedNode.handleEtcdRpcGetRequest(historyRequest);
            assertTrue(historyResponse.getHeader().isSuccess());
            assertNotNull(historyResponse.getBody());
            assertEquals("v1", historyResponse.getBody().getValue());

            EtcdRpcResponse<GetResponse> latestResponse = restartedNode.handleEtcdRpcGetRequest(new GetRequest("restart-mvcc-key"));
            assertTrue(latestResponse.getHeader().isSuccess());
            assertNotNull(latestResponse.getBody());
            assertEquals("v2", latestResponse.getBody().getValue());
        } finally {
            restartedNode.stop();
            etcdNode = restartedNode;
        }
    }


    private RaftPersistentState waitForPersistentStateWithSnapshot(long timeoutMillis) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (System.currentTimeMillis() < deadline) {
            RaftPersistentState state = loadRaftPersistentState();
            if (state != null && state.getSnapshot() != null) {
                return state;
            }
            Thread.sleep(50L);
        }
        return loadRaftPersistentState();
    }

    private RaftPersistentState loadRaftPersistentState() {
        byte[] data = storage.get("raft", "persistent-state");
        if (data == null) {
            return null;
        }
        return SerializerRegistry.getDefaultSerializer().deserialize(data, RaftPersistentState.class);
    }

    private void waitForRole(EtcdNode node, RaftRoleType expectedRole, long timeoutMillis) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (System.currentTimeMillis() < deadline) {
            if (node.getRole() == expectedRole) {
                return;
            }
            Thread.sleep(50L);
        }
        assertEquals(expectedRole, node.getRole());
    }
}
