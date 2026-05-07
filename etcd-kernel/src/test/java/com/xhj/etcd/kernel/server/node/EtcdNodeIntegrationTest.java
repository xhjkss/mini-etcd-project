package com.xhj.etcd.kernel.server.node;

import com.xhj.etcd.kernel.raft.core.RaftConfig;
import com.xhj.etcd.kernel.raft.core.RaftRoleType;
import com.xhj.etcd.kernel.raft.storage.RaftPersistentState;
import com.xhj.etcd.kernel.server.etcdrpc.DeleteRequest;
import com.xhj.etcd.kernel.server.etcdrpc.DeleteResponse;
import com.xhj.etcd.kernel.server.etcdrpc.EtcdRpcResponse;
import com.xhj.etcd.kernel.server.etcdrpc.GetRequest;
import com.xhj.etcd.kernel.server.etcdrpc.GetResponse;
import com.xhj.etcd.kernel.server.etcdrpc.ListKeysRequest;
import com.xhj.etcd.kernel.server.etcdrpc.ListKeysResponse;
import com.xhj.etcd.kernel.server.etcdrpc.PutRequest;
import com.xhj.etcd.kernel.server.etcdrpc.PutResponse;
import com.xhj.etcd.serializer.SerializerRegistry;
import com.xhj.etcd.storage.Storage;
import com.xhj.etcd.storage.memory.MemoryStorage;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

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
        assertEquals(1, deleteResponse.getBody().getDeletedKeys());

        EtcdRpcResponse<GetResponse> getResponse = etcdNode.handleEtcdRpcGetRequest(new GetRequest("key2"));
        assertTrue(getResponse.getHeader().isSuccess());
        assertNull(getResponse.getBody().getValue());
    }

    @Test
    public void testListKeysViaRaft() throws Exception {
        etcdNode.start();
        waitForRole(etcdNode, RaftRoleType.LEADER, 3000L);

        assertTrue(etcdNode.handleEtcdRpcPutRequest(new PutRequest("a", "1")).getHeader().isSuccess());
        assertTrue(etcdNode.handleEtcdRpcPutRequest(new PutRequest("b", "2")).getHeader().isSuccess());

        EtcdRpcResponse<ListKeysResponse> listResponse = etcdNode.handleEtcdRpcListKeysRequest(new ListKeysRequest("kv"));
        assertTrue(listResponse.getHeader().isSuccess());
        assertNotNull(listResponse.getBody());

        List<String> keys = listResponse.getBody().getKeys();
        assertEquals(2, listResponse.getBody().getCount());
        assertNotNull(keys);
        assertTrue(keys.contains("a"));
        assertTrue(keys.contains("b"));
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
