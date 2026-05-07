package com.xhj.etcd.kernel.server.node;

import com.xhj.etcd.kernel.raft.core.RaftConfig;
import com.xhj.etcd.kernel.raft.core.RaftRoleType;
import com.xhj.etcd.kernel.server.etcdrpc.DeleteRequest;
import com.xhj.etcd.kernel.server.etcdrpc.DeleteResponse;
import com.xhj.etcd.kernel.server.etcdrpc.EtcdRpcResponse;
import com.xhj.etcd.kernel.server.etcdrpc.GetRequest;
import com.xhj.etcd.kernel.server.etcdrpc.GetResponse;
import com.xhj.etcd.kernel.server.etcdrpc.PutRequest;
import com.xhj.etcd.kernel.server.etcdrpc.PutResponse;
import com.xhj.etcd.storage.Storage;
import com.xhj.etcd.storage.memory.MemoryStorage;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.Callable;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * EtcdNodeReadyAdvancePersistenceTest
 *
 * @author XJks
 * @description EtcdNode Ready/Advance 持久化与 apply 链路测试。
 */
public class EtcdNodeReadyAdvancePersistenceTest {

    private Storage storage;
    private EtcdNode node;

    @Before
    public void setUp() {
        storage = new MemoryStorage();

        RaftConfig config = new RaftConfig();
        config.setElectionTimeoutTicks(10);
        config.setHeartbeatTimeoutTicks(3);

        node = new EtcdNode("n1", config, storage);
        node.start();
    }

    @After
    public void tearDown() {
        if (node != null) {
            node.stop();
        }
    }

    @Test
    public void shouldPersistHardStateAndLogAndApplyPutAfterReadyAdvance() throws Exception {
        awaitLeader(3000L);

        EtcdRpcResponse<PutResponse> putResponse = node.handleEtcdRpcPutRequest(new PutRequest("k-ready-put", "v1"));
        assertNotNull(putResponse);
        assertNotNull(putResponse.getHeader());
        assertTrue(putResponse.getHeader().isSuccess());

        awaitTrue(new Callable<Boolean>() {
            @Override
            public Boolean call() {
                return storage.get("raft", "persistent-state") != null;
            }
        }, 3000L, "raft persistent state is not persisted");


        awaitTrue(new Callable<Boolean>() {
            @Override
            public Boolean call() {
                EtcdRpcResponse<GetResponse> getResponse = node.handleEtcdRpcGetRequest(new GetRequest("k-ready-put"));
                return getResponse != null
                        && getResponse.getBody() != null
                        && "v1".equals(getResponse.getBody().getValue());
            }
        }, 3000L, "put value is not applied");

        assertTrue(node.getLastAppliedIndex() >= 1L);
    }

    @Test
    public void shouldApplyDeleteAndUpdateStateMachineAfterReadyAdvance() throws Exception {
        awaitLeader(3000L);

        EtcdRpcResponse<PutResponse> putResponse = node.handleEtcdRpcPutRequest(new PutRequest("k-ready-del", "v2"));
        assertNotNull(putResponse);
        assertTrue(putResponse.getHeader().isSuccess());

        awaitTrue(new Callable<Boolean>() {
            @Override
            public Boolean call() {
                EtcdRpcResponse<GetResponse> getResponse = node.handleEtcdRpcGetRequest(new GetRequest("k-ready-del"));
                return getResponse != null
                        && getResponse.getBody() != null
                        && "v2".equals(getResponse.getBody().getValue());
            }
        }, 3000L, "put before delete is not applied");

        EtcdRpcResponse<DeleteResponse> deleteResponse = node.handleEtcdRpcDeleteRequest(new DeleteRequest("k-ready-del"));
        assertNotNull(deleteResponse);
        assertNotNull(deleteResponse.getHeader());
        assertTrue(deleteResponse.getHeader().isSuccess());

        awaitTrue(new Callable<Boolean>() {
            @Override
            public Boolean call() {
                EtcdRpcResponse<GetResponse> getResponse = node.handleEtcdRpcGetRequest(new GetRequest("k-ready-del"));
                return getResponse != null
                        && getResponse.getBody() != null
                        && getResponse.getBody().getValue() == null;
            }
        }, 3000L, "delete is not applied");

        EtcdRpcResponse<GetResponse> finalGet = node.handleEtcdRpcGetRequest(new GetRequest("k-ready-del"));
        assertNotNull(finalGet);
        assertNotNull(finalGet.getBody());
        assertNull(finalGet.getBody().getValue());
        assertTrue(node.getLastAppliedIndex() >= 2L);
    }

    private void awaitLeader(long timeoutMillis) throws Exception {
        awaitTrue(new Callable<Boolean>() {
            @Override
            public Boolean call() {
                return node.getRole() == RaftRoleType.LEADER;
            }
        }, timeoutMillis, "node is not leader");
        assertEquals(RaftRoleType.LEADER, node.getRole());
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
