package com.xhj.etcd.kernel.server.node;

import com.xhj.etcd.kernel.raft.core.RaftConfig;
import com.xhj.etcd.kernel.raft.core.RaftRoleType;
import com.xhj.etcd.kernel.server.etcdrpc.DeleteRequest;
import com.xhj.etcd.kernel.server.etcdrpc.DeleteResponse;
import com.xhj.etcd.kernel.server.etcdrpc.EtcdRpcResponse;
import com.xhj.etcd.kernel.server.etcdrpc.GetRequest;
import com.xhj.etcd.kernel.server.etcdrpc.GetResponse;
import com.xhj.etcd.kernel.server.etcdrpc.ListKeysRequest;
import com.xhj.etcd.kernel.server.etcdrpc.ListKeysResponse;
import com.xhj.etcd.kernel.server.etcdrpc.PutRequest;
import com.xhj.etcd.kernel.server.etcdrpc.PutResponse;
import com.xhj.etcd.storage.Storage;
import com.xhj.etcd.storage.memory.MemoryStorage;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.Callable;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * EtcdNodeRpcSemanticTest
 *
 * @author XJks
 * @description EtcdNode RPC 语义测试，覆盖写请求 leader 约束与强一致读链路行为。
 */
public class EtcdNodeRpcSemanticTest {

    private Storage storage;
    private EtcdNode leaderNode;
    private EtcdNode followerNode;

    @Before
    public void setUp() {
        storage = new MemoryStorage();

        RaftConfig singleNodeConfig = new RaftConfig();
        singleNodeConfig.setElectionTimeoutTicks(10);
        singleNodeConfig.setHeartbeatTimeoutTicks(3);

        RaftConfig followerConfig = new RaftConfig();
        followerConfig.setElectionTimeoutTicks(10);
        followerConfig.setHeartbeatTimeoutTicks(3);
        followerConfig.getPeerNodeIds().add("n1");

        leaderNode = new EtcdNode("n1", singleNodeConfig, storage);
        followerNode = new EtcdNode("n2", followerConfig, new MemoryStorage());

        leaderNode.start();
        followerNode.start();
    }

    @After
    public void tearDown() {
        if (leaderNode != null) {
            leaderNode.stop();
        }
        if (followerNode != null) {
            followerNode.stop();
        }
    }

    @Test
    public void shouldRejectPutOnFollowerWithNotLeaderHeader() {
        PutRequest request = new PutRequest("k-follower-put", "v");
        EtcdRpcResponse<PutResponse> response = followerNode.handleEtcdRpcPutRequest(request);

        assertNotNull(response);
        assertNotNull(response.getHeader());
        assertFalse(response.getHeader().isSuccess());
        assertTrue(response.getHeader().isNotLeader());
        assertNull(response.getBody());
    }

    @Test
    public void shouldRejectDeleteOnFollowerWithNotLeaderHeader() {
        DeleteRequest request = new DeleteRequest("k-follower-del");
        EtcdRpcResponse<DeleteResponse> response = followerNode.handleEtcdRpcDeleteRequest(request);

        assertNotNull(response);
        assertNotNull(response.getHeader());
        assertFalse(response.getHeader().isSuccess());
        assertTrue(response.getHeader().isNotLeader());
        assertNull(response.getBody());
    }

    @Test
    public void shouldServeGetAndListKeysViaRaftApplyPath() throws Exception {
        awaitLeader(3000L);

        EtcdRpcResponse<PutResponse> putResponse1 = leaderNode.handleEtcdRpcPutRequest(new PutRequest("k-read-1", "v1"));
        assertNotNull(putResponse1);
        assertNotNull(putResponse1.getHeader());
        assertTrue(putResponse1.getHeader().isSuccess());

        EtcdRpcResponse<PutResponse> putResponse2 = leaderNode.handleEtcdRpcPutRequest(new PutRequest("k-read-2", "v2"));
        assertNotNull(putResponse2);
        assertNotNull(putResponse2.getHeader());
        assertTrue(putResponse2.getHeader().isSuccess());

        awaitTrue(new Callable<Boolean>() {
            @Override
            public Boolean call() {
                EtcdRpcResponse<GetResponse> getResponse = leaderNode.handleEtcdRpcGetRequest(new GetRequest("k-read-1"));
                return getResponse != null
                        && getResponse.getHeader() != null
                        && getResponse.getHeader().isSuccess()
                        && getResponse.getBody() != null
                        && "v1".equals(getResponse.getBody().getValue());
            }
        }, 3000L, "get does not return applied value");

        EtcdRpcResponse<ListKeysResponse> listResponse = leaderNode.handleEtcdRpcListKeysRequest(new ListKeysRequest("kv"));
        assertNotNull(listResponse);
        assertNotNull(listResponse.getHeader());
        assertTrue(listResponse.getHeader().isSuccess());
        assertNotNull(listResponse.getBody());
        assertNotNull(listResponse.getBody().getKeys());
        assertTrue(listResponse.getBody().getKeys().contains("k-read-1"));
        assertTrue(listResponse.getBody().getKeys().contains("k-read-2"));
    }

    private void awaitLeader(long timeoutMillis) throws Exception {
        awaitTrue(new Callable<Boolean>() {
            @Override
            public Boolean call() {
                return leaderNode.getRole() == RaftRoleType.LEADER;
            }
        }, timeoutMillis, "leader node is not elected");
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
