package com.xhj.etcd.kernel.etcd.module.node;

import com.xhj.etcd.kernel.etcd.node.EtcdNode;

import com.xhj.etcd.kernel.raft.core.RaftConfig;
import com.xhj.etcd.kernel.raft.core.RaftRoleType;
import com.xhj.etcd.kernel.etcd.etcdrpc.DeleteRequest;
import com.xhj.etcd.kernel.etcd.etcdrpc.DeleteRangeRequest;
import com.xhj.etcd.kernel.etcd.etcdrpc.DeleteResponse;
import com.xhj.etcd.kernel.etcd.etcdrpc.DeleteRangeResponse;
import com.xhj.etcd.kernel.etcd.etcdrpc.EtcdRpcResponse;
import com.xhj.etcd.kernel.etcd.etcdrpc.GetRequest;
import com.xhj.etcd.kernel.etcd.etcdrpc.GetResponse;
import com.xhj.etcd.kernel.etcd.etcdrpc.PutRequest;
import com.xhj.etcd.kernel.etcd.etcdrpc.PutResponse;
import com.xhj.etcd.kernel.etcd.etcdrpc.RangeRequest;
import com.xhj.etcd.kernel.etcd.etcdrpc.RangeResponse;
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
    public void shouldRejectDeleteRangeOnFollowerWithNotLeaderHeader() {
        DeleteRangeRequest request = new DeleteRangeRequest();
        request.setStartKey("k-follower-del-range");
        request.setPrefixMatch(true);
        EtcdRpcResponse<DeleteRangeResponse> response = followerNode.handleEtcdRpcDeleteRangeRequest(request);

        assertNotNull(response);
        assertNotNull(response.getHeader());
        assertFalse(response.getHeader().isSuccess());
        assertTrue(response.getHeader().isNotLeader());
        assertNull(response.getBody());
    }

    @Test
    public void shouldServeGetAndRangeViaRaftApplyPath() throws Exception {
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

        RangeRequest rangeRequest = new RangeRequest();
        rangeRequest.setStartKey("k-read-");
        rangeRequest.setPrefixMatch(true);
        EtcdRpcResponse<RangeResponse> rangeResponse = leaderNode.handleEtcdRpcRangeRequest(rangeRequest);
        assertNotNull(rangeResponse);
        assertNotNull(rangeResponse.getHeader());
        assertTrue(rangeResponse.getHeader().isSuccess());
        assertNotNull(rangeResponse.getBody());
        assertEquals(2, rangeResponse.getBody().getCount());
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
