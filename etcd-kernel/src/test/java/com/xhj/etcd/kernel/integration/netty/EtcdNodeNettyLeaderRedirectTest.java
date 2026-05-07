package com.xhj.etcd.kernel.integration.netty;

import com.xhj.etcd.kernel.server.etcdrpc.EtcdRpcResponse;
import com.xhj.etcd.kernel.server.etcdrpc.PutResponse;
import com.xhj.etcd.rpc.NodeEndpoint;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * EtcdNodeNettyLeaderRedirectTest
 *
 * @author XJks
 * @description EtcdNode 通过 Netty RPC 的 follower 重定向场景测试。
 */
public class EtcdNodeNettyLeaderRedirectTest {

    private EtcdNettyClusterTestHarness harness;

    @Before
    public void setUp() {
        harness = new EtcdNettyClusterTestHarness();
    }

    @After
    public void tearDown() {
        if (harness != null) {
            harness.stopAll();
        }
    }

    @Test
    public void shouldRejectWriteOnFollowerWithLeaderHint() throws Exception {
        harness.startCluster(5);

        String leaderId = harness.awaitLeaderElected(10000L);
        String followerId = "n1".equals(leaderId) ? "n2" : "n1";

        NodeEndpoint followerEndpoint = harness.getEndpoint(followerId);
        EtcdRpcResponse<PutResponse> response = EtcdNettyTestSupport.callPutByRpc(harness.getTestClient(), followerEndpoint, "k-redirect", "v1");

        assertNotNull(response);
        assertNotNull(response.getHeader());
        assertFalse(response.getHeader().isSuccess());
        assertTrue(response.getHeader().isNotLeader());
        assertNotNull(response.getHeader().getLeaderId());
    }

    @Test
    public void shouldRetryFollowerWriteThroughKnownLeaderEndpoint() throws Exception {
        harness.startCluster(5);

        String electedLeaderId = harness.awaitLeaderElected(10000L);
        String followerId = "n1".equals(electedLeaderId) ? "n2" : "n1";

        NodeEndpoint followerEndpoint = harness.getEndpoint(followerId);
        EtcdRpcResponse<PutResponse> firstResponse = EtcdNettyTestSupport.callPutByRpc(harness.getTestClient(), followerEndpoint, "k-retry", "v2");

        assertNotNull(firstResponse);
        assertNotNull(firstResponse.getHeader());
        assertTrue(firstResponse.getHeader().isNotLeader());
        assertNotNull(firstResponse.getHeader().getLeaderId());
        assertNotEquals(followerId, firstResponse.getHeader().getLeaderId());

        NodeEndpoint leaderEndpoint = harness.getEndpoint(firstResponse.getHeader().getLeaderId());
        EtcdRpcResponse<PutResponse> retryResponse = EtcdNettyTestSupport.callPutByRpc(harness.getTestClient(), leaderEndpoint, "k-retry", "v2");

        assertNotNull(retryResponse);
        assertNotNull(retryResponse.getHeader());
        assertTrue(retryResponse.getHeader().isSuccess());

        harness.awaitValueReplicated("k-retry", "v2", 8000L);
    }
}

