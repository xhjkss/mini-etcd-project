package com.xhj.etcd.kernel.integration.netty;

import com.xhj.etcd.kernel.server.etcdrpc.EtcdRpcResponse;
import com.xhj.etcd.kernel.server.etcdrpc.PutResponse;
import com.xhj.etcd.rpc.NodeEndpoint;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * EtcdNodeNettyRestartConsistencyTest
 *
 * @author XJks
 * @description EtcdNode Netty 联调重启一致性测试。
 */
public class EtcdNodeNettyRestartConsistencyTest {

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
    public void shouldKeepReadableValueAfterFollowerRestart() throws Exception {
        harness.startCluster(5);

        String leaderId = harness.awaitLeaderElected(10000L);
        String followerId = "n1".equals(leaderId) ? "n2" : "n1";

        NodeEndpoint leaderEndpoint = harness.getEndpoint(leaderId);
        EtcdRpcResponse<PutResponse> putResponse = EtcdNettyTestSupport.callPutByRpc(harness.getTestClient(), leaderEndpoint, "k-restart-readable", "v1");
        assertNotNull(putResponse);
        assertNotNull(putResponse.getHeader());
        assertTrue(putResponse.getHeader().isSuccess());

        harness.awaitValueReplicated("k-restart-readable", "v1", 8000L);

        harness.stopNode(followerId);
        harness.restartNode(followerId);

        harness.awaitValueReplicated("k-restart-readable", "v1", 12000L);
    }

    @Test
    public void shouldReplicateNewWritesToRestartedFollower() throws Exception {
        harness.startCluster(5);

        String leaderId = harness.awaitLeaderElected(10000L);
        String stoppedFollowerId = "n1".equals(leaderId) ? "n2" : "n1";
        NodeEndpoint leaderEndpoint = harness.getEndpoint(leaderId);

        harness.stopNode(stoppedFollowerId);

        EtcdRpcResponse<PutResponse> putResponse1 = EtcdNettyTestSupport.callPutByRpc(harness.getTestClient(), leaderEndpoint, "k-restart-after-up-1", "v2-1");
        assertNotNull(putResponse1);
        assertNotNull(putResponse1.getHeader());
        assertTrue(putResponse1.getHeader().isSuccess());

        EtcdRpcResponse<PutResponse> putResponse2 = EtcdNettyTestSupport.callPutByRpc(harness.getTestClient(), leaderEndpoint, "k-restart-after-up-2", "v2-2");
        assertNotNull(putResponse2);
        assertNotNull(putResponse2.getHeader());
        assertTrue(putResponse2.getHeader().isSuccess());

        harness.restartNode(stoppedFollowerId);

        harness.awaitValueReplicated("k-restart-after-up-1", "v2-1", 12000L);
        harness.awaitValueReplicated("k-restart-after-up-2", "v2-2", 12000L);
    }
}

