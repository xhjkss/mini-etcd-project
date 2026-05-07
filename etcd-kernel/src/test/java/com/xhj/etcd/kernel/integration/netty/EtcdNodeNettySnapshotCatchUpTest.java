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
 * EtcdNodeNettySnapshotCatchUpTest
 *
 * @author XJks
 * @description EtcdNode follower 恢复后追平一致性场景测试。
 */
public class EtcdNodeNettySnapshotCatchUpTest {

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
    public void shouldCatchUpRecoveredFollowerByLogReplication() throws Exception {
        harness.startCluster(5);

        String leaderId = harness.awaitLeaderElected(10000L);
        String stoppedFollowerId = "n1".equals(leaderId) ? "n2" : "n1";

        harness.stopNode(stoppedFollowerId);

        NodeEndpoint leaderEndpoint = harness.getEndpoint(leaderId);
        EtcdRpcResponse<PutResponse> put1 = EtcdNettyTestSupport.callPutByRpc(harness.getTestClient(), leaderEndpoint, "k-catchup-1", "v1");
        EtcdRpcResponse<PutResponse> put2 = EtcdNettyTestSupport.callPutByRpc(harness.getTestClient(), leaderEndpoint, "k-catchup-2", "v2");

        assertNotNull(put1);
        assertNotNull(put2);
        assertTrue(put1.getHeader().isSuccess());
        assertTrue(put2.getHeader().isSuccess());

        harness.awaitValueReplicated("k-catchup-1", "v1", 8000L);
        harness.awaitValueReplicated("k-catchup-2", "v2", 8000L);
    }
}

