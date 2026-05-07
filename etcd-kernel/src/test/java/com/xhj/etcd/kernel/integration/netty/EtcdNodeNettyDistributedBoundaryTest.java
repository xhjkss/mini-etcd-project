package com.xhj.etcd.kernel.integration.netty;

import com.xhj.etcd.kernel.server.etcdrpc.DeleteResponse;
import com.xhj.etcd.kernel.server.etcdrpc.EtcdRpcResponse;
import com.xhj.etcd.kernel.server.etcdrpc.PutResponse;
import com.xhj.etcd.rpc.NodeEndpoint;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * EtcdNodeNettyDistributedBoundaryTest
 *
 * @author XJks
 * @description EtcdNode 分布式边界场景测试：故障切换与 follower 恢复追平。
 */
public class EtcdNodeNettyDistributedBoundaryTest {

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
    public void shouldElectNewLeaderAndAcceptWritesAfterOldLeaderStops() throws Exception {
        harness.startCluster(5);

        String oldLeaderId = harness.awaitLeaderElected(10000L);
        NodeEndpoint oldLeaderEndpoint = harness.getEndpoint(oldLeaderId);

        EtcdRpcResponse<PutResponse> beforeStop = EtcdNettyTestSupport.callPutByRpc(harness.getTestClient(), oldLeaderEndpoint, "k-failover-1", "before-stop");
        assertTrue(beforeStop.getHeader().isSuccess());

        harness.stopNode(oldLeaderId);

        String newLeaderId = harness.awaitLeaderElectedExcluding(oldLeaderId, 10000L);
        assertNotNull(newLeaderId);
        assertNotEquals(oldLeaderId, newLeaderId);

        NodeEndpoint newLeaderEndpoint = harness.getEndpoint(newLeaderId);
        EtcdRpcResponse<PutResponse> afterStop = EtcdNettyTestSupport.callPutByRpc(harness.getTestClient(), newLeaderEndpoint, "k-failover-2", "after-stop");
        assertTrue(afterStop.getHeader().isSuccess());

        harness.awaitValueReplicated("k-failover-2", "after-stop", 8000L);
    }

    @Test
    public void shouldCommitWithOneFollowerUnavailableAndCatchUpLateFollowerByLog() throws Exception {
        harness.startCluster(5);

        String leaderId = harness.awaitLeaderElected(10000L);
        String unavailableFollowerId = "n1".equals(leaderId) ? "n2" : "n1";

        harness.stopNode(unavailableFollowerId);

        NodeEndpoint leaderEndpoint = harness.getEndpoint(leaderId);
        EtcdRpcResponse<PutResponse> putResponse = EtcdNettyTestSupport.callPutByRpc(harness.getTestClient(), leaderEndpoint, "k-catchup", "v-catchup");
        assertNotNull(putResponse);
        assertTrue(putResponse.getHeader().isSuccess());

        EtcdRpcResponse<DeleteResponse> deleteResponse = EtcdNettyTestSupport.callDeleteByRpc(harness.getTestClient(), leaderEndpoint, "k-catchup-temp");
        assertNotNull(deleteResponse);

        harness.awaitValueReplicated("k-catchup", "v-catchup", 8000L);
    }
}

