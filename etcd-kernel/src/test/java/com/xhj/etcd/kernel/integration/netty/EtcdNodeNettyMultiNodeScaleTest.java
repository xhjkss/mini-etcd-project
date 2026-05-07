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
 * EtcdNodeNettyMultiNodeScaleTest
 *
 * @author XJks
 * @description EtcdNode 多节点规模场景测试。
 */
public class EtcdNodeNettyMultiNodeScaleTest {

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
    public void shouldReplicateOnFiveNodeCluster() throws Exception {
        harness.startCluster(5);

        String leaderId = harness.awaitLeaderElected(10000L);
        NodeEndpoint leader = harness.getEndpoint(leaderId);

        EtcdRpcResponse<PutResponse> put = EtcdNettyTestSupport.callPutByRpc(harness.getTestClient(), leader, "k-5n", "v-5n");
        assertNotNull(put);
        assertTrue(put.getHeader().isSuccess());

        harness.awaitValueReplicated("k-5n", "v-5n", harness.quorumSize(), 10000L);
    }

    @Test
    public void shouldReplicateOnSevenNodeClusterAfterLeaderFailover() throws Exception {
        harness.startCluster(7);

        String oldLeaderId = harness.awaitLeaderElected(12000L);
        harness.stopNode(oldLeaderId);

        String newLeaderId = harness.awaitLeaderElectedExcluding(oldLeaderId, 12000L);
        NodeEndpoint newLeader = harness.getEndpoint(newLeaderId);

        EtcdRpcResponse<PutResponse> put = EtcdNettyTestSupport.callPutByRpc(harness.getTestClient(), newLeader, "k-7n", "v-7n");
        assertNotNull(put);
        assertTrue(put.getHeader().isSuccess());

        harness.awaitValueReplicated("k-7n", "v-7n", harness.quorumSize(), 12000L);
    }
}

