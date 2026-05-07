package com.xhj.etcd.kernel.integration.netty;

import com.xhj.etcd.kernel.server.etcdrpc.DeleteResponse;
import com.xhj.etcd.kernel.server.etcdrpc.EtcdRpcResponse;
import com.xhj.etcd.kernel.server.etcdrpc.PutResponse;
import com.xhj.etcd.rpc.NodeEndpoint;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * EtcdNodeNettyClusterReplicationTest
 *
 * @author XJks
 * @description EtcdNode 多节点下 put/delete 复制一致性测试。
 */
public class EtcdNodeNettyClusterReplicationTest {

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
    public void shouldReplicatePutAndDeleteToFollowers() throws Exception {
        harness.startCluster(5);

        String leaderId = harness.awaitLeaderElected(10000L);
        NodeEndpoint leaderEndpoint = harness.getEndpoint(leaderId);

        EtcdRpcResponse<PutResponse> putResponse = EtcdNettyTestSupport.callPutByRpc(harness.getTestClient(), leaderEndpoint, "k-rep", "v-rep");
        assertNotNull(putResponse);
        assertTrue(putResponse.getHeader().isSuccess());

        harness.awaitValueReplicated("k-rep", "v-rep", 8000L);

        EtcdRpcResponse<DeleteResponse> deleteResponse = EtcdNettyTestSupport.callDeleteByRpc(harness.getTestClient(), leaderEndpoint, "k-rep");
        assertNotNull(deleteResponse);
        assertTrue(deleteResponse.getHeader().isSuccess());

        harness.awaitKeyDeleted("k-rep", 8000L);
    }
}

