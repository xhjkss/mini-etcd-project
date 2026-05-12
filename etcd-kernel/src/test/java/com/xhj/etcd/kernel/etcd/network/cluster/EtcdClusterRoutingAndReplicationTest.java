package com.xhj.etcd.kernel.etcd.network.cluster;

import com.xhj.etcd.kernel.etcd.network.support.EtcdDistributedTestSkeleton;
import com.xhj.etcd.kernel.etcd.network.support.EtcdRpcAssert;
import com.xhj.etcd.kernel.etcd.network.support.EtcdTestSupport;

import com.xhj.etcd.kernel.etcd.etcdrpc.DeleteResponse;
import com.xhj.etcd.kernel.etcd.etcdrpc.EtcdRpcResponse;
import com.xhj.etcd.kernel.etcd.etcdrpc.PutResponse;
import com.xhj.etcd.rpc.NodeEndpoint;
import org.junit.Test;

import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertNotNull;

/**
 * EtcdClusterRoutingAndReplicationTest
 *
 * @author XJks
 * @description Etcd 集群基础路由与复制测试，覆盖多规模复制、leader 重定向、leader 切换后的写入复制。
 */
public class EtcdClusterRoutingAndReplicationTest extends EtcdDistributedTestSkeleton {

    @Test
    public void shouldReplicatePutAndDeleteToFollowers() throws Exception {
        String leaderId = startClusterAndAwaitLeader(5, DEFAULT_ELECTION_TIMEOUT_MILLIS);
        NodeEndpoint leaderEndpoint = requireEndpoint(leaderId);

        EtcdRpcResponse<PutResponse> putResponse = EtcdTestSupport.callPutByRpc(harness.getTestClient(), leaderEndpoint, "k-rep", "v-rep");
        assertNotNull(putResponse);
        assertTrue(putResponse.getHeader().isSuccess());

        harness.awaitValueReplicated("k-rep", "v-rep", 8000L);

        EtcdRpcResponse<DeleteResponse> deleteResponse = EtcdTestSupport.callDeleteByRpc(harness.getTestClient(), leaderEndpoint, "k-rep");
        assertNotNull(deleteResponse);
        assertTrue(deleteResponse.getHeader().isSuccess());

        harness.awaitKeyDeleted("k-rep", 8000L);
    }

    @Test
    public void shouldReplicateOnFiveNodeCluster() throws Exception {
        String leaderId = startClusterAndAwaitLeader(5, DEFAULT_ELECTION_TIMEOUT_MILLIS);
        NodeEndpoint leaderEndpoint = requireEndpoint(leaderId);

        EtcdRpcResponse<PutResponse> putResponse = EtcdTestSupport.callPutByRpc(harness.getTestClient(), leaderEndpoint, "k-5n", "v-5n");
        assertNotNull(putResponse);
        assertTrue(putResponse.getHeader().isSuccess());

        harness.awaitValueReplicated("k-5n", "v-5n", harness.quorumSize(), 10000L);
    }

    @Test
    public void shouldReplicateOnSevenNodeClusterAfterLeaderFailover() throws Exception {
        String oldLeaderId = startClusterAndAwaitLeader(7, DEFAULT_BOUNDARY_TIMEOUT_MILLIS);
        harness.stopNode(oldLeaderId);

        String newLeaderId = awaitNewLeaderExcluding(oldLeaderId, DEFAULT_BOUNDARY_TIMEOUT_MILLIS);
        NodeEndpoint newLeaderEndpoint = requireEndpoint(newLeaderId);

        EtcdRpcResponse<PutResponse> putResponse = EtcdTestSupport.callPutByRpc(harness.getTestClient(), newLeaderEndpoint, "k-7n", "v-7n");
        assertNotNull(putResponse);
        assertTrue(putResponse.getHeader().isSuccess());

        harness.awaitValueReplicated("k-7n", "v-7n", harness.quorumSize(), 12000L);
    }

    @Test
    public void shouldRejectWriteOnFollowerWithLeaderHint() throws Exception {
        String leaderId = startClusterAndAwaitLeader(5, DEFAULT_ELECTION_TIMEOUT_MILLIS);
        String followerId = chooseFollowerId(leaderId);
        NodeEndpoint followerEndpoint = requireEndpoint(followerId);
        EtcdRpcResponse<PutResponse> response = EtcdTestSupport.callPutByRpc(harness.getTestClient(), followerEndpoint, "k-redirect", "v1");
        EtcdRpcAssert.assertNotLeaderWithLeaderHint(response);
    }

    @Test
    public void shouldRetryFollowerWriteThroughKnownLeaderEndpoint() throws Exception {
        String electedLeaderId = startClusterAndAwaitLeader(5, DEFAULT_ELECTION_TIMEOUT_MILLIS);
        String followerId = chooseFollowerId(electedLeaderId);
        NodeEndpoint followerEndpoint = requireEndpoint(followerId);
        EtcdRpcResponse<PutResponse> firstResponse = EtcdTestSupport.callPutByRpc(harness.getTestClient(), followerEndpoint, "k-retry", "v2");

        EtcdRpcAssert.assertNotLeaderWithLeaderHint(firstResponse);
        assertNotEquals(followerId, firstResponse.getHeader().getLeaderId());

        NodeEndpoint leaderEndpoint = requireEndpoint(firstResponse.getHeader().getLeaderId());
        EtcdRpcResponse<PutResponse> retryResponse = EtcdTestSupport.callPutByRpc(harness.getTestClient(), leaderEndpoint, "k-retry", "v2");

        EtcdRpcAssert.assertSuccess(retryResponse);
        assertNotNull(retryResponse.getBody());
        harness.awaitValueReplicated("k-retry", "v2", 8000L);
    }
}
