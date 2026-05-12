package com.xhj.etcd.kernel.etcd.network.consistency;

import com.xhj.etcd.kernel.etcd.network.support.EtcdDistributedTestSkeleton;
import com.xhj.etcd.kernel.etcd.network.support.EtcdConsistencyAssert;
import com.xhj.etcd.kernel.etcd.network.support.EtcdTestSupport;

import com.xhj.etcd.kernel.etcd.etcdrpc.DeleteRangeRequest;
import com.xhj.etcd.kernel.etcd.etcdrpc.DeleteRangeResponse;
import com.xhj.etcd.kernel.etcd.etcdrpc.DeleteResponse;
import com.xhj.etcd.kernel.etcd.etcdrpc.EtcdRpcResponse;
import com.xhj.etcd.kernel.etcd.etcdrpc.GetResponse;
import com.xhj.etcd.kernel.etcd.etcdrpc.RangeRequest;
import com.xhj.etcd.kernel.etcd.etcdrpc.RangeResponse;
import com.xhj.etcd.rpc.NodeEndpoint;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * EtcdConsistencyMvccAndIdempotencyTest
 *
 * @author XJks
 * @description Etcd MVCC 语义与重试幂等测试，覆盖 Range/DeleteRange、Leader 切换和 delete/deleteRange 重试幂等。
 */
public class EtcdConsistencyMvccAndIdempotencyTest extends EtcdDistributedTestSkeleton {

    @Test
    public void shouldKeepRangeDeleteRangeAvailableAfterLeaderFailoverAndFollowerCatchUp() throws Exception {
        String oldLeaderId = startClusterAndAwaitLeader(3, DEFAULT_BOUNDARY_TIMEOUT_MILLIS);
        NodeEndpoint oldLeaderEndpoint = requireEndpoint(oldLeaderId);

        assertTrue(EtcdTestSupport.callPutByRpc(harness.getTestClient(), oldLeaderEndpoint, "app/a", "v1").getHeader().isSuccess());
        assertTrue(EtcdTestSupport.callPutByRpc(harness.getTestClient(), oldLeaderEndpoint, "app/b", "v2").getHeader().isSuccess());
        assertTrue(EtcdTestSupport.callPutByRpc(harness.getTestClient(), oldLeaderEndpoint, "apq/c", "v3").getHeader().isSuccess());
        harness.awaitValueReplicated("app/a", "v1", harness.getClusterSize(), 12000L);
        harness.awaitValueReplicated("app/b", "v2", harness.getClusterSize(), 12000L);
        harness.awaitValueReplicated("apq/c", "v3", harness.getClusterSize(), 12000L);

        RangeRequest rangeRequest = new RangeRequest();
        rangeRequest.setStartKey("app/");
        rangeRequest.setPrefixMatch(true);
        EtcdRpcResponse<RangeResponse> rangeResponse = EtcdTestSupport.callRangeByRpc(harness.getTestClient(), oldLeaderEndpoint, rangeRequest);
        assertNotNull(rangeResponse);
        assertTrue(rangeResponse.getHeader().isSuccess());
        assertNotNull(rangeResponse.getBody());
        assertEquals(2, rangeResponse.getBody().getCount());

        harness.stopNode(oldLeaderId);
        String newLeaderId = awaitNewLeaderExcluding(oldLeaderId, DEFAULT_BOUNDARY_TIMEOUT_MILLIS);
        assertNotNull(newLeaderId);
        assertNotEquals(oldLeaderId, newLeaderId);
        NodeEndpoint newLeaderEndpoint = requireEndpoint(newLeaderId);

        DeleteRangeRequest deleteRangeRequest = new DeleteRangeRequest();
        deleteRangeRequest.setStartKey("app/");
        deleteRangeRequest.setPrefixMatch(true);
        deleteRangeRequest.setPrevKv(true);
        EtcdRpcResponse<DeleteRangeResponse> deleteRangeResponse = EtcdTestSupport.callDeleteRangeByRpc(harness.getTestClient(), newLeaderEndpoint, deleteRangeRequest);
        assertNotNull(deleteRangeResponse);
        assertTrue(deleteRangeResponse.getHeader().isSuccess());
        assertNotNull(deleteRangeResponse.getBody());
        assertEquals(2, deleteRangeResponse.getBody().getDeletedCount());
        assertEquals(2, deleteRangeResponse.getBody().getPrevItems().size());

        harness.awaitKeyDeleted("app/a", harness.quorumSize(), 12000L);
        harness.awaitKeyDeleted("app/b", harness.quorumSize(), 12000L);
        harness.awaitValueReplicated("apq/c", "v3", harness.quorumSize(), 12000L);

        harness.restartNode(oldLeaderId);
        harness.awaitValueVisibleOnNode(oldLeaderId, "app/a", null, 12000L);
        harness.awaitValueVisibleOnNode(oldLeaderId, "app/b", null, 12000L);
        harness.awaitValueVisibleOnNode(oldLeaderId, "apq/c", "v3", 12000L);
    }

    @Test
    public void shouldRouteLinearizableAndLocalRangeDifferentlyOnFollower() throws Exception {
        String leaderId = startClusterAndAwaitLeader(3, DEFAULT_BOUNDARY_TIMEOUT_MILLIS);
        NodeEndpoint leaderEndpoint = requireEndpoint(leaderId);

        assertTrue(EtcdTestSupport.callPutByRpc(harness.getTestClient(), leaderEndpoint, "svc/a", "1").getHeader().isSuccess());
        assertTrue(EtcdTestSupport.callPutByRpc(harness.getTestClient(), leaderEndpoint, "svc/b", "2").getHeader().isSuccess());
        harness.awaitValueReplicated("svc/a", "1", harness.getClusterSize(), 12000L);
        harness.awaitValueReplicated("svc/b", "2", harness.getClusterSize(), 12000L);

        String followerId = chooseFollowerId(leaderId);
        NodeEndpoint followerEndpoint = requireEndpoint(followerId);

        RangeRequest linearizableRangeRequest = new RangeRequest();
        linearizableRangeRequest.setStartKey("svc/");
        linearizableRangeRequest.setPrefixMatch(true);
        linearizableRangeRequest.setLinearizableRead(true);
        EtcdRpcResponse<RangeResponse> linearizableResponse = EtcdTestSupport.callRangeByRpc(
                harness.getTestClient(),
                followerEndpoint,
                linearizableRangeRequest);
        assertNotNull(linearizableResponse);
        assertNotNull(linearizableResponse.getHeader());
        assertFalse(linearizableResponse.getHeader().isSuccess());
        assertTrue(linearizableResponse.getHeader().isNotLeader());

        RangeRequest localRangeRequest = new RangeRequest();
        localRangeRequest.setStartKey("svc/");
        localRangeRequest.setPrefixMatch(true);
        localRangeRequest.setLinearizableRead(false);
        EtcdRpcResponse<RangeResponse> localResponse = EtcdTestSupport.callRangeByRpc(
                harness.getTestClient(),
                followerEndpoint,
                localRangeRequest);
        assertNotNull(localResponse);
        assertNotNull(localResponse.getHeader());
        assertTrue(localResponse.getHeader().isSuccess());
        assertNotNull(localResponse.getBody());
        assertEquals(2, localResponse.getBody().getCount());
    }

    @Test
    public void shouldRejectDeleteRangeOnFollowerWithLeaderHintOverRealRpc() throws Exception {
        String leaderId = startClusterAndAwaitLeader(3, DEFAULT_BOUNDARY_TIMEOUT_MILLIS);
        String followerId = chooseFollowerId(leaderId);
        NodeEndpoint followerEndpoint = requireEndpoint(followerId);

        DeleteRangeRequest followerDeleteRangeRequest = new DeleteRangeRequest();
        followerDeleteRangeRequest.setStartKey("follower/delete-range/");
        followerDeleteRangeRequest.setPrefixMatch(true);
        EtcdRpcResponse<DeleteRangeResponse> followerDeleteRangeResponse = EtcdTestSupport.callDeleteRangeByRpc(
                harness.getTestClient(),
                followerEndpoint,
                followerDeleteRangeRequest);
        assertNotNull(followerDeleteRangeResponse);
        assertNotNull(followerDeleteRangeResponse.getHeader());
        assertFalse(followerDeleteRangeResponse.getHeader().isSuccess());
        assertTrue(followerDeleteRangeResponse.getHeader().isNotLeader());
        assertNotNull(followerDeleteRangeResponse.getHeader().getLeaderId());
    }

    @Test
    public void shouldKeepDeleteRangeIdempotentAcrossRetryAndLeaderFailover() throws Exception {
        startClusterAndAwaitLeader(3, DEFAULT_BOUNDARY_TIMEOUT_MILLIS);

        List<String> keys = buildKeySpace("retry/dr/", 6);
        for (String key : keys) {
            putOnLeaderWithRetry(key, "value-" + key, 12000L);
        }
        for (String key : keys) {
            harness.awaitValueReplicated(key, "value-" + key, harness.getClusterSize(), 12000L);
        }

        DeleteRangeRequest firstRequest = new DeleteRangeRequest();
        firstRequest.setStartKey("retry/dr/");
        firstRequest.setPrefixMatch(true);
        firstRequest.setPrevKv(true);
        DeleteRangeResponse firstResponse = deleteRangeOnLeaderWithRetry(firstRequest, 12000L);
        assertNotNull(firstResponse);
        assertEquals(keys.size(), firstResponse.getDeletedCount());
        assertEquals(keys.size(), firstResponse.getPrevItems().size());
        long firstRevision = firstResponse.getRevision();
        assertTrue(firstRevision > 0L);

        String oldLeaderId = awaitLeader(12000L);
        harness.stopNode(oldLeaderId);
        String newLeaderId = awaitNewLeaderExcluding(oldLeaderId, 15000L);
        assertNotNull(newLeaderId);
        harness.restartNode(oldLeaderId);
        awaitLeader(15000L);

        DeleteRangeRequest retryRequest = new DeleteRangeRequest();
        retryRequest.setStartKey("retry/dr/");
        retryRequest.setPrefixMatch(true);
        retryRequest.setPrevKv(true);
        DeleteRangeResponse retryResponse = deleteRangeOnLeaderWithRetry(retryRequest, 12000L);
        assertNotNull(retryResponse);
        assertEquals(0, retryResponse.getDeletedCount());
        assertEquals(0, retryResponse.getPrevItems().size());
        assertTrue(retryResponse.getRevision() >= firstRevision);

        for (String key : keys) {
            harness.awaitValueReplicated(key, null, harness.getClusterSize(), 15000L);
            GetResponse getResponse = getLinearizableFromLeaderWithRetry(key, 12000L);
            EtcdConsistencyAssert.assertGetValueEquals("delete-range-retry key=" + key, null, getResponse);
        }
    }

    @Test
    public void shouldKeepDeleteIdempotentAcrossMultipleRetries() throws Exception {
        startClusterAndAwaitLeader(3, DEFAULT_BOUNDARY_TIMEOUT_MILLIS);

        String key = "retry/delete/single";
        putOnLeaderWithRetry(key, "v1", 12000L);
        harness.awaitValueReplicated(key, "v1", harness.getClusterSize(), 12000L);

        DeleteResponse firstDeleteResponse = deleteOnLeaderWithRetry(key, 12000L);
        assertNotNull(firstDeleteResponse);
        assertEquals(1, firstDeleteResponse.getDeletedCount());

        DeleteResponse secondDeleteResponse = deleteOnLeaderWithRetry(key, 12000L);
        assertNotNull(secondDeleteResponse);
        assertEquals(0, secondDeleteResponse.getDeletedCount());

        injectFault(FaultInjectionType.RESTART_LEADER, new StringBuilder());

        DeleteResponse thirdDeleteResponse = deleteOnLeaderWithRetry(key, 12000L);
        assertNotNull(thirdDeleteResponse);
        assertEquals(0, thirdDeleteResponse.getDeletedCount());

        harness.awaitValueReplicated(key, null, harness.getClusterSize(), 12000L);
        GetResponse finalGetResponse = getLinearizableFromLeaderWithRetry(key, 12000L);
        EtcdConsistencyAssert.assertGetValueEquals("delete-retry-final key=" + key, null, finalGetResponse);
    }

    private List<String> buildKeySpace(String prefix, int count) {
        List<String> keys = new ArrayList<>();
        for (int index = 0; index < count; index++) {
            keys.add(prefix + index);
        }
        return keys;
    }
}
