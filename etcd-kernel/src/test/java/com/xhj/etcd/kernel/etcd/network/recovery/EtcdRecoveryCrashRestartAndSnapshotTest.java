package com.xhj.etcd.kernel.etcd.network.recovery;

import com.xhj.etcd.kernel.etcd.network.support.EtcdDistributedTestSkeleton;
import com.xhj.etcd.kernel.etcd.network.support.EtcdTestSupport;

import com.xhj.etcd.kernel.etcd.etcdrpc.EtcdRpcResponse;
import com.xhj.etcd.kernel.etcd.etcdrpc.GetResponse;
import com.xhj.etcd.kernel.etcd.etcdrpc.PutResponse;
import com.xhj.etcd.rpc.NodeEndpoint;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * EtcdRecoveryCrashRestartAndSnapshotTest
 *
 * @author XJks
 * @description Etcd 崩溃恢复与快照追平测试，覆盖重启一致性、多数派恢复与 snapshot catch-up。
 */
public class EtcdRecoveryCrashRestartAndSnapshotTest extends EtcdDistributedTestSkeleton {

    /**
     * 全量崩溃重启恢复测试：
     * 先写入一批已提交数据，再全量崩溃并重启所有节点，最后验证历史数据仍可读且后续写入可继续复制。
     */
    @Test
    public void shouldRecoverCommittedDataAfterFullClusterCrashAndRestart() throws Exception {
        startClusterAndAwaitLeader(5, DEFAULT_BOUNDARY_TIMEOUT_MILLIS);

        putOnLeaderWithRetry("recovery-full-crash-k1", "v-1", 12000L);
        putOnLeaderWithRetry("recovery-full-crash-k2", "v-2", 12000L);
        putOnLeaderWithRetry("recovery-full-crash-k3", "v-3", 12000L);
        harness.awaitValueReplicated("recovery-full-crash-k1", "v-1", harness.getClusterSize(), 12000L);
        harness.awaitValueReplicated("recovery-full-crash-k2", "v-2", harness.getClusterSize(), 12000L);
        harness.awaitValueReplicated("recovery-full-crash-k3", "v-3", harness.getClusterSize(), 12000L);

        List<String> nodeIds = new ArrayList<>(harness.getNodeIds());
        for (String nodeId : nodeIds) {
            harness.stopNode(nodeId);
        }
        Thread.sleep(1000L);
        for (String nodeId : nodeIds) {
            harness.restartNode(nodeId);
        }
        awaitLeader(15000L);

        harness.awaitValueReplicated("recovery-full-crash-k1", "v-1", harness.getClusterSize(), 15000L);
        harness.awaitValueReplicated("recovery-full-crash-k2", "v-2", harness.getClusterSize(), 15000L);
        harness.awaitValueReplicated("recovery-full-crash-k3", "v-3", harness.getClusterSize(), 15000L);

        putOnLeaderWithRetry("recovery-full-crash-k4", "v-4", 12000L);
        harness.awaitValueReplicated("recovery-full-crash-k4", "v-4", harness.getClusterSize(), 15000L);
    }

    /**
     * 借鉴 MIT6.824 的 partition / rejoin / persist 场景：
     * leader 在失去多数派时不能继续提交写请求；当多数派恢复后，写请求应再次可提交并复制到可用节点。
     */
    @Test
    public void shouldRejectWritesWhileMajorityIsLostAndResumeAfterFollowersReturn() throws Exception {
        String leaderId = startClusterAndAwaitLeader(5, DEFAULT_BOUNDARY_TIMEOUT_MILLIS);
        NodeEndpoint initialLeaderEndpoint = requireEndpoint(leaderId);
        List<String> stoppedFollowerIds = new ArrayList<>(stopFollowers(leaderId, 3));
        assertEquals(3, stoppedFollowerIds.size());

        EtcdRpcResponse<PutResponse> failedResponse = tryPutOnce(initialLeaderEndpoint, "recovery-majority-key", "before-majority", 4000L);
        if (failedResponse != null && failedResponse.getHeader() != null) {
            assertTrue(!failedResponse.getHeader().isSuccess());
        }

        harness.restartNode(stoppedFollowerIds.get(0));
        harness.restartNode(stoppedFollowerIds.get(1));

        awaitLeader(15000L);
        putOnLeaderWithRetry("recovery-majority-key", "after-majority", 15000L);
        harness.awaitValueReplicated("recovery-majority-key", "after-majority", harness.quorumSize(), 15000L);

        NodeEndpoint recoveredLeaderEndpoint = harness.awaitLeaderEndpoint(15000L);
        EtcdRpcResponse<GetResponse> readResponse = EtcdTestSupport.callGetByRpc(
                harness.getTestClient(),
                recoveredLeaderEndpoint,
                "recovery-majority-key");
        assertNotNull(readResponse);
        assertNotNull(readResponse.getBody());
        String leaderValue = readResponse.getBody().getValue();
        assertEquals("after-majority", leaderValue);
    }

    /**
     * 借鉴 MIT6.824 的 snapshot RPC / snapshot recover 场景：
     * follower 长时间离线后重启，若 leader 已经压缩日志，则 follower 应通过 snapshot 追平并恢复可读状态。
     */
    @Test
    public void shouldInstallSnapshotAndRecoverLaggingFollowerAfterRestart() throws Exception {
        setSnapshotTriggerLogCount(2);
        String leaderId = startClusterAndAwaitLeader(3, DEFAULT_BOUNDARY_TIMEOUT_MILLIS);
        String laggingFollowerId = chooseFollowerId(leaderId);
        harness.stopNode(laggingFollowerId);

        String latestKey = null;
        for (int i = 1; i <= 12; i++) {
            latestKey = "recovery-snapshot-key-" + i;
            putOnLeaderWithRetry(latestKey, "value-" + i, 12000L);
        }

        harness.awaitPersistedSnapshotOnNode(leaderId, 15000L);
        assertTrue(harness.hasPersistedSnapshot(leaderId));

        harness.restartNode(laggingFollowerId);
        harness.awaitPersistedSnapshotOnNode(laggingFollowerId, 15000L);
        assertTrue(harness.hasPersistedSnapshot(laggingFollowerId));
        assertNotNull(harness.getPersistentState(laggingFollowerId).getSnapshot());
        assertTrue(harness.getPersistentState(laggingFollowerId).getSnapshot().getLastIncludedIndex() > 0L);

        harness.awaitValueVisibleOnNode(laggingFollowerId, latestKey, "value-12", 15000L);
        harness.awaitValueReplicated(latestKey, "value-12", harness.quorumSize(), 15000L);
    }

    @Test
    public void shouldKeepReadableValueAfterFollowerRestart() throws Exception {
        String leaderId = startClusterAndAwaitLeader(5, DEFAULT_ELECTION_TIMEOUT_MILLIS);
        String followerId = chooseFollowerId(leaderId);

        NodeEndpoint leaderEndpoint = requireEndpoint(leaderId);
        EtcdRpcResponse<PutResponse> putResponse = EtcdTestSupport.callPutByRpc(harness.getTestClient(), leaderEndpoint, "k-restart-readable", "v1");
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
        String leaderId = startClusterAndAwaitLeader(5, DEFAULT_ELECTION_TIMEOUT_MILLIS);
        String stoppedFollowerId = chooseFollowerId(leaderId);
        NodeEndpoint leaderEndpoint = requireEndpoint(leaderId);

        harness.stopNode(stoppedFollowerId);

        EtcdRpcResponse<PutResponse> putResponse1 = EtcdTestSupport.callPutByRpc(harness.getTestClient(), leaderEndpoint, "k-restart-after-up-1", "v2-1");
        EtcdRpcResponse<PutResponse> putResponse2 = EtcdTestSupport.callPutByRpc(harness.getTestClient(), leaderEndpoint, "k-restart-after-up-2", "v2-2");
        assertNotNull(putResponse1);
        assertNotNull(putResponse2);
        assertTrue(putResponse1.getHeader().isSuccess());
        assertTrue(putResponse2.getHeader().isSuccess());

        harness.restartNode(stoppedFollowerId);

        harness.awaitValueReplicated("k-restart-after-up-1", "v2-1", 12000L);
        harness.awaitValueReplicated("k-restart-after-up-2", "v2-2", 12000L);
    }

    @Test
    public void shouldKeepHistoricalRevisionReadableAfterFollowerRestart() throws Exception {
        String leaderId = startClusterAndAwaitLeader(5, DEFAULT_ELECTION_TIMEOUT_MILLIS);
        String followerId = chooseFollowerId(leaderId);
        NodeEndpoint leaderEndpoint = requireEndpoint(leaderId);

        EtcdRpcResponse<PutResponse> firstPutResponse = EtcdTestSupport.callPutByRpc(
                harness.getTestClient(),
                leaderEndpoint,
                "k-restart-history",
                "v1");
        assertNotNull(firstPutResponse);
        assertNotNull(firstPutResponse.getHeader());
        assertTrue(firstPutResponse.getHeader().isSuccess());
        assertNotNull(firstPutResponse.getBody());
        long firstRevision = firstPutResponse.getBody().getRevision();

        EtcdRpcResponse<PutResponse> secondPutResponse = EtcdTestSupport.callPutByRpc(
                harness.getTestClient(),
                leaderEndpoint,
                "k-restart-history",
                "v2");
        assertNotNull(secondPutResponse);
        assertNotNull(secondPutResponse.getHeader());
        assertTrue(secondPutResponse.getHeader().isSuccess());

        harness.awaitValueReplicated("k-restart-history", "v2", harness.getClusterSize(), 12000L);

        harness.stopNode(followerId);
        harness.restartNode(followerId);
        harness.awaitValueVisibleOnNode(followerId, "k-restart-history", "v2", 12000L);

        NodeEndpoint followerEndpoint = requireEndpoint(followerId);
        EtcdRpcResponse<GetResponse> historyResponse = EtcdTestSupport.callGetByRpc(
                harness.getTestClient(),
                followerEndpoint,
                "k-restart-history",
                firstRevision,
                false);
        assertNotNull(historyResponse);
        assertNotNull(historyResponse.getHeader());
        assertTrue(historyResponse.getHeader().isSuccess());
        assertNotNull(historyResponse.getBody());
        assertEquals("v1", historyResponse.getBody().getValue());
    }

    @Test
    public void shouldCatchUpRecoveredFollowerByLogReplication() throws Exception {
        String leaderId = startClusterAndAwaitLeader(5, DEFAULT_ELECTION_TIMEOUT_MILLIS);
        String stoppedFollowerId = chooseFollowerId(leaderId);

        harness.stopNode(stoppedFollowerId);

        NodeEndpoint leaderEndpoint = requireEndpoint(leaderId);
        EtcdRpcResponse<PutResponse> put1 = EtcdTestSupport.callPutByRpc(harness.getTestClient(), leaderEndpoint, "k-catchup-1", "v1");
        EtcdRpcResponse<PutResponse> put2 = EtcdTestSupport.callPutByRpc(harness.getTestClient(), leaderEndpoint, "k-catchup-2", "v2");

        assertNotNull(put1);
        assertNotNull(put2);
        assertTrue(put1.getHeader().isSuccess());
        assertTrue(put2.getHeader().isSuccess());

        harness.awaitValueReplicated("k-catchup-1", "v1", 8000L);
        harness.awaitValueReplicated("k-catchup-2", "v2", 8000L);
    }
}
