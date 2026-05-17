package com.xhj.etcd.kernel.etcd.network.consistency;

import com.xhj.etcd.kernel.etcd.etcdrpc.KvStateHashRequest;
import com.xhj.etcd.kernel.etcd.etcdrpc.KvStateHashResponse;
import com.xhj.etcd.kernel.etcd.etcdrpc.CompactRequest;
import com.xhj.etcd.kernel.etcd.etcdrpc.CompactResponse;
import com.xhj.etcd.kernel.etcd.etcdrpc.EtcdRpcResponse;
import com.xhj.etcd.kernel.etcd.etcdrpc.NodeStatusRequest;
import com.xhj.etcd.kernel.etcd.etcdrpc.NodeStatusResponse;
import com.xhj.etcd.kernel.etcd.network.support.EtcdDistributedTestSkeleton;
import com.xhj.etcd.kernel.etcd.network.support.EtcdTestSupport;
import com.xhj.etcd.rpc.NodeEndpoint;
import org.junit.Test;

import java.util.concurrent.Callable;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * EtcdDiagnosticNetworkConsistencyTest
 *
 * @author XJks
 * @description KvStateHash 和 Status 的多节点网络一致性测试。
 */
public class EtcdDiagnosticNetworkConsistencyTest extends EtcdDistributedTestSkeleton {

    @Test
    public void shouldReturnSameHashAcrossLeaderAndFollowerForSameCommittedState() throws Exception {
        String leaderId = startClusterAndAwaitLeader(3, DEFAULT_BOUNDARY_TIMEOUT_MILLIS);
        String followerId = chooseFollowerId(leaderId);

        NodeEndpoint leaderEndpoint = requireEndpoint(leaderId);
        NodeEndpoint followerEndpoint = requireEndpoint(followerId);

        putOnLeaderWithRetry("diag/network/hash/a", "v1", DEFAULT_BOUNDARY_TIMEOUT_MILLIS);
        putOnLeaderWithRetry("diag/network/hash/b", "v2", DEFAULT_BOUNDARY_TIMEOUT_MILLIS);
        harness.awaitValueReplicated("diag/network/hash/a", "v1", harness.quorumSize(), DEFAULT_BOUNDARY_TIMEOUT_MILLIS);
        harness.awaitValueReplicated("diag/network/hash/b", "v2", harness.quorumSize(), DEFAULT_BOUNDARY_TIMEOUT_MILLIS);

        KvStateHashResponse leaderHashResponse = assertKvStateHashSuccess(leaderEndpoint);
        KvStateHashResponse followerHashResponse = assertKvStateHashSuccess(followerEndpoint);

        assertEquals(leaderHashResponse.getHash(), followerHashResponse.getHash());
        assertEquals(leaderHashResponse.getRevision(), followerHashResponse.getRevision());
        assertEquals(leaderHashResponse.getKeyCount(), followerHashResponse.getKeyCount());
        assertEquals(2, leaderHashResponse.getKeyCount());

        NodeStatusResponse leaderNodeStatusResponse = assertNodeStatusSuccess(leaderEndpoint);
        NodeStatusResponse followerNodeStatusResponse = assertNodeStatusSuccess(followerEndpoint);

        assertEquals(leaderId, leaderNodeStatusResponse.getNodeId());
        assertEquals(followerId, followerNodeStatusResponse.getNodeId());
        assertEquals(leaderNodeStatusResponse.getCurrentRevision(), followerNodeStatusResponse.getCurrentRevision());
        assertEquals(leaderNodeStatusResponse.getCompactRevision(), followerNodeStatusResponse.getCompactRevision());
        assertEquals(leaderNodeStatusResponse.getKeyCount(), followerNodeStatusResponse.getKeyCount());
        assertEquals(leaderNodeStatusResponse.getLeaseCount(), followerNodeStatusResponse.getLeaseCount());
        assertEquals(leaderNodeStatusResponse.getWatchCount(), followerNodeStatusResponse.getWatchCount());
        assertTrue(leaderNodeStatusResponse.getCurrentRevision() >= 2L);
        assertTrue(leaderNodeStatusResponse.getRole() != null);
    }

    @Test
    public void shouldRejectCompactedHashRevisionOnLeaderAndFollower() throws Exception {
        String leaderId = startClusterAndAwaitLeader(3, DEFAULT_BOUNDARY_TIMEOUT_MILLIS);
        String followerId = chooseFollowerId(leaderId);
        NodeEndpoint leaderEndpoint = requireEndpoint(leaderId);
        NodeEndpoint followerEndpoint = requireEndpoint(followerId);

        putOnLeaderWithRetry("diag/network/compact/hash", "v1", DEFAULT_BOUNDARY_TIMEOUT_MILLIS);
        putOnLeaderWithRetry("diag/network/compact/hash", "v2", DEFAULT_BOUNDARY_TIMEOUT_MILLIS);
        harness.awaitValueReplicated("diag/network/compact/hash", "v2", harness.quorumSize(), DEFAULT_BOUNDARY_TIMEOUT_MILLIS);

        NodeStatusResponse nodeStatusBeforeCompact = assertNodeStatusSuccess(leaderEndpoint);
        long currentRevisionBeforeCompact = nodeStatusBeforeCompact.getCurrentRevision();

        CompactRequest compactRequest = new CompactRequest();
        compactRequest.setRevision(currentRevisionBeforeCompact);
        CompactResponse compactResponse = compactOnLeaderWithRetry(compactRequest, DEFAULT_BOUNDARY_TIMEOUT_MILLIS);
        assertNotNull(compactResponse);
        assertEquals(currentRevisionBeforeCompact, compactResponse.getCompactRevision());
        awaitCompactRevisionApplied(followerEndpoint, currentRevisionBeforeCompact, DEFAULT_BOUNDARY_TIMEOUT_MILLIS);

        long compactedRevision = currentRevisionBeforeCompact - 1L;
        if (compactedRevision <= 0L) {
            compactedRevision = 1L;
        }

        EtcdRpcResponse<KvStateHashResponse> leaderCompactedHashResponse = EtcdTestSupport.callKvStateHashByRpc(
                harness.getTestClient(),
                leaderEndpoint,
                new KvStateHashRequest(compactedRevision));
        assertNotNull(leaderCompactedHashResponse);
        assertNotNull(leaderCompactedHashResponse.getHeader());
        assertFalse(leaderCompactedHashResponse.getHeader().isSuccess());
        assertTrue(leaderCompactedHashResponse.getHeader().getMessage().contains("compacted"));

        EtcdRpcResponse<KvStateHashResponse> followerCompactedHashResponse = EtcdTestSupport.callKvStateHashByRpc(
                harness.getTestClient(),
                followerEndpoint,
                new KvStateHashRequest(compactedRevision));
        assertNotNull(followerCompactedHashResponse);
        assertNotNull(followerCompactedHashResponse.getHeader());
        assertFalse(followerCompactedHashResponse.getHeader().isSuccess());
        assertTrue(followerCompactedHashResponse.getHeader().getMessage().contains("compacted"));
    }

    @Test
    public void shouldKeepDiagnosticStatusAndHashConsistentAfterCompactThenNewWrite() throws Exception {
        String leaderId = startClusterAndAwaitLeader(3, DEFAULT_BOUNDARY_TIMEOUT_MILLIS);
        String followerId = chooseFollowerId(leaderId);
        NodeEndpoint leaderEndpoint = requireEndpoint(leaderId);
        NodeEndpoint followerEndpoint = requireEndpoint(followerId);

        putOnLeaderWithRetry("diag/network/compact/write/a", "v1", DEFAULT_BOUNDARY_TIMEOUT_MILLIS);
        putOnLeaderWithRetry("diag/network/compact/write/b", "v2", DEFAULT_BOUNDARY_TIMEOUT_MILLIS);
        NodeStatusResponse nodeStatusBeforeCompact = assertNodeStatusSuccess(leaderEndpoint);

        CompactRequest compactRequest = new CompactRequest();
        compactRequest.setRevision(nodeStatusBeforeCompact.getCurrentRevision());
        CompactResponse compactResponse = compactOnLeaderWithRetry(compactRequest, DEFAULT_BOUNDARY_TIMEOUT_MILLIS);
        assertNotNull(compactResponse);
        awaitCompactRevisionApplied(followerEndpoint, compactResponse.getCompactRevision(), DEFAULT_BOUNDARY_TIMEOUT_MILLIS);

        putOnLeaderWithRetry("diag/network/compact/write/c", "v3", DEFAULT_BOUNDARY_TIMEOUT_MILLIS);
        harness.awaitValueReplicated("diag/network/compact/write/c", "v3", harness.quorumSize(), DEFAULT_BOUNDARY_TIMEOUT_MILLIS);

        NodeStatusResponse leaderNodeStatusAfterWrite = assertNodeStatusSuccess(leaderEndpoint);
        NodeStatusResponse followerNodeStatusAfterWrite = assertNodeStatusSuccess(followerEndpoint);
        KvStateHashResponse leaderHashAfterWrite = assertKvStateHashSuccess(leaderEndpoint);
        KvStateHashResponse followerHashAfterWrite = assertKvStateHashSuccess(followerEndpoint);

        assertEquals(compactResponse.getCompactRevision(), leaderNodeStatusAfterWrite.getCompactRevision());
        assertEquals(compactResponse.getCompactRevision(), followerNodeStatusAfterWrite.getCompactRevision());
        assertEquals(leaderNodeStatusAfterWrite.getCurrentRevision(), followerNodeStatusAfterWrite.getCurrentRevision());
        assertEquals(leaderHashAfterWrite.getHash(), followerHashAfterWrite.getHash());
        assertTrue(leaderNodeStatusAfterWrite.getCurrentRevision() > compactResponse.getCompactRevision());
    }

    @Test
    public void shouldKeepStatusConsistentAfterFollowerRestart() throws Exception {
        String leaderId = startClusterAndAwaitLeader(3, DEFAULT_BOUNDARY_TIMEOUT_MILLIS);
        String followerId = chooseFollowerId(leaderId);
        NodeEndpoint leaderEndpoint = requireEndpoint(leaderId);
        NodeEndpoint followerEndpoint = requireEndpoint(followerId);

        putOnLeaderWithRetry("diag/network/restart/a", "v1", DEFAULT_BOUNDARY_TIMEOUT_MILLIS);
        putOnLeaderWithRetry("diag/network/restart/b", "v2", DEFAULT_BOUNDARY_TIMEOUT_MILLIS);
        harness.awaitValueReplicated("diag/network/restart/a", "v1", harness.quorumSize(), DEFAULT_BOUNDARY_TIMEOUT_MILLIS);
        harness.awaitValueReplicated("diag/network/restart/b", "v2", harness.quorumSize(), DEFAULT_BOUNDARY_TIMEOUT_MILLIS);

        NodeStatusResponse leaderNodeStatusBeforeRestart = assertNodeStatusSuccess(leaderEndpoint);
        harness.restartNode(followerId);
        harness.awaitValueReplicated("diag/network/restart/a", "v1", harness.quorumSize(), DEFAULT_BOUNDARY_TIMEOUT_MILLIS);
        harness.awaitValueReplicated("diag/network/restart/b", "v2", harness.quorumSize(), DEFAULT_BOUNDARY_TIMEOUT_MILLIS);

        NodeStatusResponse followerNodeStatusAfterRestart = assertNodeStatusSuccess(followerEndpoint);
        NodeStatusResponse leaderNodeStatusAfterRestart = assertNodeStatusSuccess(leaderEndpoint);
        KvStateHashResponse leaderHashResponse = assertKvStateHashSuccess(leaderEndpoint);
        KvStateHashResponse followerHashResponse = assertKvStateHashSuccess(followerEndpoint);

        assertEquals(leaderNodeStatusBeforeRestart.getCurrentRevision(), followerNodeStatusAfterRestart.getCurrentRevision());
        assertEquals(leaderNodeStatusAfterRestart.getCurrentRevision(), followerNodeStatusAfterRestart.getCurrentRevision());
        assertEquals(leaderNodeStatusAfterRestart.getCompactRevision(), followerNodeStatusAfterRestart.getCompactRevision());
        assertEquals(leaderHashResponse.getHash(), followerHashResponse.getHash());
        assertEquals(leaderHashResponse.getRevision(), followerHashResponse.getRevision());
    }

    @Test
    public void shouldServeDiagnosticLocalReadsWithoutQuorum() throws Exception {
        String leaderId = startClusterAndAwaitLeader(3, DEFAULT_BOUNDARY_TIMEOUT_MILLIS);
        String remainingNodeId = chooseFollowerId(leaderId);
        String stoppedFollowerId = null;
        for (String nodeId : harness.getNodeIds()) {
            if (!leaderId.equals(nodeId) && !remainingNodeId.equals(nodeId)) {
                stoppedFollowerId = nodeId;
                break;
            }
        }
        if (stoppedFollowerId == null) {
            throw new AssertionError("stopped follower node not found");
        }

        NodeEndpoint remainingEndpoint = requireEndpoint(remainingNodeId);

        putOnLeaderWithRetry("diag/network/no-quorum/hash", "v1", DEFAULT_BOUNDARY_TIMEOUT_MILLIS);
        harness.awaitValueReplicated("diag/network/no-quorum/hash", "v1", harness.quorumSize(), DEFAULT_BOUNDARY_TIMEOUT_MILLIS);

        harness.stopNode(leaderId);
        harness.stopNode(stoppedFollowerId);

        KvStateHashResponse kvStateHashResponse = assertKvStateHashSuccess(remainingEndpoint);
        NodeStatusResponse nodeStatusResponse = assertNodeStatusSuccess(remainingEndpoint);

        assertTrue(kvStateHashResponse.getRevision() >= 1L);
        assertTrue(kvStateHashResponse.getKeyCount() >= 1);
        assertEquals(remainingNodeId, nodeStatusResponse.getNodeId());
        assertTrue(nodeStatusResponse.getCurrentRevision() >= 1L);
    }

    @Test
    public void shouldExposeSnapshotBoundaryThroughNodeStatus() throws Exception {
        setSnapshotTriggerLogCount(5);
        String leaderId = startClusterAndAwaitLeader(3, DEFAULT_BOUNDARY_TIMEOUT_MILLIS);
        NodeEndpoint leaderEndpoint = requireEndpoint(leaderId);

        for (int index = 1; index <= 12; index++) {
            putOnLeaderWithRetry("diag/network/snapshot/key-" + index, "v-" + index, DEFAULT_BOUNDARY_TIMEOUT_MILLIS);
        }

        harness.awaitPersistedSnapshotOnNode(leaderId, DEFAULT_BOUNDARY_TIMEOUT_MILLIS);
        NodeStatusResponse nodeStatusResponse = assertNodeStatusSuccess(leaderEndpoint);
        assertTrue(nodeStatusResponse.getSnapshotLastIncludedIndex() > 0L);
        assertTrue(nodeStatusResponse.getSnapshotLastIncludedTerm() > 0L);
    }

    private KvStateHashResponse assertKvStateHashSuccess(NodeEndpoint endpoint) throws Exception {
        EtcdRpcResponse<KvStateHashResponse> response = EtcdTestSupport.callKvStateHashByRpc(
                harness.getTestClient(),
                endpoint,
                new KvStateHashRequest(0L));
        assertNotNull(response);
        assertNotNull(response.getHeader());
        assertTrue(response.getHeader().isSuccess());
        assertNotNull(response.getBody());
        return response.getBody();
    }

    private NodeStatusResponse assertNodeStatusSuccess(NodeEndpoint endpoint) throws Exception {
        EtcdRpcResponse<NodeStatusResponse> response = EtcdTestSupport.callNodeStatusByRpc(
                harness.getTestClient(),
                endpoint,
                new NodeStatusRequest());
        assertNotNull(response);
        assertNotNull(response.getHeader());
        assertTrue(response.getHeader().isSuccess());
        assertNotNull(response.getBody());
        return response.getBody();
    }

    private void awaitCompactRevisionApplied(final NodeEndpoint endpoint, final long expectedCompactRevision, long timeoutMillis) throws Exception {
        EtcdTestSupport.awaitTrue(new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                NodeStatusResponse nodeStatusResponse = assertNodeStatusSuccess(endpoint);
                return nodeStatusResponse.getCompactRevision() >= expectedCompactRevision;
            }
        }, timeoutMillis, "compact revision is not applied on node, endpoint=" + endpoint.endpointKey());
    }
}
