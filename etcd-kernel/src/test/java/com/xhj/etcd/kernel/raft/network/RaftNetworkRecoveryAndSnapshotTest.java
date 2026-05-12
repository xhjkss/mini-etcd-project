package com.xhj.etcd.kernel.raft.network;

import com.xhj.etcd.kernel.raft.network.support.RaftDistributedTestSkeleton;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * RaftNetworkRecoveryAndSnapshotTest
 *
 * @author XJks
 * @description Raft 持久化恢复与快照追平测试，覆盖 6.824 2C Persist 场景与 lagging follower snapshot catch-up 场景。
 */
public class RaftNetworkRecoveryAndSnapshotTest extends RaftDistributedTestSkeleton {

    @Test
    public void shouldRecoverCommittedStateAfterFullClusterCrashRestart() throws Exception {
        startClusterAndAwaitLeader(3, DEFAULT_ELECTION_TIMEOUT_MILLIS);

        long committedIndexBeforeCrash = 0L;
        for (int index = 1; index <= 6; index++) {
            committedIndexBeforeCrash = proposeAndAwaitCommitOnQuorum(
                    ("persist1-before-crash-" + index).getBytes(StandardCharsets.UTF_8),
                    12000L);
            harness.awaitCommitIndexAtLeastOnAllRunningNodes(committedIndexBeforeCrash, 15000L);
        }

        List<String> nodeIds = new ArrayList<>(harness.getNodeIds());
        crashAndRestartNodes(nodeIds, 1200L);

        String recoveredLeaderId = harness.awaitLeaderElected(20000L);
        assertNotNull(recoveredLeaderId);
        for (String nodeId : harness.getNodeIds()) {
            assertNotNull("persistent state should survive full crash/restart, nodeId=" + nodeId,
                    harness.getPersistentState(nodeId));
        }

        long committedIndexAfterRestart = proposeAndAwaitCommitOnQuorum(
                "persist1-after-restart".getBytes(StandardCharsets.UTF_8),
                12000L);
        harness.awaitCommitIndexAtLeastOnAllRunningNodes(committedIndexAfterRestart, 20000L);
        assertTrue(committedIndexAfterRestart > committedIndexBeforeCrash);
    }

    @Test
    public void shouldKeepMakingProgressAcrossRepeatedFollowerCrashRestart() throws Exception {
        String leaderId = startClusterAndAwaitLeader(5, DEFAULT_ELECTION_TIMEOUT_MILLIS);
        assertNotNull(leaderId);

        long committedIndex = proposeAndAwaitCommitOnQuorum(
                "persist2-baseline".getBytes(StandardCharsets.UTF_8),
                12000L);
        harness.awaitCommitIndexAtLeastOnAllRunningNodes(committedIndex, 15000L);

        for (int round = 1; round <= 6; round++) {
            String currentLeaderId = harness.awaitLeaderElected(15000L);
            List<String> restartFollowerIds = harness.chooseFollowerIds(currentLeaderId, 2);
            for (String followerId : restartFollowerIds) {
                crashAndRestartNodes(java.util.Collections.singletonList(followerId), 300L);
            }
            harness.awaitLeaderElected(15000L);

            committedIndex = proposeAndAwaitCommitOnQuorum(
                    ("persist2-round-" + round).getBytes(StandardCharsets.UTF_8),
                    12000L);
            harness.awaitCommitIndexAtLeastOnAllRunningNodes(committedIndex, 20000L);
        }
    }

    @Test
    public void shouldConvergeAfterLaggingFollowerAndLeaderCrashRestartSequence() throws Exception {
        String oldLeaderId = startClusterAndAwaitLeader(3, DEFAULT_ELECTION_TIMEOUT_MILLIS);
        assertNotNull(oldLeaderId);

        long baselineCommitIndex = proposeAndAwaitCommitOnQuorum(
                "persist3-baseline".getBytes(StandardCharsets.UTF_8),
                12000L);
        harness.awaitCommitIndexAtLeastOnAllRunningNodes(baselineCommitIndex, 15000L);

        String laggingFollowerId = harness.chooseFollowerId(oldLeaderId);
        harness.stopNode(laggingFollowerId);

        long majorityOnlyCommitIndex = baselineCommitIndex;
        for (int index = 1; index <= 5; index++) {
            majorityOnlyCommitIndex = proposeAndAwaitCommitOnQuorum(
                    ("persist3-majority-only-" + index).getBytes(StandardCharsets.UTF_8),
                    12000L);
        }
        harness.awaitCommitIndexAtLeastOnQuorum(majorityOnlyCommitIndex, 15000L);

        harness.restartNode(laggingFollowerId);
        harness.awaitLeaderElected(20000L);
        harness.awaitCommitIndexAtLeastOnAllRunningNodes(majorityOnlyCommitIndex, 25000L);

        harness.stopNode(oldLeaderId);
        harness.awaitLeaderElectedExcluding(oldLeaderId, 20000L);

        long commitIndexAfterElection = proposeAndAwaitCommitOnQuorum(
                "persist3-after-recovery-election".getBytes(StandardCharsets.UTF_8),
                15000L);
        harness.awaitCommitIndexAtLeastOnQuorum(commitIndexAfterElection, 15000L);

        harness.restartNode(oldLeaderId);
        harness.awaitLeaderElected(20000L);
        harness.awaitCommitIndexAtLeastOnAllRunningNodes(commitIndexAfterElection, 25000L);
    }

    @Test
    public void shouldCatchUpLaggingFollowerBySnapshotAfterRestart() throws Exception {
        setSnapshotTriggerLogCount(2);
        String leaderId = startClusterAndAwaitLeader(3, DEFAULT_ELECTION_TIMEOUT_MILLIS);
        String laggingFollowerId = chooseFollowerId(leaderId);

        harness.stopNode(laggingFollowerId);

        for (int index = 1; index <= 14; index++) {
            proposeOnLeaderWithRetry(("snapshot-catchup-" + index).getBytes(StandardCharsets.UTF_8), 12000L);
        }

        String activeLeaderId = harness.awaitLeaderElected(15000L);
        long leaderCommitIndex = harness.getCommitIndex(activeLeaderId);
        harness.awaitCommitIndexAtLeastOnQuorum(leaderCommitIndex, 15000L);
        harness.awaitPersistedSnapshotOnNode(activeLeaderId, 15000L);
        assertTrue(harness.hasPersistedSnapshot(activeLeaderId));

        harness.restartNode(laggingFollowerId);
        harness.awaitPersistedSnapshotOnNode(laggingFollowerId, 20000L);
        assertTrue(harness.hasPersistedSnapshot(laggingFollowerId));
        harness.awaitCommitIndexAtLeastOnAllRunningNodes(leaderCommitIndex, 25000L);
    }

    private void crashAndRestartNodes(List<String> nodeIds, long crashWindowMillis) throws Exception {
        for (String nodeId : nodeIds) {
            harness.stopNode(nodeId);
        }
        Thread.sleep(crashWindowMillis);
        for (String nodeId : nodeIds) {
            harness.restartNode(nodeId);
        }
    }
}
