package com.xhj.etcd.kernel.raft.network;

import com.xhj.etcd.kernel.raft.network.support.RaftDistributedTestSkeleton;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.Random;

import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;

/**
 * RaftNetworkElectionAndFailoverTest
 *
 * @author XJks
 * @description Raft 选举与 Leader 故障切换场景测试，覆盖 initial election / many elections / leader failover。
 */
public class RaftNetworkElectionAndFailoverTest extends RaftDistributedTestSkeleton {

    @Test
    public void shouldElectLeaderInitiallyAndRemainElectableAfterRepeatedNodeRestarts() throws Exception {
        String leaderId = startClusterAndAwaitLeader(5, DEFAULT_ELECTION_TIMEOUT_MILLIS);
        assertNotNull(leaderId);

        Random random = new Random(2026051201L);
        for (int round = 1; round <= 8; round++) {
            String currentLeaderId = harness.awaitLeaderElected(15000L);
            String followerId = harness.chooseFollowerId(currentLeaderId);
            if (random.nextBoolean()) {
                harness.stopNode(followerId);
                Thread.sleep(300L);
                harness.restartNode(followerId);
            } else {
                harness.stopNode(currentLeaderId);
                Thread.sleep(300L);
                harness.restartNode(currentLeaderId);
            }
            String newLeaderId = harness.awaitLeaderElected(15000L);
            assertNotNull("leader should exist after restart round=" + round, newLeaderId);
        }
    }

    @Test
    public void shouldElectNewLeaderAfterLeaderStopsAndRecoverAfterRestart() throws Exception {
        String oldLeaderId = startClusterAndAwaitLeader(5, DEFAULT_ELECTION_TIMEOUT_MILLIS);
        assertNotNull(oldLeaderId);

        harness.stopNode(oldLeaderId);
        String majorityLeaderId = harness.awaitLeaderElectedExcluding(oldLeaderId, 15000L);
        assertNotNull(majorityLeaderId);

        long majorityCommitIndex = proposeAndAwaitCommitOnQuorum("election-stop-majority".getBytes(StandardCharsets.UTF_8), 12000L);

        harness.restartNode(oldLeaderId);
        harness.awaitLeaderElected(15000L);
        harness.awaitCommitIndexAtLeastOnAllRunningNodes(majorityCommitIndex, 20000L);
    }

    @Test
    public void shouldElectNewLeaderAndCommitAfterOldLeaderStops() throws Exception {
        String oldLeaderId = startClusterAndAwaitLeader(5, DEFAULT_ELECTION_TIMEOUT_MILLIS);
        assertNotNull(oldLeaderId);

        proposeOnLeaderWithRetry("failover-before".getBytes(StandardCharsets.UTF_8), 12000L);
        long beforeStopCommitIndex = harness.getCommitIndex(oldLeaderId);
        harness.awaitCommitIndexAtLeastOnQuorum(beforeStopCommitIndex, 12000L);

        harness.stopNode(oldLeaderId);

        String newLeaderId = harness.awaitLeaderElectedExcluding(oldLeaderId, 15000L);
        assertNotNull(newLeaderId);
        assertNotEquals(oldLeaderId, newLeaderId);

        proposeOnLeaderWithRetry("failover-after".getBytes(StandardCharsets.UTF_8), 12000L);
        long afterStopCommitIndex = harness.getCommitIndex(newLeaderId);
        harness.awaitCommitIndexAtLeastOnQuorum(afterStopCommitIndex, 12000L);
    }
}
