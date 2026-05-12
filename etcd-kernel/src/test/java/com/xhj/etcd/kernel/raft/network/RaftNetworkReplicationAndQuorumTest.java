package com.xhj.etcd.kernel.raft.network;

import com.xhj.etcd.kernel.raft.core.RaftProposeResult;
import com.xhj.etcd.kernel.raft.network.support.RaftDistributedTestSkeleton;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * RaftNetworkReplicationAndQuorumTest
 *
 * @author XJks
 * @description Raft 复制一致性与 quorum 边界测试，覆盖 basic agreement / fail agree / fail-no-agree / rejoin / concurrent / figure8-style。
 */
public class RaftNetworkReplicationAndQuorumTest extends RaftDistributedTestSkeleton {

    @Test
    public void shouldReplicateSequentialCommandsToAllRunningNodes() throws Exception {
        startClusterAndAwaitLeader(3, DEFAULT_ELECTION_TIMEOUT_MILLIS);

        long lastCommittedIndex = 0L;
        for (int index = 1; index <= 8; index++) {
            lastCommittedIndex = proposeAndAwaitCommitOnQuorum(("basic-agree-" + index).getBytes(StandardCharsets.UTF_8), 12000L);
            harness.awaitCommitIndexAtLeastOnAllRunningNodes(lastCommittedIndex, 12000L);
        }
        assertTrue(lastCommittedIndex > 0L);
    }

    @Test
    public void shouldKeepMajorityAgreementWhenOneFollowerIsolatedAndCatchUpAfterHeal() throws Exception {
        startClusterAndAwaitLeader(5, DEFAULT_ELECTION_TIMEOUT_MILLIS);
        long baselineCommitIndex = proposeAndAwaitCommitOnQuorum("fail-agree-baseline".getBytes(StandardCharsets.UTF_8), 12000L);
        harness.awaitCommitIndexAtLeastOnAllRunningNodes(baselineCommitIndex, 15000L);

        String leaderId = harness.awaitLeaderElected(12000L);
        String isolatedFollowerId = harness.chooseFollowerId(leaderId);
        List<String> rightNodeIds = new ArrayList<>();
        for (String nodeId : harness.getNodeIds()) {
            if (!isolatedFollowerId.equals(nodeId)) {
                rightNodeIds.add(nodeId);
            }
        }
        harness.isolateBidirectional(Collections.singletonList(isolatedFollowerId), rightNodeIds);

        long majorityCommitIndex = baselineCommitIndex;
        for (int index = 1; index <= 5; index++) {
            majorityCommitIndex = proposeAndAwaitCommitOnQuorum(("fail-agree-majority-" + index).getBytes(StandardCharsets.UTF_8), 12000L);
        }
        assertTrue(majorityCommitIndex > baselineCommitIndex);

        harness.healAllNetworkIsolation();
        harness.awaitLeaderElected(15000L);
        harness.awaitCommitIndexAtLeastOnAllRunningNodes(majorityCommitIndex, 25000L);

        long expectedCommitIndex = -1L;
        for (String nodeId : harness.getNodeIds()) {
            long currentCommitIndex = harness.getCommitIndex(nodeId);
            if (expectedCommitIndex < 0L) {
                expectedCommitIndex = currentCommitIndex;
                continue;
            }
            assertTrue("commit index should converge after heal, nodeId=" + nodeId,
                    expectedCommitIndex == currentCommitIndex);
        }
    }

    @Test
    public void shouldKeepMajorityProgressAndConvergeAfterPartitionHeal() throws Exception {
        String leaderId = startClusterAndAwaitLeader(5, DEFAULT_ELECTION_TIMEOUT_MILLIS);

        List<String> minorityNodeIds = harness.chooseFollowerIds(leaderId, 2);
        List<String> majorityNodeIds = new ArrayList<>();
        for (String nodeId : harness.getNodeIds()) {
            if (!minorityNodeIds.contains(nodeId)) {
                majorityNodeIds.add(nodeId);
            }
        }

        harness.isolateBidirectional(minorityNodeIds, majorityNodeIds);

        proposeOnLeaderWithRetry("partition-majority".getBytes(StandardCharsets.UTF_8), 15000L);
        String majorityLeaderId = harness.awaitLeaderElected(15000L);
        long majorityCommitIndex = harness.getCommitIndex(majorityLeaderId);
        harness.awaitCommitIndexAtLeastOnQuorum(majorityCommitIndex, 15000L);

        harness.healAllNetworkIsolation();
        harness.awaitLeaderElected(15000L);
        harness.awaitCommitIndexAtLeastOnAllRunningNodes(majorityCommitIndex, 20000L);

        long firstCommitIndex = harness.getCommitIndex(harness.getNodeIds().get(0));
        for (String nodeId : harness.getNodeIds()) {
            assertEquals("commit index diverged after heal, nodeId=" + nodeId,
                    firstCommitIndex,
                    harness.getCommitIndex(nodeId));
        }
    }

    @Test
    public void shouldNotAdvanceCommitIndexWithoutQuorumAndRecoverAfterFollowersRestart() throws Exception {
        startClusterAndAwaitLeader(5, DEFAULT_ELECTION_TIMEOUT_MILLIS);
        long baselineCommitIndex = proposeAndAwaitCommitOnQuorum("quorum-baseline".getBytes(StandardCharsets.UTF_8), 12000L);
        harness.awaitCommitIndexAtLeastOnAllRunningNodes(baselineCommitIndex, 12000L);

        String currentLeaderId = harness.awaitLeaderElected(12000L);
        assertNotNull(currentLeaderId);
        List<String> stoppedFollowerIds = stopFollowers(currentLeaderId, 3);
        assertEquals(3, stoppedFollowerIds.size());

        try {
            proposeOnLeaderWithRetry("quorum-no-majority".getBytes(StandardCharsets.UTF_8), 4000L);
        } catch (AssertionError ignore) {
            // 在无 quorum 窗口下，propose 可能直接超时，这是预期行为。
        }

        Thread.sleep(2 * 1000L);
        for (String nodeId : harness.getNodeIds()) {
            if (!harness.isNodeRunning(nodeId)) {
                continue;
            }
            assertEquals("commit index should not advance without quorum, nodeId=" + nodeId,
                    baselineCommitIndex,
                    harness.getCommitIndex(nodeId));
        }

        for (String nodeId : stoppedFollowerIds) {
            harness.restartNode(nodeId);
        }
        harness.awaitLeaderElected(20000L);
        Thread.sleep(1500L);

        long recoverCommittedIndex = proposeAndAwaitCommitOnQuorum(
                "quorum-recover-after-restart".getBytes(StandardCharsets.UTF_8),
                40000L);
        assertTrue("cluster should recover and continue committing after quorum is restored",
                recoverCommittedIndex >= baselineCommitIndex);
        harness.awaitCommitIndexAtLeastOnQuorum(recoverCommittedIndex, 40000L);
    }

    @Test
    public void shouldConvergeAfterPartitionedLeaderRejoinsMajority() throws Exception {
        String oldLeaderId = startClusterAndAwaitLeader(3, DEFAULT_ELECTION_TIMEOUT_MILLIS);
        long baselineCommitIndex = proposeAndAwaitCommitOnQuorum("rejoin-baseline".getBytes(StandardCharsets.UTF_8), 12000L);
        harness.awaitCommitIndexAtLeastOnAllRunningNodes(baselineCommitIndex, 12000L);

        List<String> otherNodeIds = new ArrayList<>();
        for (String nodeId : harness.getNodeIds()) {
            if (!oldLeaderId.equals(nodeId)) {
                otherNodeIds.add(nodeId);
            }
        }
        harness.isolateBidirectional(Collections.singletonList(oldLeaderId), otherNodeIds);

        String majorityLeaderId = harness.awaitLeaderElectedExcluding(oldLeaderId, 15000L);
        assertNotNull(majorityLeaderId);
        long majorityCommitIndex = proposeAndAwaitCommitOnQuorum("rejoin-majority-progress".getBytes(StandardCharsets.UTF_8), 12000L);

        harness.healAllNetworkIsolation();
        harness.awaitLeaderElected(15000L);
        harness.awaitCommitIndexAtLeastOnAllRunningNodes(majorityCommitIndex, 20000L);
    }

    @Test
    public void shouldKeepProgressUnderConcurrentProposals() throws Exception {
        startClusterAndAwaitLeader(3, DEFAULT_ELECTION_TIMEOUT_MILLIS);
        long baselineCommitIndex = proposeAndAwaitCommitOnQuorum("concurrent-baseline".getBytes(StandardCharsets.UTF_8), 12000L);

        int concurrentCount = 16;
        CountDownLatch doneLatch = new CountDownLatch(concurrentCount);
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicLong maxAcceptedLogIndex = new AtomicLong(baselineCommitIndex);
        List<Thread> threads = new ArrayList<>();
        for (int index = 0; index < concurrentCount; index++) {
            final int commandIndex = index;
            Thread thread = new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        RaftProposeResult proposeResult =
                                harness.proposeOnLeader(("concurrent-" + commandIndex).getBytes(StandardCharsets.UTF_8), 12000L);
                        if (proposeResult != null && proposeResult.isAccepted()) {
                            successCount.incrementAndGet();
                            maxAcceptedLogIndex.accumulateAndGet(proposeResult.getLogIndex(), Math::max);
                        }
                    } catch (Exception ignore) {
                        // 并发期间如果发生短暂领导切换，允许单个提案失败，最终只检查整体进展。
                    } finally {
                        doneLatch.countDown();
                    }
                }
            }, "raft-concurrent-propose-" + index);
            thread.start();
            threads.add(thread);
        }

        doneLatch.await(40, TimeUnit.SECONDS);
        for (Thread thread : threads) {
            thread.join(1000L);
        }

        assertTrue("at least half proposals should succeed under stable network", successCount.get() >= concurrentCount / 2);

        long expectedCommitIndex = maxAcceptedLogIndex.get();
        harness.awaitCommitIndexAtLeastOnAllRunningNodes(expectedCommitIndex, 30000L);
        assertTrue("commit index should advance after concurrent proposals", expectedCommitIndex > baselineCommitIndex);
    }

    @Test
    public void shouldKeepQuorumProgressUnderRepeatedCrashRestartWindows() throws Exception {
        long seed = 2026051202L;
        Random random = new Random(seed);
        StringBuilder operationTrace = new StringBuilder();
        operationTrace.append("seed=").append(seed).append('\n');

        startClusterAndAwaitLeader(5, DEFAULT_ELECTION_TIMEOUT_MILLIS);

        long maxCommittedIndex = 0L;
        for (int step = 1; step <= 40; step++) {
            if (step % 5 == 0) {
                FaultInjectionType injectionType = random.nextBoolean()
                        ? FaultInjectionType.RESTART_FOLLOWER
                        : FaultInjectionType.RESTART_LEADER;
                injectFault(injectionType, operationTrace);
            }

            long committedIndex = proposeAndAwaitCommitOnQuorum(
                    ("figure8-style-step-" + step).getBytes(StandardCharsets.UTF_8),
                    15000L);
            if (committedIndex > maxCommittedIndex) {
                maxCommittedIndex = committedIndex;
            }
            operationTrace.append("step=").append(step)
                    .append(", committedIndex=").append(committedIndex)
                    .append('\n');
        }

        harness.awaitLeaderElected(20000L);
        harness.awaitCommitIndexAtLeastOnAllRunningNodes(maxCommittedIndex, 30000L);
        assertTrue("cluster should make progress under restart windows\n" + operationTrace,
                maxCommittedIndex > 0L);
    }
}
