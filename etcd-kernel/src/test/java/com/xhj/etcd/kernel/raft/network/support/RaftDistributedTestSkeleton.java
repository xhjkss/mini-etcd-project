package com.xhj.etcd.kernel.raft.network.support;

import com.xhj.etcd.kernel.raft.core.RaftProposeResult;
import com.xhj.etcd.kernel.testsupport.network.DistributedNetworkTestSkeleton;

import java.util.List;

/**
 * RaftDistributedTestSkeleton
 *
 * @author XJks
 * @description Raft 真实网络测试骨架，复用通用分布式场景驱动并补充 Raft 语义操作。
 */
public abstract class RaftDistributedTestSkeleton
        extends DistributedNetworkTestSkeleton<RaftClusterTestHarness> {

    protected static final long DEFAULT_ELECTION_TIMEOUT_MILLIS = 12000L;
    protected static final long DEFAULT_BOUNDARY_TIMEOUT_MILLIS = 15000L;

    @Override
    protected RaftClusterTestHarness createHarness() {
        return new RaftClusterTestHarness();
    }

    @Override
    protected long defaultBoundaryTimeoutMillis() {
        return 15000L;
    }

    protected String startClusterAndAwaitLeader(int nodeCount, long timeoutMillis) throws Exception {
        harness.startCluster(nodeCount);
        return harness.awaitLeaderElected(timeoutMillis);
    }

    protected void setSnapshotTriggerLogCount(int snapshotTriggerLogCount) {
        harness.setSnapshotTriggerLogCount(snapshotTriggerLogCount);
    }

    protected String chooseFollowerId(String leaderId) {
        return harness.chooseFollowerId(leaderId);
    }

    protected List<String> stopFollowers(String leaderId, int maxCount) {
        return harness.stopFollowers(leaderId, maxCount);
    }

    protected void proposeOnLeaderWithRetry(byte[] commandData, long timeoutMillis) throws Exception {
        harness.proposeOnLeader(commandData, timeoutMillis);
    }

    protected long proposeAndAwaitCommitOnQuorum(byte[] commandData, long timeoutMillis) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        Exception lastException = null;
        while (System.currentTimeMillis() < deadline) {
            long remainingMillis = deadline - System.currentTimeMillis();
            if (remainingMillis <= 0L) {
                break;
            }
            long proposeTimeoutMillis = Math.min(remainingMillis, 5000L);
            RaftProposeResult proposeResult = harness.proposeOnLeader(commandData, proposeTimeoutMillis);
            if (proposeResult == null || !proposeResult.isAccepted()) {
                continue;
            }
            try {
                long commitWaitMillis = Math.min(deadline - System.currentTimeMillis(), 6000L);
                if (commitWaitMillis <= 0L) {
                    break;
                }
                harness.awaitCommitIndexAtLeastOnQuorum(proposeResult.getLogIndex(), commitWaitMillis);
                return proposeResult.getLogIndex();
            } catch (Exception e) {
                lastException = e;
            }
        }
        throw new AssertionError("raft propose is not committed on quorum before timeout", lastException);
    }
}
