package com.xhj.etcd.kernel.testsupport.network;

import org.junit.After;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * DistributedNetworkTestSkeleton
 *
 * @author XJks
 * @description Raft / Etcd 真实网络测试统一骨架：统一 harness 生命周期、故障注入、随机场景、分区窗口和并发驱动。
 */
public abstract class DistributedNetworkTestSkeleton<H extends DistributedClusterHarness> {

    /**
     * 分布式故障注入类型。
     */
    protected enum FaultInjectionType {
        RESTART_FOLLOWER,
        RESTART_LEADER,
        FULL_CLUSTER_CRASH_RESTART,
        ISOLATE_LEADER_FROM_MAJORITY_AND_HEAL
    }

    /**
     * 随机场景单步执行器。
     */
    protected interface RandomScenarioStepExecutor {
        void executeStep(int step, Random random, StringBuilder operationTrace) throws Exception;
    }

    /**
     * 并发 worker 单步执行器。
     */
    protected interface ConcurrentWorkerStepExecutor {
        void executeStep(int workerIndex, int operationIndex, Random random, StringBuilder operationTrace) throws Exception;
    }

    /**
     * 随机分区拓扑构建器。
     */
    protected interface RandomPartitionTopologyBuilder {
        PartitionWindowTopology build(Random random, String leaderId, List<String> sortedNodeIds);
    }

    /**
     * 分区窗口拓扑。
     */
    protected static class PartitionWindowTopology {
        private final List<String> leftNodeIds;
        private final List<String> rightNodeIds;
        private final String topologyLabel;

        private PartitionWindowTopology(List<String> leftNodeIds, List<String> rightNodeIds, String topologyLabel) {
            this.leftNodeIds = leftNodeIds;
            this.rightNodeIds = rightNodeIds;
            this.topologyLabel = topologyLabel;
        }

        public static PartitionWindowTopology of(List<String> leftNodeIds, List<String> rightNodeIds, String topologyLabel) {
            return new PartitionWindowTopology(leftNodeIds, rightNodeIds, topologyLabel);
        }
    }

    protected H harness;

    @Before
    public void setUpDistributedHarness() {
        harness = createHarness();
    }

    @After
    public void tearDownDistributedHarness() {
        if (harness != null) {
            harness.stopAll();
        }
    }

    /**
     * 创建具体领域的测试 harness。
     */
    protected abstract H createHarness();

    /**
     * 单节点重启时的等待时间。
     */
    protected long singleNodeRestartSleepMillis() {
        return 150L;
    }

    /**
     * 全集群崩溃恢复时的等待时间。
     */
    protected long fullClusterRestartSleepMillis() {
        return 1200L;
    }

    /**
     * 故障注入默认边界超时。
     */
    protected long defaultBoundaryTimeoutMillis() {
        return 12000L;
    }

    /**
     * 新 leader 选举等待超时。
     */
    protected long newLeaderElectionTimeoutMillis() {
        return 15000L;
    }

    /**
     * 运行随机操作序列。
     */
    protected void runRandomScenario(long seed,
                                     int totalSteps,
                                     int injectEverySteps,
                                     FaultInjectionType periodicFaultInjectionType,
                                     RandomScenarioStepExecutor stepExecutor,
                                     StringBuilder operationTrace) throws Exception {
        if (totalSteps <= 0) {
            throw new IllegalArgumentException("totalSteps must be > 0");
        }
        if (stepExecutor == null) {
            throw new IllegalArgumentException("stepExecutor must not be null");
        }
        Random random = new Random(seed);
        operationTrace.append("seed=").append(seed).append('\n');
        for (int step = 1; step <= totalSteps; step++) {
            if (periodicFaultInjectionType != null && injectEverySteps > 0 && step % injectEverySteps == 0) {
                injectFault(periodicFaultInjectionType, operationTrace);
            }
            stepExecutor.executeStep(step, random, operationTrace);
        }
    }

    /**
     * 运行“随机分区拓扑 + 恢复窗口”场景。
     */
    protected void runRandomPartitionWindowScenario(long seed,
                                                    int totalSteps,
                                                    int injectEverySteps,
                                                    int partitionWindowSteps,
                                                    RandomPartitionTopologyBuilder topologyBuilder,
                                                    RandomScenarioStepExecutor stepExecutor,
                                                    StringBuilder operationTrace) throws Exception {
        if (totalSteps <= 0) {
            throw new IllegalArgumentException("totalSteps must be > 0");
        }
        if (stepExecutor == null) {
            throw new IllegalArgumentException("stepExecutor must not be null");
        }
        if (injectEverySteps <= 0) {
            throw new IllegalArgumentException("injectEverySteps must be > 0");
        }
        if (partitionWindowSteps <= 0) {
            throw new IllegalArgumentException("partitionWindowSteps must be > 0");
        }

        Random random = new Random(seed);
        RandomPartitionTopologyBuilder effectiveTopologyBuilder =
                topologyBuilder == null ? new RandomPartitionTopologyBuilder() {
                    @Override
                    public PartitionWindowTopology build(Random random, String leaderId, List<String> sortedNodeIds) {
                        return buildRandomMinorityPartitionTopology(random, leaderId, sortedNodeIds);
                    }
                } : topologyBuilder;

        operationTrace.append("seed=").append(seed).append('\n');
        boolean partitionActive = false;
        int remainingWindowSteps = 0;
        try {
            for (int step = 1; step <= totalSteps; step++) {
                if (!partitionActive && step % injectEverySteps == 0) {
                    String leaderId = harness.awaitLeaderElected(12000L);
                    List<String> sortedNodeIds = harness.getNodeIds();
                    PartitionWindowTopology topology = effectiveTopologyBuilder.build(random, leaderId, sortedNodeIds);
                    if (topology != null
                            && topology.leftNodeIds != null
                            && topology.rightNodeIds != null
                            && !topology.leftNodeIds.isEmpty()
                            && !topology.rightNodeIds.isEmpty()) {
                        harness.isolateBidirectional(topology.leftNodeIds, topology.rightNodeIds);
                        partitionActive = true;
                        remainingWindowSteps = partitionWindowSteps;
                        operationTrace.append("inject=partition-open, step=").append(step)
                                .append(", topology=").append(topology.topologyLabel)
                                .append(", left=").append(topology.leftNodeIds)
                                .append(", right=").append(topology.rightNodeIds)
                                .append('\n');
                    }
                }

                stepExecutor.executeStep(step, random, operationTrace);

                if (partitionActive) {
                    remainingWindowSteps--;
                    if (remainingWindowSteps <= 0) {
                        harness.healAllNetworkIsolation();
                        partitionActive = false;
                        operationTrace.append("inject=partition-heal, step=").append(step).append('\n');
                        harness.awaitLeaderElected(15000L);
                    }
                }
            }
        } finally {
            if (partitionActive) {
                harness.healAllNetworkIsolation();
                harness.awaitLeaderElected(15000L);
            }
        }
    }

    /**
     * 并发执行混合操作。
     */
    protected void runConcurrentWorkers(int workerCount,
                                        int operationsPerWorker,
                                        long seed,
                                        ConcurrentWorkerStepExecutor stepExecutor,
                                        StringBuilder operationTrace) throws Exception {
        if (workerCount <= 0) {
            throw new IllegalArgumentException("workerCount must be > 0");
        }
        if (operationsPerWorker <= 0) {
            throw new IllegalArgumentException("operationsPerWorker must be > 0");
        }
        if (stepExecutor == null) {
            throw new IllegalArgumentException("stepExecutor must not be null");
        }

        ExecutorService pool = Executors.newFixedThreadPool(workerCount);
        List<Future<Void>> futures = new ArrayList<>();
        try {
            for (int workerIndex = 0; workerIndex < workerCount; workerIndex++) {
                final int stableWorkerIndex = workerIndex;
                futures.add(pool.submit(new Callable<Void>() {
                    @Override
                    public Void call() throws Exception {
                        Random workerRandom = new Random(seed + stableWorkerIndex * 1000003L);
                        for (int operationIndex = 1; operationIndex <= operationsPerWorker; operationIndex++) {
                            stepExecutor.executeStep(stableWorkerIndex, operationIndex, workerRandom, operationTrace);
                        }
                        return null;
                    }
                }));
            }
            for (Future<Void> future : futures) {
                future.get(180, TimeUnit.SECONDS);
            }
        } finally {
            pool.shutdownNow();
            pool.awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    /**
     * 默认随机拓扑：将一个随机少数派分区与其余多数派隔离。
     */
    protected PartitionWindowTopology buildRandomMinorityPartitionTopology(Random random, String leaderId, List<String> sortedNodeIds) {
        if (sortedNodeIds == null || sortedNodeIds.size() < 3) {
            return null;
        }
        List<String> candidates = new ArrayList<>(sortedNodeIds);
        Collections.shuffle(candidates, random);

        int maxMinoritySize = (candidates.size() - 1) / 2;
        int minoritySize = 1 + random.nextInt(maxMinoritySize);
        List<String> minorityNodeIds = new ArrayList<>();
        for (int index = 0; index < minoritySize && index < candidates.size(); index++) {
            minorityNodeIds.add(candidates.get(index));
        }
        List<String> majorityNodeIds = new ArrayList<>();
        for (String nodeId : sortedNodeIds) {
            if (!minorityNodeIds.contains(nodeId)) {
                majorityNodeIds.add(nodeId);
            }
        }
        return PartitionWindowTopology.of(
                minorityNodeIds,
                majorityNodeIds,
                "random-minority-partition(leader=" + leaderId + ")");
    }

    /**
     * 故障注入流程统一实现。
     */
    protected void injectFault(FaultInjectionType faultInjectionType, StringBuilder operationTrace) throws Exception {
        if (faultInjectionType == null) {
            return;
        }
        switch (faultInjectionType) {
            case RESTART_FOLLOWER: {
                String leaderId = harness.awaitLeaderElected(defaultBoundaryTimeoutMillis());
                String followerId = harness.chooseFollowerId(leaderId);
                harness.stopNode(followerId);
                operationTrace.append("inject=restart-follower-stop, node=").append(followerId).append('\n');
                Thread.sleep(singleNodeRestartSleepMillis());
                harness.restartNode(followerId);
                operationTrace.append("inject=restart-follower-start, node=").append(followerId).append('\n');
                harness.awaitLeaderElected(defaultBoundaryTimeoutMillis());
                return;
            }
            case RESTART_LEADER: {
                String oldLeaderId = harness.awaitLeaderElected(defaultBoundaryTimeoutMillis());
                harness.stopNode(oldLeaderId);
                operationTrace.append("inject=restart-leader-stop, node=").append(oldLeaderId).append('\n');
                String newLeaderId = harness.awaitLeaderElectedExcluding(oldLeaderId, newLeaderElectionTimeoutMillis());
                operationTrace.append("inject=restart-leader-new-leader, node=").append(newLeaderId).append('\n');
                harness.restartNode(oldLeaderId);
                operationTrace.append("inject=restart-leader-start, node=").append(oldLeaderId).append('\n');
                harness.awaitLeaderElected(defaultBoundaryTimeoutMillis());
                return;
            }
            case FULL_CLUSTER_CRASH_RESTART: {
                List<String> nodeIds = new ArrayList<>(harness.getNodeIds());
                for (String nodeId : nodeIds) {
                    harness.stopNode(nodeId);
                    operationTrace.append("inject=full-crash-stop, node=").append(nodeId).append('\n');
                }
                Thread.sleep(fullClusterRestartSleepMillis());
                for (String nodeId : nodeIds) {
                    harness.restartNode(nodeId);
                    operationTrace.append("inject=full-crash-start, node=").append(nodeId).append('\n');
                }
                harness.awaitLeaderElected(20000L);
                return;
            }
            case ISOLATE_LEADER_FROM_MAJORITY_AND_HEAL: {
                String isolatedLeaderId = harness.awaitLeaderElected(defaultBoundaryTimeoutMillis());
                List<String> otherNodeIds = new ArrayList<>();
                for (String nodeId : harness.getNodeIds()) {
                    if (!isolatedLeaderId.equals(nodeId)) {
                        otherNodeIds.add(nodeId);
                    }
                }
                harness.isolateBidirectional(Collections.singletonList(isolatedLeaderId), otherNodeIds);
                operationTrace.append("inject=isolate-leader-start, node=").append(isolatedLeaderId).append('\n');
                String majorityLeaderId = harness.awaitLeaderElectedExcluding(isolatedLeaderId, newLeaderElectionTimeoutMillis());
                operationTrace.append("inject=isolate-leader-new-majority-leader, node=").append(majorityLeaderId).append('\n');
                harness.healAllNetworkIsolation();
                operationTrace.append("inject=isolate-leader-heal, node=").append(isolatedLeaderId).append('\n');
                harness.awaitLeaderElected(defaultBoundaryTimeoutMillis());
                return;
            }
            default:
                throw new IllegalArgumentException("unsupported fault injection type: " + faultInjectionType);
        }
    }
}
