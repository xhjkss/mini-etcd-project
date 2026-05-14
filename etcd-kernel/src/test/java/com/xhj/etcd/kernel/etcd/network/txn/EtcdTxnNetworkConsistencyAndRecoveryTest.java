package com.xhj.etcd.kernel.etcd.network.txn;

import com.xhj.etcd.kernel.etcd.etcdrpc.EtcdRpcResponse;
import com.xhj.etcd.kernel.etcd.etcdrpc.DeleteRequest;
import com.xhj.etcd.kernel.etcd.etcdrpc.GetRequest;
import com.xhj.etcd.kernel.etcd.etcdrpc.GetResponse;
import com.xhj.etcd.kernel.etcd.etcdrpc.PutRequest;
import com.xhj.etcd.kernel.etcd.etcdrpc.PutResponse;
import com.xhj.etcd.kernel.etcd.etcdrpc.RangeRequest;
import com.xhj.etcd.kernel.etcd.etcdrpc.TxnCompareCondition;
import com.xhj.etcd.kernel.etcd.etcdrpc.TxnCompareFieldType;
import com.xhj.etcd.kernel.etcd.etcdrpc.TxnCompareOperatorType;
import com.xhj.etcd.kernel.etcd.etcdrpc.TxnOperationRequest;
import com.xhj.etcd.kernel.etcd.etcdrpc.TxnRequest;
import com.xhj.etcd.kernel.etcd.etcdrpc.TxnResponse;
import com.xhj.etcd.kernel.etcd.network.support.EtcdDistributedTestSkeleton;
import com.xhj.etcd.kernel.etcd.network.support.EtcdTestSupport;
import com.xhj.etcd.rpc.NodeEndpoint;
import com.xhj.etcd.rpc.RpcException;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * EtcdTxnNetworkConsistencyAndRecoveryTest
 *
 * @author XJks
 * @description 基于 Etcd 分布式测试骨架的 Txn 网络测试，覆盖并发冲突、故障切换与崩溃恢复场景。
 */
public class EtcdTxnNetworkConsistencyAndRecoveryTest extends EtcdDistributedTestSkeleton {

    @Test
    public void shouldKeepTxnStateConvergedUnderLongRunningSoakWithLeaderRestartInjection() throws Exception {
        long seed = 2026051304L;
        StringBuilder operationTrace = new StringBuilder();
        int[] committedOperationCounter = new int[]{0};

        startClusterAndAwaitLeader(5, DEFAULT_BOUNDARY_TIMEOUT_MILLIS);
        List<String> keySpace = buildKeySpace("txn/soak/long/key-", 12);
        Map<String, String> expectedValueByKey = new HashMap<>();

        runRandomScenario(seed, 120, 18, FaultInjectionType.RESTART_LEADER, new RandomScenarioStepExecutor() {
            @Override
            public void executeStep(int step, Random random, StringBuilder trace) throws Exception {
                String key = keySpace.get(random.nextInt(keySpace.size()));
                int operationCode = random.nextInt(100);

                TxnRequest txnRequest;
                String operationType;
                if (operationCode < 70) {
                    String updateValue = "lv-" + step + "-" + random.nextInt(100000);
                    txnRequest = buildUnconditionalSetTxnRequest(key, updateValue);
                    operationType = "long-set";
                } else if (operationCode < 90) {
                    String expectedValue = random.nextBoolean() ? expectedValueByKey.get(key) : "long-mismatch-" + step;
                    String updateValue = "lc-" + step + "-" + random.nextInt(100000);
                    txnRequest = buildCompareValueSetTxnRequest(key, expectedValue, updateValue);
                    operationType = "long-compare-set";
                } else {
                    txnRequest = buildCompareValueDeleteTxnRequest(key, expectedValueByKey.get(key));
                    operationType = "long-compare-delete";
                }

                TxnResponse txnResponse = executeTxnWithLeaderFallback(txnRequest, trace, step, operationType);
                if (txnResponse == null) {
                    return;
                }
                committedOperationCounter[0]++;
                trace.append("step=").append(step)
                        .append(", op=").append(operationType)
                        .append(", txnSucceeded=").append(txnResponse.isSucceeded())
                        .append('\n');

                GetResponse linearizableResponse = executeGetWithLeaderFallback(key, trace, step);
                if (linearizableResponse != null) {
                    String actualValue = linearizableResponse.getValue();
                    if (actualValue == null) {
                        expectedValueByKey.remove(key);
                    } else {
                        expectedValueByKey.put(key, actualValue);
                    }
                }
            }
        }, operationTrace);

        assertTrue("committed operation count should be > 40\n" + operationTrace, committedOperationCounter[0] > 40);
        awaitLeader(25000L);
        refreshExpectedModelFromLeader(keySpace, expectedValueByKey, 15000L, operationTrace);
        verifyModelConvergedOnAllNodes(keySpace, expectedValueByKey, 30000L);
    }

    @Test
    public void shouldConvergeUnderRandomTxnOpsWithFollowerRestartInjection() throws Exception {
        long seed = 2026051302L;
        StringBuilder operationTrace = new StringBuilder();
        int[] compareResultCounter = new int[]{0, 0};

        startClusterAndAwaitLeader(5, DEFAULT_BOUNDARY_TIMEOUT_MILLIS);
        List<String> keySpace = buildKeySpace("txn/soak/restart/key-", 8);
        Map<String, String> expectedValueByKey = new HashMap<>();

        for (int index = 0; index < 4; index++) {
            String key = keySpace.get(index);
            String value = "seed-" + index;
            putOnLeaderWithRetry(key, value, 12000L);
            expectedValueByKey.put(key, value);
        }

        runRandomScenario(seed, 70, 14, FaultInjectionType.RESTART_FOLLOWER, new RandomScenarioStepExecutor() {
            @Override
            public void executeStep(int step, Random random, StringBuilder trace) throws Exception {
                String key = keySpace.get(random.nextInt(keySpace.size()));
                int operationCode = random.nextInt(100);

                if (operationCode < 65) {
                    String modelValue = expectedValueByKey.get(key);
                    String expectedValue = random.nextBoolean() ? modelValue : "mismatch-" + step;
                    String updateValue = "rv-" + step + "-" + random.nextInt(10000);
                    TxnRequest txnRequest = buildCompareValueSetTxnRequest(key, expectedValue, updateValue);
                    TxnResponse txnResponse = executeTxnWithLeaderFallback(txnRequest, trace, step, "compare-set");
                    if (txnResponse == null) {
                        return;
                    }
                    if (txnResponse.isSucceeded()) {
                        compareResultCounter[0]++;
                    } else {
                        compareResultCounter[1]++;
                    }
                    trace.append("step=").append(step).append(", op=compare-set, key=").append(key)
                            .append(", expected=").append(expectedValue)
                            .append(", succeeded=").append(txnResponse.isSucceeded()).append('\n');
                } else if (operationCode < 85) {
                    String updateValue = "uv-" + step + "-" + random.nextInt(10000);
                    TxnRequest txnRequest = buildUnconditionalSetTxnRequest(key, updateValue);
                    TxnResponse txnResponse = executeTxnWithLeaderFallback(txnRequest, trace, step, "unconditional-set");
                    if (txnResponse == null) {
                        return;
                    }
                    trace.append("step=").append(step).append(", op=unconditional-set, key=").append(key)
                            .append(", succeeded=").append(txnResponse.isSucceeded()).append('\n');
                } else {
                    String modelValue = expectedValueByKey.get(key);
                    TxnRequest txnRequest = buildCompareValueDeleteTxnRequest(key, modelValue);
                    TxnResponse txnResponse = executeTxnWithLeaderFallback(txnRequest, trace, step, "compare-delete");
                    if (txnResponse == null) {
                        return;
                    }
                    if (txnResponse.isSucceeded()) {
                        compareResultCounter[0]++;
                    } else {
                        compareResultCounter[1]++;
                    }
                    trace.append("step=").append(step).append(", op=compare-delete, key=").append(key)
                            .append(", expected=").append(modelValue)
                            .append(", succeeded=").append(txnResponse.isSucceeded()).append('\n');
                }

                GetResponse linearizableResponse = executeGetWithLeaderFallback(key, trace, step);
                if (linearizableResponse != null) {
                    String actualValue = linearizableResponse.getValue();
                    if (actualValue == null) {
                        expectedValueByKey.remove(key);
                    } else {
                        expectedValueByKey.put(key, actualValue);
                    }
                }
            }
        }, operationTrace);

        assertTrue("compare success count should be > 0\n" + operationTrace, compareResultCounter[0] > 0);
        assertTrue("compare failure count should be > 0\n" + operationTrace, compareResultCounter[1] > 0);
        awaitLeader(20000L);
        verifyModelConvergedOnAllNodes(keySpace, expectedValueByKey, 25000L);
    }

    @Test
    public void shouldConvergeUnderRandomTxnOpsWithPartitionWindowInjection() throws Exception {
        long seed = 2026051303L;
        StringBuilder operationTrace = new StringBuilder();

        startClusterAndAwaitLeader(5, DEFAULT_BOUNDARY_TIMEOUT_MILLIS);
        List<String> keySpace = buildKeySpace("txn/soak/partition/key-", 8);
        Map<String, String> expectedValueByKey = new HashMap<>();

        runRandomPartitionWindowScenario(seed, 64, 16, 6, null, new RandomScenarioStepExecutor() {
            @Override
            public void executeStep(int step, Random random, StringBuilder trace) throws Exception {
                String key = keySpace.get(random.nextInt(keySpace.size()));
                int operationCode = random.nextInt(100);

                if (operationCode < 75) {
                    String updateValue = "pv-" + step + "-" + random.nextInt(10000);
                    TxnRequest txnRequest = buildUnconditionalSetTxnRequest(key, updateValue);
                    TxnResponse txnResponse = executeTxnWithLeaderFallback(txnRequest, trace, step, "partition-set");
                    if (txnResponse == null) {
                        return;
                    }
                    trace.append("step=").append(step).append(", op=partition-set, key=").append(key)
                            .append(", succeeded=").append(txnResponse.isSucceeded()).append('\n');
                } else {
                    TxnRequest txnRequest = buildCompareValueDeleteTxnRequest(key, expectedValueByKey.get(key));
                    TxnResponse txnResponse = executeTxnWithLeaderFallback(txnRequest, trace, step, "partition-delete");
                    if (txnResponse == null) {
                        return;
                    }
                    trace.append("step=").append(step).append(", op=partition-delete, key=").append(key)
                            .append(", succeeded=").append(txnResponse.isSucceeded()).append('\n');
                }

                GetResponse linearizableResponse = executeGetWithLeaderFallback(key, trace, step);
                if (linearizableResponse != null) {
                    String actualValue = linearizableResponse.getValue();
                    if (actualValue == null) {
                        expectedValueByKey.remove(key);
                    } else {
                        expectedValueByKey.put(key, actualValue);
                    }
                }
            }
        }, operationTrace);

        awaitLeader(25000L);
        verifyModelConvergedOnAllNodes(keySpace, expectedValueByKey, 30000L);
    }

    @Test
    public void shouldRejectTxnWhenLeaderLosesMajorityAndResumeAfterMajorityRecovery() throws Exception {
        String leaderId = startClusterAndAwaitLeader(5, DEFAULT_BOUNDARY_TIMEOUT_MILLIS);
        NodeEndpoint initialLeaderEndpoint = requireEndpoint(leaderId);
        List<String> stoppedFollowerIds = new ArrayList<>(stopFollowers(leaderId, 3));
        assertEquals(3, stoppedFollowerIds.size());

        TxnRequest failedTxnRequest = buildVersionEqualsPutTxnRequest(
                "txn/majority-loss/guard",
                0L,
                "txn/majority-loss/result",
                "should-fail-before-majority");
        boolean rejectedByRpcException = false;
        try {
            EtcdRpcResponse<TxnResponse> failedTxnResponse = EtcdTestSupport.callTxnByRpc(
                    harness.getTestClient(),
                    initialLeaderEndpoint,
                    failedTxnRequest);
            if (failedTxnResponse != null && failedTxnResponse.getHeader() != null) {
                assertFalse(failedTxnResponse.getHeader().isSuccess());
            }
        } catch (RpcException expected) {
            rejectedByRpcException = true;
        }
        if (!rejectedByRpcException) {
            harness.awaitKeyDeleted("txn/majority-loss/result", harness.quorumSize(), 10000L);
        }

        harness.restartNode(stoppedFollowerIds.get(0));
        harness.restartNode(stoppedFollowerIds.get(1));
        awaitLeader(15000L);

        TxnRequest recoveredTxnRequest = buildVersionEqualsPutTxnRequest(
                "txn/majority-loss/guard",
                0L,
                "txn/majority-loss/result",
                "committed-after-majority");
        TxnResponse recoveredTxnResponse = txnOnLeaderWithRetry(recoveredTxnRequest, 15000L);
        assertNotNull(recoveredTxnResponse);
        assertTrue(recoveredTxnResponse.isSucceeded());
        harness.awaitValueReplicated("txn/majority-loss/result", "committed-after-majority", harness.quorumSize(), 15000L);
    }

    @Test
    public void shouldKeepTxnFailureBranchResultConsistentAcrossNodes() throws Exception {
        startClusterAndAwaitLeader(5, DEFAULT_BOUNDARY_TIMEOUT_MILLIS);
        putOnLeaderWithRetry("txn/failure-branch/guard", "closed", 10000L);

        TxnRequest txnRequest = new TxnRequest();
        TxnCompareCondition compareCondition = new TxnCompareCondition();
        compareCondition.setKey("txn/failure-branch/guard");
        compareCondition.setCompareFieldType(TxnCompareFieldType.VALUE);
        compareCondition.setCompareOperatorType(TxnCompareOperatorType.EQUAL);
        compareCondition.setData("open");
        txnRequest.getCompareConditions().add(compareCondition);
        txnRequest.getSuccessOperations().add(TxnOperationRequest.put(new PutRequest("txn/failure-branch/success", "should-not-appear")));
        txnRequest.getFailureOperations().add(TxnOperationRequest.put(new PutRequest("txn/failure-branch/failure", "applied-value")));

        TxnResponse txnResponse = txnOnLeaderWithRetry(txnRequest, 15000L);
        assertNotNull(txnResponse);
        assertFalse(txnResponse.isSucceeded());

        harness.awaitValueReplicated("txn/failure-branch/failure", "applied-value", harness.getClusterSize(), 15000L);
        harness.awaitKeyDeleted("txn/failure-branch/success", harness.getClusterSize(), 10000L);
    }

    @Test
    public void shouldKeepTxnSemanticsCorrectAfterLeaderFailover() throws Exception {
        String oldLeaderId = startClusterAndAwaitLeader(5, DEFAULT_BOUNDARY_TIMEOUT_MILLIS);
        harness.stopNode(oldLeaderId);
        String newLeaderId = awaitNewLeaderExcluding(oldLeaderId, 15000L);
        assertNotNull(newLeaderId);
        assertNotEquals(oldLeaderId, newLeaderId);

        TxnRequest txnRequest = new TxnRequest();
        TxnCompareCondition compareCondition = new TxnCompareCondition();
        compareCondition.setKey("txn/failover/guard");
        compareCondition.setCompareFieldType(TxnCompareFieldType.VERSION);
        compareCondition.setCompareOperatorType(TxnCompareOperatorType.EQUAL);
        compareCondition.setData(0L);
        txnRequest.getCompareConditions().add(compareCondition);
        txnRequest.getSuccessOperations().add(TxnOperationRequest.put(new PutRequest("txn/failover/key", "after-failover")));

        TxnResponse txnResponse = txnOnLeaderWithRetry(txnRequest, 15000L);
        assertNotNull(txnResponse);
        assertTrue(txnResponse.isSucceeded());

        harness.awaitValueReplicated("txn/failover/key", "after-failover", harness.quorumSize(), 15000L);
        harness.restartNode(oldLeaderId);
        harness.awaitValueReplicated("txn/failover/key", "after-failover", harness.getClusterSize(), 15000L);
    }

    @Test
    public void shouldAllowOnlyOneTxnCasSuccessUnderConcurrentConflict() throws Exception {
        startClusterAndAwaitLeader(5, DEFAULT_BOUNDARY_TIMEOUT_MILLIS);
        String casKey = "txn/cas/conflict-key";
        putOnLeaderWithRetry(casKey, "base", 12000L);

        int concurrentClientCount = 6;
        ExecutorService pool = Executors.newFixedThreadPool(concurrentClientCount);
        CountDownLatch startLatch = new CountDownLatch(1);
        List<Future<TxnResponse>> futures = new ArrayList<>();
        List<String> workerValues = new ArrayList<>();
        try {
            for (int workerIndex = 0; workerIndex < concurrentClientCount; workerIndex++) {
                String workerValue = "worker-value-" + workerIndex;
                workerValues.add(workerValue);
                futures.add(pool.submit(() -> {
                    startLatch.await(5, TimeUnit.SECONDS);
                    TxnRequest txnRequest = buildCompareAndSwapTxnRequest(casKey, "base", workerValue);
                    return txnOnLeaderWithRetry(txnRequest, 15000L);
                }));
            }

            startLatch.countDown();

            int succeededCount = 0;
            String expectedWinnerValue = null;
            for (int workerIndex = 0; workerIndex < futures.size(); workerIndex++) {
                TxnResponse txnResponse = futures.get(workerIndex).get(30, TimeUnit.SECONDS);
                assertNotNull(txnResponse);
                if (txnResponse.isSucceeded()) {
                    succeededCount++;
                    expectedWinnerValue = workerValues.get(workerIndex);
                }
            }
            assertEquals(1, succeededCount);
            assertNotNull(expectedWinnerValue);

            harness.awaitValueReplicated(casKey, expectedWinnerValue, harness.getClusterSize(), 15000L);
            GetResponse finalValueResponse = getLinearizableFromLeaderWithRetry(casKey, 12000L);
            assertNotNull(finalValueResponse);
            assertEquals(expectedWinnerValue, finalValueResponse.getValue());
        } finally {
            pool.shutdownNow();
            assertTrue(pool.awaitTermination(3, TimeUnit.SECONDS));
        }
    }

    @Test
    public void shouldRecoverCommittedTxnResultAfterFullClusterCrashAndRestart() throws Exception {
        startClusterAndAwaitLeader(5, DEFAULT_BOUNDARY_TIMEOUT_MILLIS);

        TxnRequest txnRequest = new TxnRequest();
        TxnCompareCondition compareCondition = new TxnCompareCondition();
        compareCondition.setKey("txn/recovery/success/guard");
        compareCondition.setCompareFieldType(TxnCompareFieldType.VERSION);
        compareCondition.setCompareOperatorType(TxnCompareOperatorType.EQUAL);
        compareCondition.setData(0L);
        txnRequest.getCompareConditions().add(compareCondition);
        txnRequest.getSuccessOperations().add(TxnOperationRequest.put(new PutRequest("txn/recovery/success/key", "txn-success-value")));

        TxnResponse txnResponse = txnOnLeaderWithRetry(txnRequest, 15000L);
        assertNotNull(txnResponse);
        assertTrue(txnResponse.isSucceeded());
        long txnRevision = txnResponse.getRevision();

        harness.awaitValueReplicated("txn/recovery/success/key", "txn-success-value", harness.getClusterSize(), 15000L);

        List<String> nodeIds = new ArrayList<>(harness.getNodeIds());
        for (String nodeId : nodeIds) {
            harness.stopNode(nodeId);
        }
        Thread.sleep(1000L);
        for (String nodeId : nodeIds) {
            harness.restartNode(nodeId);
        }
        awaitLeader(20000L);

        harness.awaitValueReplicated("txn/recovery/success/key", "txn-success-value", harness.getClusterSize(), 20000L);
        GetResponse recoveredResponse = getLinearizableFromLeaderWithRetry("txn/recovery/success/key", 12000L);
        assertNotNull(recoveredResponse);
        assertEquals("txn-success-value", recoveredResponse.getValue());
        assertTrue(recoveredResponse.getRevision() >= txnRevision);
    }

    @Test
    public void shouldNotLeavePartialTxnWritesAfterRollbackAndRestart() throws Exception {
        startClusterAndAwaitLeader(5, DEFAULT_BOUNDARY_TIMEOUT_MILLIS);
        putOnLeaderWithRetry("txn/recovery/rollback/baseline", "baseline", 10000L);

        TxnRequest txnRequest = new TxnRequest();
        TxnCompareCondition compareCondition = new TxnCompareCondition();
        compareCondition.setKey("txn/recovery/rollback/guard");
        compareCondition.setCompareFieldType(TxnCompareFieldType.VERSION);
        compareCondition.setCompareOperatorType(TxnCompareOperatorType.EQUAL);
        compareCondition.setData(0L);
        txnRequest.getCompareConditions().add(compareCondition);
        txnRequest.getSuccessOperations().add(TxnOperationRequest.put(new PutRequest("txn/recovery/rollback/key", "should-not-exist")));

        RangeRequest invalidRangeRequest = new RangeRequest();
        invalidRangeRequest.setStartKey("txn/recovery/rollback/z");
        invalidRangeRequest.setEndKeyExclusive("txn/recovery/rollback/a");
        invalidRangeRequest.setLinearizableRead(false);
        txnRequest.getSuccessOperations().add(TxnOperationRequest.range(invalidRangeRequest));

        NodeEndpoint leaderEndpoint = harness.awaitLeaderEndpoint(10000L);
        EtcdRpcResponse<TxnResponse> failedTxnResponse = EtcdTestSupport.callTxnByRpc(harness.getTestClient(), leaderEndpoint, txnRequest);
        assertNotNull(failedTxnResponse);
        assertNotNull(failedTxnResponse.getHeader());
        assertFalse(failedTxnResponse.getHeader().isSuccess());

        List<String> nodeIds = new ArrayList<>(harness.getNodeIds());
        for (String nodeId : nodeIds) {
            harness.stopNode(nodeId);
        }
        Thread.sleep(1000L);
        for (String nodeId : nodeIds) {
            harness.restartNode(nodeId);
        }
        awaitLeader(20000L);

        harness.awaitKeyDeleted("txn/recovery/rollback/key", harness.getClusterSize(), 15000L);
        harness.awaitValueReplicated("txn/recovery/rollback/baseline", "baseline", harness.getClusterSize(), 15000L);
    }

    @Test
    public void shouldSupportCreateRevisionCompareTxnAcrossCluster() throws Exception {
        startClusterAndAwaitLeader(5, DEFAULT_BOUNDARY_TIMEOUT_MILLIS);

        NodeEndpoint leaderEndpoint = harness.awaitLeaderEndpoint(10000L);
        EtcdRpcResponse<PutResponse> putResponse = EtcdTestSupport.callPutByRpc(
                harness.getTestClient(),
                leaderEndpoint,
                "txn/create-revision/guard",
                "v1");
        assertNotNull(putResponse);
        assertNotNull(putResponse.getHeader());
        assertTrue(putResponse.getHeader().isSuccess());
        assertNotNull(putResponse.getBody());
        long createRevision = putResponse.getBody().getRevision();

        TxnRequest txnRequest = new TxnRequest();
        TxnCompareCondition compareCondition = new TxnCompareCondition();
        compareCondition.setKey("txn/create-revision/guard");
        compareCondition.setCompareFieldType(TxnCompareFieldType.CREATE_REVISION);
        compareCondition.setCompareOperatorType(TxnCompareOperatorType.EQUAL);
        compareCondition.setData(createRevision);
        txnRequest.getCompareConditions().add(compareCondition);
        txnRequest.getSuccessOperations().add(TxnOperationRequest.put(new PutRequest("txn/create-revision/result", "ok")));
        txnRequest.getFailureOperations().add(TxnOperationRequest.get(new GetRequest("txn/create-revision/guard", false)));

        TxnResponse txnResponse = txnOnLeaderWithRetry(txnRequest, 15000L);
        assertNotNull(txnResponse);
        assertTrue(txnResponse.isSucceeded());
        harness.awaitValueReplicated("txn/create-revision/result", "ok", harness.getClusterSize(), 15000L);
    }

    @Test
    public void shouldCommitTxnOnMajorityPartitionAndConvergeAfterNetworkHeal() throws Exception {
        String oldLeaderId = startClusterAndAwaitLeader(5, DEFAULT_BOUNDARY_TIMEOUT_MILLIS);
        List<String> followerNodeIds = chooseFollowerIds(oldLeaderId, 4);
        assertEquals(4, followerNodeIds.size());

        List<String> minorityNodeIds = new ArrayList<>();
        minorityNodeIds.add(oldLeaderId);
        minorityNodeIds.add(followerNodeIds.get(0));
        List<String> majorityNodeIds = new ArrayList<>();
        majorityNodeIds.add(followerNodeIds.get(1));
        majorityNodeIds.add(followerNodeIds.get(2));
        majorityNodeIds.add(followerNodeIds.get(3));

        isolateBidirectional(minorityNodeIds, majorityNodeIds);

        NodeEndpoint oldLeaderEndpoint = requireEndpoint(oldLeaderId);
        TxnRequest minorityTxnRequest = buildVersionEqualsPutTxnRequest(
                "txn/partition/guard",
                0L,
                "txn/partition/result",
                "minority-write");
        try {
            EtcdRpcResponse<TxnResponse> minorityTxnResponse = EtcdTestSupport.callTxnByRpc(
                    harness.getTestClient(),
                    oldLeaderEndpoint,
                    minorityTxnRequest);
            if (minorityTxnResponse != null && minorityTxnResponse.getHeader() != null) {
                assertFalse(minorityTxnResponse.getHeader().isSuccess());
            }
        } catch (RpcException ignore) {
            // minority 分区上 direct RPC 失败是预期行为。
        }

        String newLeaderId = awaitNewLeaderExcluding(oldLeaderId, 15000L);
        assertNotNull(newLeaderId);
        assertTrue(majorityNodeIds.contains(newLeaderId));

        TxnRequest majorityTxnRequest = buildVersionEqualsPutTxnRequest(
                "txn/partition/guard",
                0L,
                "txn/partition/result",
                "majority-committed");
        TxnResponse majorityTxnResponse = txnOnLeaderWithRetry(majorityTxnRequest, 15000L);
        assertNotNull(majorityTxnResponse);
        assertTrue(majorityTxnResponse.isSucceeded());
        harness.awaitValueReplicated("txn/partition/result", "majority-committed", majorityNodeIds.size(), 15000L);

        healAllNetworkIsolation();
        harness.awaitValueReplicated("txn/partition/result", "majority-committed", harness.getClusterSize(), 20000L);
    }

    @Test
    public void shouldCatchUpRestartedFollowerBySnapshotAfterTxnBurst() throws Exception {
        setSnapshotTriggerLogCount(2);
        String leaderId = startClusterAndAwaitLeader(3, DEFAULT_BOUNDARY_TIMEOUT_MILLIS);
        String laggingFollowerId = chooseFollowerId(leaderId);
        harness.stopNode(laggingFollowerId);

        String latestKey = null;
        for (int index = 1; index <= 12; index++) {
            latestKey = "txn/snapshot/key-" + index;
            TxnRequest txnRequest = buildVersionEqualsPutTxnRequest(
                    "txn/snapshot/guard-" + index,
                    0L,
                    latestKey,
                    "value-" + index);
            TxnResponse txnResponse = txnOnLeaderWithRetry(txnRequest, 15000L);
            assertNotNull(txnResponse);
            assertTrue(txnResponse.isSucceeded());
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
    public void shouldApplyFailureBranchWhenAnyCompareConditionMismatched() throws Exception {
        startClusterAndAwaitLeader(5, DEFAULT_BOUNDARY_TIMEOUT_MILLIS);
        putOnLeaderWithRetry("txn/multi-compare/guard-a", "a1", 12000L);
        putOnLeaderWithRetry("txn/multi-compare/guard-b", "b1", 12000L);

        TxnRequest txnRequest = new TxnRequest();
        TxnCompareCondition firstCondition = TxnCompareCondition.value(
                "txn/multi-compare/guard-a",
                TxnCompareOperatorType.EQUAL,
                "a1");
        TxnCompareCondition secondCondition = TxnCompareCondition.value(
                "txn/multi-compare/guard-b",
                TxnCompareOperatorType.EQUAL,
                "b2");
        txnRequest.getCompareConditions().add(firstCondition);
        txnRequest.getCompareConditions().add(secondCondition);
        txnRequest.getSuccessOperations().add(TxnOperationRequest.put(new PutRequest("txn/multi-compare/success", "not-expected")));
        txnRequest.getFailureOperations().add(TxnOperationRequest.put(new PutRequest("txn/multi-compare/failure", "expected")));

        TxnResponse txnResponse = txnOnLeaderWithRetry(txnRequest, 15000L);
        assertNotNull(txnResponse);
        assertFalse(txnResponse.isSucceeded());
        harness.awaitValueReplicated("txn/multi-compare/failure", "expected", harness.getClusterSize(), 15000L);
        harness.awaitKeyDeleted("txn/multi-compare/success", harness.getClusterSize(), 10000L);
    }

    private TxnRequest buildCompareAndSwapTxnRequest(String key, String expectedValue, String updateValue) {
        TxnRequest txnRequest = new TxnRequest();

        TxnCompareCondition compareCondition = new TxnCompareCondition();
        compareCondition.setKey(key);
        compareCondition.setCompareFieldType(TxnCompareFieldType.VALUE);
        compareCondition.setCompareOperatorType(TxnCompareOperatorType.EQUAL);
        compareCondition.setData(expectedValue);
        txnRequest.getCompareConditions().add(compareCondition);

        txnRequest.getSuccessOperations().add(TxnOperationRequest.put(new PutRequest(key, updateValue)));
        txnRequest.getFailureOperations().add(TxnOperationRequest.get(new GetRequest(key, false)));
        return txnRequest;
    }

    private TxnRequest buildCompareValueSetTxnRequest(String key, String expectedValue, String updateValue) {
        TxnRequest txnRequest = new TxnRequest();
        txnRequest.getCompareConditions().add(TxnCompareCondition.value(key, TxnCompareOperatorType.EQUAL, expectedValue));
        txnRequest.getSuccessOperations().add(TxnOperationRequest.put(new PutRequest(key, updateValue)));
        txnRequest.getFailureOperations().add(TxnOperationRequest.get(new GetRequest(key, false)));
        return txnRequest;
    }

    private TxnRequest buildUnconditionalSetTxnRequest(String key, String updateValue) {
        TxnRequest txnRequest = new TxnRequest();
        txnRequest.getSuccessOperations().add(TxnOperationRequest.put(new PutRequest(key, updateValue)));
        return txnRequest;
    }

    private TxnRequest buildCompareValueDeleteTxnRequest(String key, String expectedValue) {
        TxnRequest txnRequest = new TxnRequest();
        txnRequest.getCompareConditions().add(TxnCompareCondition.value(key, TxnCompareOperatorType.EQUAL, expectedValue));
        txnRequest.getSuccessOperations().add(TxnOperationRequest.delete(new DeleteRequest(key)));
        txnRequest.getFailureOperations().add(TxnOperationRequest.get(new GetRequest(key, false)));
        return txnRequest;
    }

    private TxnResponse executeTxnWithLeaderFallback(TxnRequest txnRequest,
                                                     StringBuilder operationTrace,
                                                     int step,
                                                     String operationType) throws Exception {
        try {
            return txnOnLeaderWithRetry(txnRequest, 12000L);
        } catch (AssertionError assertionError) {
            if (isLeaderWindowAssertion(assertionError)) {
                operationTrace.append("step=").append(step)
                        .append(", op=").append(operationType)
                        .append(", skipped=leader-window").append('\n');
                return null;
            }
            throw assertionError;
        }
    }

    private GetResponse executeGetWithLeaderFallback(String key,
                                                     StringBuilder operationTrace,
                                                     int step) throws Exception {
        try {
            return getLinearizableFromLeaderWithRetry(key, 10000L);
        } catch (AssertionError assertionError) {
            if (isLeaderWindowAssertion(assertionError)) {
                operationTrace.append("step=").append(step)
                        .append(", op=get")
                        .append(", key=").append(key)
                        .append(", skipped=leader-window").append('\n');
                return null;
            }
            throw assertionError;
        }
    }

    private boolean isLeaderWindowAssertion(AssertionError assertionError) {
        if (assertionError == null || assertionError.getMessage() == null) {
            return false;
        }
        String message = assertionError.getMessage();
        return message.contains("leader is not elected")
                || message.contains("txn retry timeout")
                || message.contains("linearizable get retry timeout");
    }

    private List<String> buildKeySpace(String keyPrefix, int keyCount) {
        List<String> keySpace = new ArrayList<>();
        for (int index = 0; index < keyCount; index++) {
            keySpace.add(keyPrefix + index);
        }
        return keySpace;
    }

    private void verifyModelConvergedOnAllNodes(List<String> keySpace,
                                                Map<String, String> expectedValueByKey,
                                                long timeoutMillis) throws Exception {
        for (String key : keySpace) {
            String expectedValue = expectedValueByKey.get(key);
            harness.awaitValueReplicated(key, expectedValue, harness.getClusterSize(), timeoutMillis);
            GetResponse response = getLinearizableFromLeaderWithRetry(key, timeoutMillis);
            assertNotNull(response);
            assertEquals(expectedValue, response.getValue());
        }
    }

    private void refreshExpectedModelFromLeader(List<String> keySpace,
                                                Map<String, String> expectedValueByKey,
                                                long timeoutMillis,
                                                StringBuilder operationTrace) throws Exception {
        expectedValueByKey.clear();
        for (String key : keySpace) {
            GetResponse response;
            try {
                response = getLinearizableFromLeaderWithRetry(key, timeoutMillis);
            } catch (AssertionError assertionError) {
                throw new AssertionError("failed to refresh expected model from leader, key=" + key + '\n' + operationTrace, assertionError);
            }
            if (response != null && response.getValue() != null) {
                expectedValueByKey.put(key, response.getValue());
            }
        }
    }

    private TxnRequest buildVersionEqualsPutTxnRequest(String guardKey, long expectedVersion, String writeKey, String writeValue) {
        TxnRequest txnRequest = new TxnRequest();
        TxnCompareCondition compareCondition = TxnCompareCondition.version(
                guardKey,
                TxnCompareOperatorType.EQUAL,
                expectedVersion);
        txnRequest.getCompareConditions().add(compareCondition);
        txnRequest.getSuccessOperations().add(TxnOperationRequest.put(new PutRequest(writeKey, writeValue)));
        return txnRequest;
    }
}
