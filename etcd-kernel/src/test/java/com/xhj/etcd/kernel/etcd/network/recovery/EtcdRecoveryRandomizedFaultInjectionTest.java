package com.xhj.etcd.kernel.etcd.network.recovery;

import com.xhj.etcd.kernel.etcd.network.support.EtcdDistributedTestSkeleton;
import com.xhj.etcd.kernel.etcd.network.support.EtcdRpcAssert;
import com.xhj.etcd.kernel.etcd.network.support.EtcdTestSupport;

import com.xhj.etcd.kernel.etcd.etcdrpc.DeleteRangeRequest;
import com.xhj.etcd.kernel.etcd.etcdrpc.DeleteRangeResponse;
import com.xhj.etcd.kernel.etcd.etcdrpc.EtcdRpcResponse;
import com.xhj.etcd.kernel.etcd.etcdrpc.GetResponse;
import com.xhj.etcd.kernel.etcd.etcdrpc.KeyValueView;
import com.xhj.etcd.kernel.etcd.etcdrpc.PutResponse;
import com.xhj.etcd.kernel.etcd.etcdrpc.RangeRequest;
import com.xhj.etcd.kernel.etcd.etcdrpc.RangeResponse;
import com.xhj.etcd.rpc.NodeEndpoint;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * EtcdRecoveryRandomizedFaultInjectionTest
 *
 * @author XJks
 * @description Etcd 随机故障注入恢复测试，覆盖分区窗口与随机重启注入下的一致性与收敛性。
 */
public class EtcdRecoveryRandomizedFaultInjectionTest extends EtcdDistributedTestSkeleton {

    @Test
    public void shouldSwitchLeaderOnMajoritySideWhenOldLeaderIsIsolatedAndHealConsistently() throws Exception {
        String oldLeaderId = startClusterAndAwaitLeader(5, DEFAULT_BOUNDARY_TIMEOUT_MILLIS);
        putOnLeaderWithRetry("partition/window/base", "v0", 12000L);
        harness.awaitValueReplicated("partition/window/base", "v0", harness.getClusterSize(), 12000L);

        List<String> otherNodeIds = new ArrayList<>();
        for (String nodeId : harness.getNodeIds()) {
            if (!oldLeaderId.equals(nodeId)) {
                otherNodeIds.add(nodeId);
            }
        }
        isolateBidirectional(Collections.singletonList(oldLeaderId), otherNodeIds);

        NodeEndpoint oldLeaderEndpoint = requireEndpoint(oldLeaderId);
        EtcdRpcResponse<PutResponse> isolatedWriteResponse = tryPutOnce(oldLeaderEndpoint, "partition/window/key", "isolated-write", 2500L);
        if (isolatedWriteResponse != null && isolatedWriteResponse.getHeader() != null) {
            assertTrue(!isolatedWriteResponse.getHeader().isSuccess());
        }

        String majorityLeaderId = awaitNewLeaderExcluding(oldLeaderId, 15000L);
        assertNotNull(majorityLeaderId);
        assertNotEquals(oldLeaderId, majorityLeaderId);

        NodeEndpoint majorityLeaderEndpoint = requireEndpoint(majorityLeaderId);
        EtcdRpcResponse<PutResponse> majorityWriteResponse = EtcdTestSupport.callPutByRpc(
                harness.getTestClient(),
                majorityLeaderEndpoint,
                "partition/window/key",
                "majority-write");
        EtcdRpcAssert.assertSuccess(majorityWriteResponse);
        harness.awaitValueReplicated("partition/window/key", "majority-write", harness.quorumSize(), 12000L);

        healAllNetworkIsolation();
        awaitLeader(15000L);
        harness.awaitValueReplicated("partition/window/key", "majority-write", harness.getClusterSize(), 15000L);
    }

    @Test
    public void shouldKeepMajorityWritableWhenMinorityIsolatedAndRecoverFollowerDataAfterHeal() throws Exception {
        String leaderId = startClusterAndAwaitLeader(5, DEFAULT_BOUNDARY_TIMEOUT_MILLIS);
        List<String> minorityNodeIds = chooseFollowerIds(leaderId, 2);
        assertTrue(minorityNodeIds.size() == 2);

        List<String> majorityNodeIds = new ArrayList<>();
        for (String nodeId : harness.getNodeIds()) {
            if (!minorityNodeIds.contains(nodeId)) {
                majorityNodeIds.add(nodeId);
            }
        }
        isolateBidirectional(minorityNodeIds, majorityNodeIds);

        putOnLeaderWithRetry("partition/minority/key", "v-majority", 12000L);
        harness.awaitValueReplicated("partition/minority/key", "v-majority", harness.quorumSize(), 12000L);

        healAllNetworkIsolation();
        awaitLeader(15000L);
        harness.awaitValueReplicated("partition/minority/key", "v-majority", harness.getClusterSize(), 15000L);
    }

    /**
     * 随机执行 put/delete/get，并周期性注入 follower/leader 重启。
     *
     * <p>校验目标：</p>
     * <p>1) 线性一致读始终与测试侧 reference model 一致；</p>
     * <p>2) 重启注入后仍能继续提交写请求；</p>
     * <p>3) 最终状态可在全节点追平。</p>
     */
    @Test
    public void shouldKeepStateConsistentUnderRandomOpsWithSingleNodeRestartInjection() throws Exception {
        long seed = 2026051101L;
        StringBuilder operationTrace = new StringBuilder();

        startClusterAndAwaitLeader(3, DEFAULT_BOUNDARY_TIMEOUT_MILLIS);

        List<String> keySpace = buildKeySpace("rnd-a/key-", 8);
        Map<String, String> expectedValueByKey = new HashMap<>();

        runRandomScenario(seed, 90, 15, FaultInjectionType.RESTART_FOLLOWER, new RandomScenarioStepExecutor() {
            @Override
            public void executeStep(int step, Random random, StringBuilder trace) throws Exception {
                String key = keySpace.get(random.nextInt(keySpace.size()));
                int opCode = random.nextInt(100);
                if (opCode < 50) {
                    String value = "v-" + step + "-" + random.nextInt(1000);
                    putOnLeaderWithRetry(key, value, 12000L);
                    expectedValueByKey.put(key, value);
                    trace.append("step=").append(step).append(", op=put, key=").append(key).append(", value=").append(value).append('\n');
                    return;
                }
                if (opCode < 75) {
                    deleteOnLeaderWithRetry(key, 12000L);
                    expectedValueByKey.remove(key);
                    trace.append("step=").append(step).append(", op=delete, key=").append(key).append('\n');
                    return;
                }
                GetResponse response = getLinearizableFromLeaderWithRetry(key, 10000L);
                assertNotNull(response);
                String expectedValue = expectedValueByKey.get(key);
                assertEquals("linearizable read mismatch, seed=" + seed + ", step=" + step + '\n' + trace,
                        expectedValue,
                        response.getValue());
                trace.append("step=").append(step).append(", op=get, key=").append(key).append(", value=").append(response.getValue()).append('\n');
            }
        }, operationTrace);

        verifyModelConvergedOnAllNodes(keySpace, expectedValueByKey, 20000L);
    }

    /**
     * 随机序列执行中注入一次全量崩溃重启，验证恢复后随机读写语义仍正确。
     */
    @Test
    public void shouldKeepStateConsistentUnderRandomOpsWithFullClusterCrashInjection() throws Exception {
        long seed = 2026051102L;
        StringBuilder operationTrace = new StringBuilder();

        startClusterAndAwaitLeader(5, DEFAULT_BOUNDARY_TIMEOUT_MILLIS);

        List<String> keySpace = buildKeySpace("rnd-b/key-", 10);
        Map<String, String> expectedValueByKey = new HashMap<>();

        runRandomScenario(seed, 70, 36, FaultInjectionType.FULL_CLUSTER_CRASH_RESTART, new RandomScenarioStepExecutor() {
            @Override
            public void executeStep(int step, Random random, StringBuilder trace) throws Exception {
                String key = keySpace.get(random.nextInt(keySpace.size()));
                int opCode = random.nextInt(100);
                if (opCode < 55) {
                    String value = "v-" + step + "-" + random.nextInt(1000);
                    putOnLeaderWithRetry(key, value, 12000L);
                    expectedValueByKey.put(key, value);
                    trace.append("step=").append(step).append(", op=put, key=").append(key).append(", value=").append(value).append('\n');
                    return;
                }
                if (opCode < 80) {
                    deleteOnLeaderWithRetry(key, 12000L);
                    expectedValueByKey.remove(key);
                    trace.append("step=").append(step).append(", op=delete, key=").append(key).append('\n');
                    return;
                }
                GetResponse response = getLinearizableFromLeaderWithRetry(key, 10000L);
                assertNotNull(response);
                String expectedValue = expectedValueByKey.get(key);
                assertEquals("linearizable read mismatch, seed=" + seed + ", step=" + step + '\n' + trace,
                        expectedValue,
                        response.getValue());
                trace.append("step=").append(step).append(", op=get, key=").append(key).append(", value=").append(response.getValue()).append('\n');
            }
        }, operationTrace);

        verifyModelConvergedOnAllNodes(keySpace, expectedValueByKey, 25000L);
    }

    /**
     * 随机执行 put/delete/range-prefix/deleteRange-prefix，并注入重启，校验前缀语义与 reference model 一致。
     */
    @Test
    public void shouldKeepPrefixRangeAndDeleteRangeSemanticsConsistentUnderRestartInjection() throws Exception {
        long seed = 2026051103L;
        Random random = new Random(seed);
        StringBuilder operationTrace = new StringBuilder();
        operationTrace.append("seed=").append(seed).append('\n');

        startClusterAndAwaitLeader(3, DEFAULT_BOUNDARY_TIMEOUT_MILLIS);

        List<String> keySpace = new ArrayList<>();
        keySpace.addAll(buildKeySpace("app/a/", 6));
        keySpace.addAll(buildKeySpace("app/b/", 6));
        keySpace.addAll(buildKeySpace("sys/c/", 6));
        Map<String, String> expectedValueByKey = new HashMap<>();
        String[] prefixPool = new String[]{"app/a/", "app/b/", "sys/c/"};

        int totalSteps = 80;
        for (int step = 1; step <= totalSteps; step++) {
            if (step % 12 == 0) {
                injectFault(FaultInjectionType.RESTART_FOLLOWER, operationTrace);
            }

            int opCode = random.nextInt(100);
            if (opCode < 35) {
                String key = keySpace.get(random.nextInt(keySpace.size()));
                String value = "pv-" + step + "-" + random.nextInt(1000);
                putOnLeaderWithRetry(key, value, 12000L);
                expectedValueByKey.put(key, value);
                operationTrace.append("step=").append(step).append(", op=put, key=").append(key).append(", value=").append(value).append('\n');
                continue;
            }

            if (opCode < 55) {
                String key = keySpace.get(random.nextInt(keySpace.size()));
                deleteOnLeaderWithRetry(key, 12000L);
                expectedValueByKey.remove(key);
                operationTrace.append("step=").append(step).append(", op=delete, key=").append(key).append('\n');
                continue;
            }

            if (opCode < 80) {
                String prefix = prefixPool[random.nextInt(prefixPool.length)];
                RangeResponse rangeResponse = rangePrefixWithModelCheck(prefix, expectedValueByKey, 12000L, seed, step, operationTrace);
                operationTrace.append("step=").append(step).append(", op=range-prefix, prefix=").append(prefix)
                        .append(", count=").append(rangeResponse.getCount()).append('\n');
                continue;
            }

            String prefix = prefixPool[random.nextInt(prefixPool.length)];
            int expectedDeleteCount = countKeysByPrefix(expectedValueByKey, prefix);
            DeleteRangeRequest deleteRangeRequest = new DeleteRangeRequest();
            deleteRangeRequest.setStartKey(prefix);
            deleteRangeRequest.setPrefixMatch(true);
            deleteRangeRequest.setPrevKv(true);
            DeleteRangeResponse deleteRangeResponse = deleteRangeOnLeaderWithRetry(deleteRangeRequest, 12000L);
            if (deleteRangeResponse.getDeletedCount() != expectedDeleteCount) {
                RangeRequest remainRangeRequest = new RangeRequest();
                remainRangeRequest.setStartKey(prefix);
                remainRangeRequest.setPrefixMatch(true);
                RangeResponse remainRangeResponse = rangeOnLeaderWithRetry(remainRangeRequest, 12000L);
                assertEquals("delete-range count mismatch, and prefix still has remaining keys, seed=" + seed + ", step=" + step + '\n' + operationTrace,
                        0,
                        remainRangeResponse.getCount());
                operationTrace.append("step=").append(step)
                        .append(", note=delete-range-count-mismatch-but-prefix-cleared")
                        .append(", prefix=").append(prefix)
                        .append(", expectedDeleteCount=").append(expectedDeleteCount)
                        .append(", responseDeletedCount=").append(deleteRangeResponse.getDeletedCount())
                        .append('\n');
            } else {
                assertEquals("delete-range prev-items size mismatch, seed=" + seed + ", step=" + step + '\n' + operationTrace,
                        expectedDeleteCount,
                        deleteRangeResponse.getPrevItems().size());
            }
            removeKeysByPrefix(expectedValueByKey, prefix);
            operationTrace.append("step=").append(step).append(", op=delete-range-prefix, prefix=").append(prefix)
                    .append(", deleted=").append(deleteRangeResponse.getDeletedCount()).append('\n');
        }

        for (String prefix : prefixPool) {
            rangePrefixWithModelCheck(prefix, expectedValueByKey, 12000L, seed, totalSteps + 1, operationTrace);
        }
        verifyModelConvergedOnAllNodes(keySpace, expectedValueByKey, 20000L);
    }

    @Test
    public void shouldKeepLinearizableSemanticsUnderRandomPartitionWindowsAndConvergeAfterHeal() throws Exception {
        long seed = 2026051301L;
        StringBuilder operationTrace = new StringBuilder();

        startClusterAndAwaitLeader(5, DEFAULT_BOUNDARY_TIMEOUT_MILLIS);
        List<String> keySpace = buildKeySpace("partition-rnd/key-", 12);
        Map<String, String> expectedValueByKey = new HashMap<>();

        runRandomPartitionWindowScenario(seed,
                96,
                24,
                6,
                null,
                new RandomScenarioStepExecutor() {
                    @Override
                    public void executeStep(int step, Random random, StringBuilder trace) throws Exception {
                        int opCode = random.nextInt(100);
                        if (opCode < 55) {
                            String key = keySpace.get(random.nextInt(keySpace.size()));
                            String value = "pv-" + step + "-" + random.nextInt(10000);
                            boolean committed = executeStepWithLeaderFallback(trace, "put", step, new StepAction() {
                                @Override
                                public void run() throws Exception {
                                    putOnLeaderWithRetry(key, value, 30000L);
                                }
                            });
                            if (committed) {
                                expectedValueByKey.put(key, value);
                                trace.append("step=").append(step).append(", op=put, key=").append(key).append(", value=").append(value).append('\n');
                            }
                            return;
                        }

                        if (opCode < 80) {
                            String key = keySpace.get(random.nextInt(keySpace.size()));
                            boolean committed = executeStepWithLeaderFallback(trace, "delete", step, new StepAction() {
                                @Override
                                public void run() throws Exception {
                                    deleteOnLeaderWithRetry(key, 30000L);
                                }
                            });
                            if (committed) {
                                expectedValueByKey.remove(key);
                                trace.append("step=").append(step).append(", op=delete, key=").append(key).append('\n');
                            }
                            return;
                        }

                        String key = keySpace.get(random.nextInt(keySpace.size()));
                        GetResponse response = executeGetWithLeaderFallback(trace, step, key);
                        if (response != null) {
                            assertEquals("linearizable get mismatch at step=" + step + '\n' + trace,
                                    expectedValueByKey.get(key),
                                    response.getValue());
                            trace.append("step=").append(step).append(", op=get, key=").append(key).append(", value=").append(response.getValue()).append('\n');
                        }
                    }
                },
                operationTrace);

        awaitLeader(20000L);
        verifyModelConvergedOnAllNodes(keySpace, expectedValueByKey, 30000L);
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

    private RangeResponse rangePrefixWithModelCheck(String prefix,
                                                    Map<String, String> expectedValueByKey,
                                                    long timeoutMillis,
                                                    long seed,
                                                    int step,
                                                    StringBuilder operationTrace) throws Exception {
        RangeRequest rangeRequest = new RangeRequest();
        rangeRequest.setStartKey(prefix);
        rangeRequest.setPrefixMatch(true);
        RangeResponse rangeResponse = rangeOnLeaderWithRetry(rangeRequest, timeoutMillis);
        assertNotNull(rangeResponse);

        Set<String> actualKeys = new HashSet<>();
        if (rangeResponse.getItems() != null) {
            for (KeyValueView item : rangeResponse.getItems()) {
                actualKeys.add(item.getKey());
                String expectedValue = expectedValueByKey.get(item.getKey());
                assertEquals("range item value mismatch, seed=" + seed + ", step=" + step + '\n' + operationTrace,
                        expectedValue,
                        item.getValue());
            }
        }

        Set<String> expectedKeys = new HashSet<>();
        for (Map.Entry<String, String> entry : expectedValueByKey.entrySet()) {
            if (entry.getKey().startsWith(prefix)) {
                expectedKeys.add(entry.getKey());
            }
        }

        assertEquals("range keys mismatch, seed=" + seed + ", step=" + step + '\n' + operationTrace,
                expectedKeys,
                actualKeys);
        assertEquals("range count mismatch, seed=" + seed + ", step=" + step + '\n' + operationTrace,
                expectedKeys.size(),
                rangeResponse.getCount());

        RangeRequest countOnlyRequest = new RangeRequest();
        countOnlyRequest.setStartKey(prefix);
        countOnlyRequest.setPrefixMatch(true);
        countOnlyRequest.setCountOnly(true);
        RangeResponse countOnlyResponse = rangeOnLeaderWithRetry(countOnlyRequest, timeoutMillis);
        assertNotNull(countOnlyResponse);
        assertEquals("count-only mismatch, seed=" + seed + ", step=" + step + '\n' + operationTrace,
                expectedKeys.size(),
                countOnlyResponse.getCount());
        return rangeResponse;
    }

    private int countKeysByPrefix(Map<String, String> expectedValueByKey, String prefix) {
        int count = 0;
        for (String key : expectedValueByKey.keySet()) {
            if (key.startsWith(prefix)) {
                count++;
            }
        }
        return count;
    }

    private void removeKeysByPrefix(Map<String, String> expectedValueByKey, String prefix) {
        List<String> toRemoveKeys = new ArrayList<>();
        for (String key : expectedValueByKey.keySet()) {
            if (key.startsWith(prefix)) {
                toRemoveKeys.add(key);
            }
        }
        for (String key : toRemoveKeys) {
            expectedValueByKey.remove(key);
        }
    }

    private interface StepAction {
        void run() throws Exception;
    }

    private boolean executeStepWithLeaderFallback(StringBuilder operationTrace,
                                                  String operationType,
                                                  int step,
                                                  StepAction action) throws Exception {
        try {
            action.run();
            return true;
        } catch (AssertionError assertionError) {
            if (isNoLeaderAssertion(assertionError)) {
                operationTrace.append("step=").append(step).append(", op=").append(operationType)
                        .append(", skipped=no-leader-window").append('\n');
                return false;
            }
            throw assertionError;
        }
    }

    private GetResponse executeGetWithLeaderFallback(StringBuilder operationTrace, int step, String key) throws Exception {
        try {
            GetResponse response = getLinearizableFromLeaderWithRetry(key, 30000L);
            assertNotNull(response);
            return response;
        } catch (AssertionError assertionError) {
            if (isNoLeaderAssertion(assertionError)) {
                operationTrace.append("step=").append(step).append(", op=get, key=").append(key)
                        .append(", skipped=no-leader-window").append('\n');
                return null;
            }
            throw assertionError;
        }
    }

    private boolean isNoLeaderAssertion(AssertionError assertionError) {
        if (assertionError == null || assertionError.getMessage() == null) {
            return false;
        }
        return assertionError.getMessage().contains("leader is not elected");
    }
}
