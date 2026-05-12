package com.xhj.etcd.kernel.etcd.network.consistency;

import com.xhj.etcd.kernel.etcd.network.support.EtcdDistributedTestSkeleton;
import com.xhj.etcd.kernel.etcd.network.support.EtcdConsistencyAssert;
import com.xhj.etcd.kernel.etcd.network.support.EtcdTestSupport;

import com.xhj.etcd.kernel.etcd.etcdrpc.DeleteRangeRequest;
import com.xhj.etcd.kernel.etcd.etcdrpc.DeleteRangeResponse;
import com.xhj.etcd.kernel.etcd.etcdrpc.DeleteResponse;
import com.xhj.etcd.kernel.etcd.etcdrpc.EtcdRpcResponse;
import com.xhj.etcd.kernel.etcd.etcdrpc.GetResponse;
import com.xhj.etcd.kernel.etcd.etcdrpc.KeyValueView;
import com.xhj.etcd.kernel.etcd.etcdrpc.PutResponse;
import com.xhj.etcd.kernel.etcd.etcdrpc.RangeRequest;
import com.xhj.etcd.kernel.etcd.etcdrpc.RangeResponse;
import com.xhj.etcd.rpc.NodeEndpoint;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * EtcdConsistencyConcurrentOperationTest
 *
 * @author XJks
 * @description Etcd 并发一致性测试，覆盖并发写入与并发混合操作语义。
 */
public class EtcdConsistencyConcurrentOperationTest extends EtcdDistributedTestSkeleton {

    @Test
    public void shouldKeepClusterWritableUnderConcurrentPuts() throws Exception {
        String leaderId = startClusterAndAwaitLeader(5, DEFAULT_ELECTION_TIMEOUT_MILLIS);
        NodeEndpoint leaderEndpoint = requireEndpoint(leaderId);

        int concurrent = 12;
        ExecutorService pool = Executors.newFixedThreadPool(concurrent);
        CountDownLatch startLatch = new CountDownLatch(1);
        List<Future<Boolean>> futures = new ArrayList<>();
        try {
            for (int i = 0; i < concurrent; i++) {
                final int seq = i;
                futures.add(pool.submit(() -> {
                    startLatch.await(3, TimeUnit.SECONDS);
                    String key = "k-concurrent-" + seq;
                    String value = "v-" + seq;
                    EtcdRpcResponse<PutResponse> response = EtcdTestSupport.callPutByRpc(harness.getTestClient(), leaderEndpoint, key, value);
                    return response != null && response.getHeader() != null && response.getHeader().isSuccess();
                }));
            }

            startLatch.countDown();

            int success = 0;
            for (Future<Boolean> future : futures) {
                if (Boolean.TRUE.equals(future.get(10, TimeUnit.SECONDS))) {
                    success++;
                }
            }
            assertEquals(concurrent, success);

            for (int i = 0; i < concurrent; i++) {
                harness.awaitValueReplicated("k-concurrent-" + i, "v-" + i, 8000L);
            }
        } finally {
            pool.shutdownNow();
            assertTrue(pool.awaitTermination(3, TimeUnit.SECONDS));
        }
    }

    @Test
    public void shouldConvergeToModelAfterConcurrentMixedOpsWithRestartInjection() throws Exception {
        startClusterAndAwaitLeader(5, DEFAULT_BOUNDARY_TIMEOUT_MILLIS);

        List<String> keySpace = new ArrayList<>();
        keySpace.addAll(buildKeySpace("mix/a/", 8));
        keySpace.addAll(buildKeySpace("mix/b/", 8));
        keySpace.addAll(buildKeySpace("mix/c/", 8));
        String[] prefixPool = new String[]{"mix/a/", "mix/b/", "mix/c/"};

        AtomicBoolean running = new AtomicBoolean(true);
        Thread faultThread = new Thread(new Runnable() {
            @Override
            public void run() {
                StringBuilder faultTrace = new StringBuilder();
                while (running.get()) {
                    try {
                        // 并发模型校验阶段只注入 follower 重启，避免 leader 切换下“提交成功但客户端超时”带来的不可判定重试结果。
                        injectFault(FaultInjectionType.RESTART_FOLLOWER, faultTrace);
                        Thread.sleep(1000L);
                    } catch (Exception ignore) {
                        // 测试结束或短暂 leader 切换窗口的异常都允许忽略。
                    }
                }
            }
        }, "mixed-op-fault-injector");
        faultThread.setDaemon(true);
        faultThread.start();

        StringBuilder operationTrace = new StringBuilder();
        operationTrace.append("seed=").append(2026051202L).append('\n');
        try {
            runConcurrentWorkers(8, 45, 2026051202L, new ConcurrentWorkerStepExecutor() {
                @Override
                public void executeStep(int workerIndex, int operationIndex, Random random, StringBuilder trace) throws Exception {
                    String key = keySpace.get(random.nextInt(keySpace.size()));
                    int opCode = random.nextInt(100);
                    if (opCode < 42) {
                        String value = "wv-" + workerIndex + "-" + operationIndex + "-" + random.nextInt(10000);
                        putOnLeaderWithRetry(key, value, 12000L);
                        trace.append("worker=").append(workerIndex).append(", op=").append(operationIndex)
                                .append(", type=put, key=").append(key).append(", value=").append(value).append('\n');
                        return;
                    }

                    if (opCode < 62) {
                        DeleteResponse deleteResponse = deleteOnLeaderWithRetry(key, 12000L);
                        trace.append("worker=").append(workerIndex).append(", op=").append(operationIndex)
                                .append(", type=delete, key=").append(key).append(", revision=").append(deleteResponse.getRevision())
                                .append(", deleted=").append(deleteResponse.getDeletedCount()).append('\n');
                        return;
                    }

                    if (opCode < 78) {
                        String prefix = prefixPool[random.nextInt(prefixPool.length)];
                        DeleteRangeRequest deleteRangeRequest = new DeleteRangeRequest();
                        deleteRangeRequest.setStartKey(prefix);
                        deleteRangeRequest.setPrefixMatch(true);
                        deleteRangeRequest.setPrevKv(true);
                        DeleteRangeResponse deleteRangeResponse = deleteRangeOnLeaderWithRetry(deleteRangeRequest, 12000L);
                        trace.append("worker=").append(workerIndex).append(", op=").append(operationIndex)
                                .append(", type=delete-range, prefix=").append(prefix).append(", revision=").append(deleteRangeResponse.getRevision())
                                .append(", deleted=").append(deleteRangeResponse.getDeletedCount()).append('\n');
                        return;
                    }

                    if (opCode < 90) {
                        String prefix = prefixPool[random.nextInt(prefixPool.length)];
                        RangeRequest rangeRequest = new RangeRequest();
                        rangeRequest.setStartKey(prefix);
                        rangeRequest.setPrefixMatch(true);
                        RangeResponse rangeResponse = rangeOnLeaderWithRetry(rangeRequest, 12000L);
                        assertNotNull(rangeResponse);
                        trace.append("worker=").append(workerIndex).append(", op=").append(operationIndex)
                                .append(", type=range-prefix, prefix=").append(prefix).append(", count=").append(rangeResponse.getCount()).append('\n');
                        return;
                    }

                    GetResponse getResponse = getLinearizableFromLeaderWithRetry(key, 12000L);
                    assertNotNull(getResponse);
                    trace.append("worker=").append(workerIndex).append(", op=").append(operationIndex)
                            .append(", type=get, key=").append(key).append(", value=").append(getResponse.getValue()).append('\n');
                }
            }, operationTrace);
        } finally {
            running.set(false);
            faultThread.join(3000L);
        }

        ensureAllNodesRunning();
        awaitLeader(20000L);

        for (String key : keySpace) {
            GetResponse leaderGetResponse = getLinearizableFromLeaderWithRetry(key, 15000L);
            assertNotNull(leaderGetResponse);
            String expectedValue = leaderGetResponse.getValue();
            harness.awaitValueReplicated(key, expectedValue, harness.quorumSize(), 30000L);
            GetResponse actualResponse = getLinearizableFromLeaderWithRetry(key, 15000L);
            EtcdConsistencyAssert.assertGetValueEquals("mixed-final-check key=" + key + '\n' + operationTrace, expectedValue, actualResponse);
        }

        for (String prefix : prefixPool) {
            RangeRequest rangeRequest = new RangeRequest();
            rangeRequest.setStartKey(prefix);
            rangeRequest.setPrefixMatch(true);
            RangeResponse rangeResponse = rangeOnLeaderWithRetry(rangeRequest, 15000L);
            assertNotNull(rangeResponse);

            RangeRequest countOnlyRequest = new RangeRequest();
            countOnlyRequest.setStartKey(prefix);
            countOnlyRequest.setPrefixMatch(true);
            countOnlyRequest.setCountOnly(true);
            RangeResponse countOnlyResponse = rangeOnLeaderWithRetry(countOnlyRequest, 15000L);
            assertNotNull(countOnlyResponse);
            assertEquals("mixed-prefix-count-only mismatch prefix=" + prefix + '\n' + operationTrace,
                    rangeResponse.getCount(),
                    countOnlyResponse.getCount());

            for (KeyValueView item : rangeResponse.getItems()) {
                GetResponse getResponse = getLinearizableFromLeaderWithRetry(item.getKey(), 15000L);
                EtcdConsistencyAssert.assertGetValueEquals("mixed-prefix-item-get mismatch key=" + item.getKey() + '\n' + operationTrace,
                        item.getValue(),
                        getResponse);
            }
        }
    }

    private void ensureAllNodesRunning() throws Exception {
        for (String nodeId : harness.getNodeIds()) {
            if (!harness.isNodeRunning(nodeId)) {
                harness.restartNode(nodeId);
            }
        }
    }

    private List<String> buildKeySpace(String keyPrefix, int keyCount) {
        List<String> keySpace = new ArrayList<>();
        for (int index = 0; index < keyCount; index++) {
            keySpace.add(keyPrefix + index);
        }
        return keySpace;
    }
}
