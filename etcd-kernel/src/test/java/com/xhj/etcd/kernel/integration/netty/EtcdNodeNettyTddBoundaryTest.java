package com.xhj.etcd.kernel.integration.netty;

import com.xhj.etcd.kernel.server.etcdrpc.EtcdRpcResponse;
import com.xhj.etcd.kernel.server.etcdrpc.GetResponse;
import com.xhj.etcd.kernel.server.etcdrpc.PutResponse;
import com.xhj.etcd.rpc.NodeEndpoint;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * EtcdNodeNettyTddBoundaryTest
 *
 * @author XJks
 * @description 基于 MIT6.824 测试思路提炼的 TDD 边界测试，覆盖崩溃恢复、线性一致读和故障切换后的复制追平。
 */
public class EtcdNodeNettyTddBoundaryTest {

    private EtcdNettyClusterTestHarness harness;

    @Before
    public void setUp() {
        harness = new EtcdNettyClusterTestHarness();
    }

    @After
    public void tearDown() {
        if (harness != null) {
            harness.stopAll();
        }
    }

    /**
     * 借鉴 MIT6.824 的持久化恢复模式：
     * 先写入一批已提交数据，再全量崩溃并重启所有节点，最后验证历史数据仍然可读且后续写入仍可复制。
     */
    @Test
    public void shouldRecoverCommittedDataAfterFullClusterCrashAndRestart() throws Exception {
        harness.startCluster(5);
        harness.awaitLeaderElected(12000L);

        putOnLeaderWithRetry("tdd-recover-k1", "v-1", 12000L);
        putOnLeaderWithRetry("tdd-recover-k2", "v-2", 12000L);
        putOnLeaderWithRetry("tdd-recover-k3", "v-3", 12000L);
        harness.awaitValueReplicated("tdd-recover-k1", "v-1", harness.getClusterSize(), 12000L);
        harness.awaitValueReplicated("tdd-recover-k2", "v-2", harness.getClusterSize(), 12000L);
        harness.awaitValueReplicated("tdd-recover-k3", "v-3", harness.getClusterSize(), 12000L);

        List<String> nodeIds = new ArrayList<String>(harness.getNodeIds());
        for (String nodeId : nodeIds) {
            harness.stopNode(nodeId);
        }

        Thread.sleep(1000L);

        for (String nodeId : nodeIds) {
            harness.restartNode(nodeId);
        }
        harness.awaitLeaderElected(15000L);

        harness.awaitValueReplicated("tdd-recover-k1", "v-1", harness.getClusterSize(), 15000L);
        harness.awaitValueReplicated("tdd-recover-k2", "v-2", harness.getClusterSize(), 15000L);
        harness.awaitValueReplicated("tdd-recover-k3", "v-3", harness.getClusterSize(), 15000L);

        putOnLeaderWithRetry("tdd-recover-k4", "v-4", 12000L);
        harness.awaitValueReplicated("tdd-recover-k4", "v-4", harness.getClusterSize(), 15000L);
    }

    /**
     * 借鉴 MIT6.824 的 linearizability 压测思路：
     * 在单 key 上持续递增写入，同时并发线性一致读，并在中途触发 leader 崩溃切换。
     *
     * <p>断言目标：</p>
     * <ol>
     *     <li>每个读线程观察到的 value 不回退（单调不减）。</li>
     *     <li>故障切换后写入仍可继续提交。</li>
     *     <li>最终值可在全节点追平。</li>
     * </ol>
     */
    @Test
    public void shouldKeepLinearizableReadMonotonicUnderLeaderCrashFailover() throws Exception {
        harness.startCluster(5);
        String oldLeaderId = harness.awaitLeaderElected(12000L);

        final String key = "tdd-linearizable-counter";
        putOnLeaderWithRetry(key, formatCounter(0), 8000L);

        ExecutorService pool = Executors.newFixedThreadPool(4);
        AtomicBoolean writerDone = new AtomicBoolean(false);
        try {
            Future<Void> writerFuture = pool.submit(new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    for (int i = 1; i <= 80; i++) {
                        putOnLeaderWithRetry(key, formatCounter(i), 10000L);
                        if (i == 30) {
                            harness.stopNode(oldLeaderId);
                            harness.awaitLeaderElectedExcluding(oldLeaderId, 15000L);
                        }
                        if (i == 45) {
                            harness.restartNode(oldLeaderId);
                        }
                    }
                    writerDone.set(true);
                    return null;
                }
            });

            List<Future<Void>> readerFutures = new ArrayList<Future<Void>>();
            for (int readerIndex = 0; readerIndex < 3; readerIndex++) {
                readerFutures.add(pool.submit(new Callable<Void>() {
                    @Override
                    public Void call() throws Exception {
                        int lastSeen = -1;
                        long deadline = System.currentTimeMillis() + 20000L;
                        while (!writerDone.get() || System.currentTimeMillis() < deadline) {
                            GetResponse response = getLinearizableFromLeaderWithRetry(key, 6000L);
                            assertNotNull(response);
                            if (response.getValue() == null) {
                                continue;
                            }
                            int currentValue = Integer.parseInt(response.getValue());
                            assertTrue("linearizable read should not go backwards, lastSeen=" + lastSeen + ", currentValue=" + currentValue,
                                    currentValue >= lastSeen);
                            lastSeen = currentValue;

                            if (writerDone.get() && currentValue >= 80) {
                                break;
                            }
                        }
                        return null;
                    }
                }));
            }

            writerFuture.get(120, TimeUnit.SECONDS);
            for (Future<Void> readerFuture : readerFutures) {
                readerFuture.get(120, TimeUnit.SECONDS);
            }
        } finally {
            pool.shutdownNow();
            pool.awaitTermination(5, TimeUnit.SECONDS);
        }

        harness.awaitLeaderElected(15000L);
        GetResponse finalResponse = getLinearizableFromLeaderWithRetry(key, 10000L);
        assertNotNull(finalResponse);
        assertEquals(formatCounter(80), finalResponse.getValue());
        harness.awaitValueReplicated(key, formatCounter(80), harness.getClusterSize(), 20000L);
    }

    private void putOnLeaderWithRetry(String key, String value, long timeoutMillis) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        Exception lastException = null;
        while (System.currentTimeMillis() < deadline) {
            try {
                NodeEndpoint leaderEndpoint = harness.awaitLeaderEndpoint(4000L);
                EtcdRpcResponse<PutResponse> response = EtcdNettyTestSupport.callPutByRpc(
                        harness.getTestClient(),
                        leaderEndpoint,
                        key,
                        value);
                if (response != null && response.getHeader() != null && response.getHeader().isSuccess()) {
                    return;
                }
            } catch (Exception e) {
                lastException = e;
            }
            Thread.sleep(100L);
        }
        throw new AssertionError("put retry timeout, key=" + key + ", value=" + value, lastException);
    }

    private GetResponse getLinearizableFromLeaderWithRetry(String key, long timeoutMillis) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        Exception lastException = null;
        while (System.currentTimeMillis() < deadline) {
            try {
                NodeEndpoint leaderEndpoint = harness.awaitLeaderEndpoint(4000L);
                EtcdRpcResponse<GetResponse> response = EtcdNettyTestSupport.callGetByRpc(
                        harness.getTestClient(),
                        leaderEndpoint,
                        key,
                        true);
                if (response != null && response.getHeader() != null && response.getHeader().isSuccess() && response.getBody() != null) {
                    return response.getBody();
                }
            } catch (Exception e) {
                lastException = e;
            }
            Thread.sleep(80L);
        }
        throw new AssertionError("linearizable get retry timeout, key=" + key, lastException);
    }

    private static String formatCounter(int value) {
        return String.format("%06d", value);
    }
}

