package com.xhj.etcd.kernel.integration.netty;

import com.xhj.etcd.kernel.server.etcdrpc.EtcdRpcResponse;
import com.xhj.etcd.kernel.server.etcdrpc.PutResponse;
import com.xhj.etcd.rpc.NodeEndpoint;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * EtcdNodeNettyConcurrentClientsTest
 *
 * @author XJks
 * @description EtcdNode 多客户端并发写入场景测试。
 */
public class EtcdNodeNettyConcurrentClientsTest {

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

    @Test
    public void shouldKeepClusterWritableUnderConcurrentPuts() throws Exception {
        harness.startCluster(5);

        String leaderId = harness.awaitLeaderElected(10000L);
        NodeEndpoint leaderEndpoint = harness.getEndpoint(leaderId);

        int concurrent = 12;
        ExecutorService pool = Executors.newFixedThreadPool(concurrent);
        CountDownLatch startLatch = new CountDownLatch(1);
        List<Future<Boolean>> futures = new ArrayList<Future<Boolean>>();
        try {
            for (int i = 0; i < concurrent; i++) {
                final int seq = i;
                futures.add(pool.submit(() -> {
                    startLatch.await(3, TimeUnit.SECONDS);
                    String key = "k-concurrent-" + seq;
                    String value = "v-" + seq;
                    EtcdRpcResponse<PutResponse> response = EtcdNettyTestSupport.callPutByRpc(harness.getTestClient(), leaderEndpoint, key, value);
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
}

