package com.xhj.etcd.kernel.etcd.network.consistency;

import com.xhj.etcd.kernel.etcd.network.support.EtcdDistributedTestSkeleton;
import com.xhj.etcd.kernel.etcd.network.support.EtcdRpcAssert;
import com.xhj.etcd.kernel.etcd.network.support.EtcdTestSupport;

import com.xhj.etcd.kernel.etcd.etcdrpc.DeleteResponse;
import com.xhj.etcd.kernel.etcd.etcdrpc.EtcdRpcResponse;
import com.xhj.etcd.kernel.etcd.etcdrpc.GetResponse;
import com.xhj.etcd.kernel.etcd.etcdrpc.PutResponse;
import com.xhj.etcd.rpc.NodeEndpoint;
import com.xhj.etcd.rpc.RpcException;
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
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * EtcdConsistencyBoundaryAndFailoverTest
 *
 * @author XJks
 * @description Etcd 一致性边界与故障切换测试，覆盖 failover、majority 边界、线性一致读单调性。
 */
public class EtcdConsistencyBoundaryAndFailoverTest extends EtcdDistributedTestSkeleton {

    @Test
    public void shouldElectNewLeaderAndAcceptWritesAfterOldLeaderStops() throws Exception {
        String oldLeaderId = startClusterAndAwaitLeader(5, DEFAULT_ELECTION_TIMEOUT_MILLIS);
        NodeEndpoint oldLeaderEndpoint = requireEndpoint(oldLeaderId);

        EtcdRpcResponse<PutResponse> beforeStop = EtcdTestSupport.callPutByRpc(harness.getTestClient(), oldLeaderEndpoint, "k-failover-1", "before-stop");
        EtcdRpcAssert.assertSuccess(beforeStop);

        harness.stopNode(oldLeaderId);

        String newLeaderId = awaitNewLeaderExcluding(oldLeaderId, DEFAULT_ELECTION_TIMEOUT_MILLIS);
        assertNotNull(newLeaderId);
        assertNotEquals(oldLeaderId, newLeaderId);

        NodeEndpoint newLeaderEndpoint = requireEndpoint(newLeaderId);
        EtcdRpcResponse<PutResponse> afterStop = EtcdTestSupport.callPutByRpc(harness.getTestClient(), newLeaderEndpoint, "k-failover-2", "after-stop");
        EtcdRpcAssert.assertSuccess(afterStop);

        harness.awaitValueReplicated("k-failover-2", "after-stop", 8000L);
    }

    @Test
    public void shouldCommitWithOneFollowerUnavailableAndCatchUpLateFollowerByLog() throws Exception {
        String leaderId = startClusterAndAwaitLeader(5, DEFAULT_ELECTION_TIMEOUT_MILLIS);
        String unavailableFollowerId = chooseFollowerId(leaderId);

        harness.stopNode(unavailableFollowerId);

        NodeEndpoint leaderEndpoint = requireEndpoint(leaderId);
        EtcdRpcResponse<PutResponse> putResponse = EtcdTestSupport.callPutByRpc(harness.getTestClient(), leaderEndpoint, "k-catchup", "v-catchup");
        assertNotNull(putResponse);
        EtcdRpcAssert.assertSuccess(putResponse);

        EtcdRpcResponse<DeleteResponse> deleteResponse = EtcdTestSupport.callDeleteByRpc(harness.getTestClient(), leaderEndpoint, "k-catchup-temp");
        assertNotNull(deleteResponse);

        harness.awaitValueReplicated("k-catchup", "v-catchup", 8000L);
    }

    @Test
    public void shouldFailWriteWhenLeaderLosesMajorityInFiveNodeCluster() throws Exception {
        String leaderId = startClusterAndAwaitLeader(5, DEFAULT_ELECTION_TIMEOUT_MILLIS);
        stopFollowers(leaderId, 3);

        NodeEndpoint leaderEndpoint = requireEndpoint(leaderId);
        try {
            EtcdRpcResponse<PutResponse> response = EtcdTestSupport.callPutByRpc(harness.getTestClient(), leaderEndpoint, "k-no-majority", "v");
            EtcdRpcAssert.assertFailed(response);
        } catch (RpcException expected) {
            assertNotNull(expected.getMessage());
        }
    }

    @Test
    public void shouldSucceedWriteWhenMajorityIsStillAvailableInFiveNodeCluster() throws Exception {
        String leaderId = startClusterAndAwaitLeader(5, DEFAULT_ELECTION_TIMEOUT_MILLIS);
        stopFollowers(leaderId, 2);

        NodeEndpoint leaderEndpoint = requireEndpoint(leaderId);
        EtcdRpcResponse<PutResponse> response = EtcdTestSupport.callPutByRpc(harness.getTestClient(), leaderEndpoint, "k-still-majority", "ok");
        EtcdRpcAssert.assertSuccess(response);
        harness.awaitValueReplicated("k-still-majority", "ok", 8000L);
    }

    @Test
    public void shouldRecoverWriteAfterRestartingFollowersToRestoreMajority() throws Exception {
        String leaderId = startClusterAndAwaitLeader(5, DEFAULT_ELECTION_TIMEOUT_MILLIS);
        List<String> stoppedFollowerIds = stopFollowers(leaderId, 3);
        assertEquals(3, stoppedFollowerIds.size());

        NodeEndpoint leaderEndpoint = requireEndpoint(leaderId);
        try {
            EtcdTestSupport.callPutByRpc(harness.getTestClient(), leaderEndpoint, "k-recover-majority", "v1");
        } catch (RpcException ignore) {
        }

        harness.restartNode(stoppedFollowerIds.get(0));
        harness.restartNode(stoppedFollowerIds.get(1));

        EtcdTestSupport.awaitTrue(new Callable<Boolean>() {
            @Override
            public Boolean call() {
                try {
                    EtcdRpcResponse<PutResponse> retryResponse = EtcdTestSupport.callPutByRpc(
                            harness.getTestClient(),
                            leaderEndpoint,
                            "k-recover-majority",
                            "v2");
                    return retryResponse != null
                            && retryResponse.getHeader() != null
                            && retryResponse.getHeader().isSuccess();
                } catch (Exception e) {
                    return false;
                }
            }
        }, 10000L, "cluster does not recover majority write");

        harness.awaitValueReplicated("k-recover-majority", "v2", 8000L);
    }

    @Test
    public void shouldKeepLinearizableReadMonotonicUnderLeaderCrashFailover() throws Exception {
        String oldLeaderId = startClusterAndAwaitLeader(5, DEFAULT_BOUNDARY_TIMEOUT_MILLIS);

        final String key = "linearizable-failover-counter";
        putOnLeaderWithRetry(key, formatCounter(0), 8000L);

        ExecutorService pool = Executors.newFixedThreadPool(4);
        AtomicBoolean writerDone = new AtomicBoolean(false);
        try {
            Future<Void> writerFuture = pool.submit(new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    for (int index = 1; index <= 80; index++) {
                        putOnLeaderWithRetry(key, formatCounter(index), 10000L);
                        if (index == 30) {
                            harness.stopNode(oldLeaderId);
                            awaitNewLeaderExcluding(oldLeaderId, 15000L);
                        }
                        if (index == 45) {
                            harness.restartNode(oldLeaderId);
                        }
                    }
                    writerDone.set(true);
                    return null;
                }
            });

            List<Future<Void>> readerFutures = new ArrayList<>();
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

        awaitLeader(15000L);
        GetResponse finalResponse = getLinearizableFromLeaderWithRetry(key, 10000L);
        assertNotNull(finalResponse);
        assertEquals(formatCounter(80), finalResponse.getValue());
        harness.awaitValueReplicated(key, formatCounter(80), harness.getClusterSize(), 20000L);
    }

    private static String formatCounter(int value) {
        return String.format("%06d", value);
    }
}
