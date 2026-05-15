package com.xhj.etcd.kernel.etcd.network.lease;

import com.xhj.etcd.kernel.etcd.etcdrpc.EtcdRpcResponse;
import com.xhj.etcd.kernel.etcd.etcdrpc.GetResponse;
import com.xhj.etcd.kernel.etcd.etcdrpc.LeaseGrantRequest;
import com.xhj.etcd.kernel.etcd.etcdrpc.LeaseGrantResponse;
import com.xhj.etcd.kernel.etcd.etcdrpc.LeaseKeepAliveRequest;
import com.xhj.etcd.kernel.etcd.etcdrpc.LeaseListRequest;
import com.xhj.etcd.kernel.etcd.etcdrpc.LeaseListResponse;
import com.xhj.etcd.kernel.etcd.etcdrpc.LeaseRevokeRequest;
import com.xhj.etcd.kernel.etcd.etcdrpc.LeaseRevokeResponse;
import com.xhj.etcd.kernel.etcd.etcdrpc.LeaseTtlRequest;
import com.xhj.etcd.kernel.etcd.etcdrpc.LeaseTtlResponse;
import com.xhj.etcd.kernel.etcd.etcdrpc.PutResponse;
import com.xhj.etcd.kernel.etcd.network.support.EtcdDistributedTestSkeleton;
import com.xhj.etcd.kernel.etcd.network.support.EtcdTestSupport;
import com.xhj.etcd.rpc.NodeEndpoint;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * EtcdLeaseNetworkConsistencyAndRecoveryTest
 *
 * @author XJks
 * @description 基于真实网络的 Lease 测试，覆盖 grant / revoke / expire / restart 恢复场景。
 */
public class EtcdLeaseNetworkConsistencyAndRecoveryTest extends EtcdDistributedTestSkeleton {

    @Test
    public void shouldReplicateLeaseGrantPutAndRevokeAcrossCluster() throws Exception {
        String leaderId = startClusterAndAwaitLeader(3, DEFAULT_BOUNDARY_TIMEOUT_MILLIS);
        NodeEndpoint leaderEndpoint = requireEndpoint(leaderId);

        LeaseGrantResponse leaseGrantResponse = leaseGrantOnLeaderWithRetry(new LeaseGrantRequest(0L, 5L), 12000L);
        assertNotNull(leaseGrantResponse);
        assertNotNull(leaseGrantResponse.getLease());
        long leaseId = leaseGrantResponse.getLease().getLeaseId();

        EtcdRpcResponse<PutResponse> putResponse = EtcdTestSupport.callPutByRpc(
                harness.getTestClient(),
                leaderEndpoint,
                "lease/network/key",
                "network-value",
                leaseId);
        assertNotNull(putResponse);
        assertNotNull(putResponse.getHeader());
        assertTrue(putResponse.getHeader().isSuccess());
        assertNotNull(putResponse.getBody());

        harness.awaitValueReplicated("lease/network/key", "network-value", harness.quorumSize(), 12000L);

        for (String nodeId : harness.getNodeIds()) {
            NodeEndpoint endpoint = requireEndpoint(nodeId);
            EtcdRpcResponse<GetResponse> getResponse = EtcdTestSupport.callGetByRpc(
                    harness.getTestClient(),
                    endpoint,
                    "lease/network/key",
                    false);
            assertNotNull(getResponse);
            assertNotNull(getResponse.getHeader());
            assertTrue(getResponse.getHeader().isSuccess());
            assertNotNull(getResponse.getBody());
            assertEquals("network-value", getResponse.getBody().getValue());
            assertEquals(leaseId, getResponse.getBody().getLeaseId());
        }

        LeaseListResponse leaseListResponse = leaseListOnLeaderWithRetry(new LeaseListRequest(), 12000L);
        assertNotNull(leaseListResponse);
        assertEquals(1, leaseListResponse.getLeases().size());
        assertEquals(leaseId, leaseListResponse.getLeases().get(0).getLeaseId());
        assertEquals("lease/network/key", leaseListResponse.getLeases().get(0).getKeys().get(0));

        LeaseRevokeResponse revokeResponse = leaseRevokeOnLeaderWithRetry(new LeaseRevokeRequest(leaseId), 12000L);
        assertNotNull(revokeResponse);
        assertEquals(leaseId, revokeResponse.getLeaseId());
        assertEquals(1, revokeResponse.getDeletedCount());

        harness.awaitKeyDeleted("lease/network/key", 12000L);

        LeaseListResponse emptyLeaseListResponse = leaseListOnLeaderWithRetry(new LeaseListRequest(), 12000L);
        assertNotNull(emptyLeaseListResponse);
        assertEquals(0, emptyLeaseListResponse.getLeases().size());
    }

    @Test
    public void shouldRestoreLeaseAndBoundKeyAfterFullClusterRestart() throws Exception {
        setSnapshotTriggerLogCount(1);
        String leaderId = startClusterAndAwaitLeader(3, DEFAULT_BOUNDARY_TIMEOUT_MILLIS);
        NodeEndpoint leaderEndpoint = requireEndpoint(leaderId);

        LeaseGrantResponse leaseGrantResponse = leaseGrantOnLeaderWithRetry(new LeaseGrantRequest(0L, 5L), 12000L);
        long leaseId = leaseGrantResponse.getLease().getLeaseId();

        EtcdRpcResponse<PutResponse> putResponse = EtcdTestSupport.callPutByRpc(
                harness.getTestClient(),
                leaderEndpoint,
                "lease/restart/key",
                "restart-value",
                leaseId);
        assertNotNull(putResponse);
        assertTrue(putResponse.getHeader().isSuccess());

        harness.awaitValueReplicated("lease/restart/key", "restart-value", harness.getClusterSize(), 12000L);
        harness.awaitPersistedSnapshotOnNode(leaderId, 12000L);

        List<String> nodeIds = new ArrayList<>(harness.getNodeIds());
        for (String nodeId : nodeIds) {
            harness.stopNode(nodeId);
        }
        Thread.sleep(1000L);
        for (String nodeId : nodeIds) {
            harness.restartNode(nodeId);
        }

        String restartedLeaderId = awaitLeader(15000L);
        NodeEndpoint restartedLeaderEndpoint = requireEndpoint(restartedLeaderId);

        harness.awaitValueReplicated("lease/restart/key", "restart-value", harness.getClusterSize(), 15000L);

        LeaseListResponse leaseListResponse = leaseListOnLeaderWithRetry(new LeaseListRequest(), 12000L);
        assertNotNull(leaseListResponse);
        assertEquals(1, leaseListResponse.getLeases().size());
        assertEquals(leaseId, leaseListResponse.getLeases().get(0).getLeaseId());
        assertEquals("lease/restart/key", leaseListResponse.getLeases().get(0).getKeys().get(0));

        LeaseTtlResponse ttlResponse = leaseTtlOnLeaderWithRetry(new LeaseTtlRequest(leaseId), 12000L);
        assertNotNull(ttlResponse);
        assertNotNull(ttlResponse.getLease());
        assertEquals(leaseId, ttlResponse.getLease().getLeaseId());

        EtcdRpcResponse<GetResponse> getResponse = EtcdTestSupport.callGetByRpc(
                harness.getTestClient(),
                restartedLeaderEndpoint,
                "lease/restart/key",
                false);
        assertNotNull(getResponse);
        assertNotNull(getResponse.getHeader());
        assertTrue(getResponse.getHeader().isSuccess());
        assertNotNull(getResponse.getBody());
        assertEquals("restart-value", getResponse.getBody().getValue());
        assertEquals(leaseId, getResponse.getBody().getLeaseId());
    }

    @Test
    public void shouldKeepAliveLeaseAndDelayAutomaticExpirationAcrossCluster() throws Exception {
        String leaderId = startClusterAndAwaitLeader(3, DEFAULT_BOUNDARY_TIMEOUT_MILLIS);
        NodeEndpoint leaderEndpoint = requireEndpoint(leaderId);

        LeaseGrantResponse leaseGrantResponse = leaseGrantOnLeaderWithRetry(new LeaseGrantRequest(0L, 2L), 12000L);
        assertNotNull(leaseGrantResponse);
        long leaseId = leaseGrantResponse.getLease().getLeaseId();

        EtcdRpcResponse<PutResponse> putResponse = EtcdTestSupport.callPutByRpc(
                harness.getTestClient(),
                leaderEndpoint,
                "lease/keepalive/key",
                "keepalive-value",
                leaseId);
        assertNotNull(putResponse);
        assertTrue(putResponse.getHeader().isSuccess());

        harness.awaitValueReplicated("lease/keepalive/key", "keepalive-value", harness.getClusterSize(), 12000L);

        Thread.sleep(700L);

        LeaseKeepAliveRequest keepAliveRequest = new LeaseKeepAliveRequest(leaseId);
        assertNotNull(leaseKeepAliveOnLeaderWithRetry(keepAliveRequest, 12000L));

        Thread.sleep(1400L);

        for (String nodeId : harness.getNodeIds()) {
            NodeEndpoint endpoint = requireEndpoint(nodeId);
            EtcdRpcResponse<GetResponse> getResponse = EtcdTestSupport.callGetByRpc(
                    harness.getTestClient(),
                    endpoint,
                    "lease/keepalive/key",
                    false);
            assertNotNull(getResponse);
            assertNotNull(getResponse.getHeader());
            assertTrue(getResponse.getHeader().isSuccess());
            assertNotNull(getResponse.getBody());
            assertEquals("keepalive-value", getResponse.getBody().getValue());
            assertEquals(leaseId, getResponse.getBody().getLeaseId());
        }

        harness.awaitKeyDeleted("lease/keepalive/key", 12000L);

        EtcdRpcResponse<LeaseTtlResponse> ttlAfterDeleteResponse = EtcdTestSupport.callLeaseTtlByRpc(
                harness.getTestClient(),
                harness.awaitLeaderEndpoint(4000L),
                new LeaseTtlRequest(leaseId));
        assertNotNull(ttlAfterDeleteResponse);
        assertNotNull(ttlAfterDeleteResponse.getHeader());
        assertFalse(ttlAfterDeleteResponse.getHeader().isSuccess());
    }

    @Test
    public void shouldRemainConsistentUnderConcurrentLeaseGrantAndKeepAliveWorkload() throws Exception {
        startClusterAndAwaitLeader(3, DEFAULT_BOUNDARY_TIMEOUT_MILLIS);

        int concurrentLeaseCount = 12;
        ExecutorService executorService = Executors.newFixedThreadPool(6);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(concurrentLeaseCount);

        List<Throwable> failures = Collections.synchronizedList(new ArrayList<Throwable>());
        Map<Long, String> keyByLeaseId = Collections.synchronizedMap(new HashMap<Long, String>());
        List<String> createdKeys = Collections.synchronizedList(new ArrayList<String>());

        for (int leaseIndex = 0; leaseIndex < concurrentLeaseCount; leaseIndex++) {
            final int currentIndex = leaseIndex;
            executorService.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        startLatch.await();
                        LeaseGrantResponse leaseGrantResponse = leaseGrantOnLeaderWithRetry(new LeaseGrantRequest(0L, 10L), 12000L);
                        long leaseId = leaseGrantResponse.getLease().getLeaseId();
                        String key = "lease/concurrent/key-" + currentIndex;
                        String value = "value-" + currentIndex;

                        putWithLeaseOnLeaderWithRetry(key, value, leaseId, 12000L);
                        leaseKeepAliveOnLeaderWithRetry(new LeaseKeepAliveRequest(leaseId), 12000L);

                        createdKeys.add(key);
                        keyByLeaseId.put(leaseId, key);
                    } catch (Throwable throwable) {
                        failures.add(throwable);
                    } finally {
                        doneLatch.countDown();
                    }
                }
            });
        }

        startLatch.countDown();
        assertTrue(doneLatch.await(60L, TimeUnit.SECONDS));
        executorService.shutdownNow();

        assertEquals(0, failures.size());
        assertEquals(concurrentLeaseCount, createdKeys.size());
        assertEquals(concurrentLeaseCount, keyByLeaseId.size());

        for (int leaseIndex = 0; leaseIndex < concurrentLeaseCount; leaseIndex++) {
            String key = "lease/concurrent/key-" + leaseIndex;
            String value = "value-" + leaseIndex;
            harness.awaitValueReplicated(key, value, harness.getClusterSize(), 15000L);
        }

        LeaseListResponse leaseListResponse = leaseListOnLeaderWithRetry(new LeaseListRequest(), 12000L);
        assertNotNull(leaseListResponse);
        assertEquals(concurrentLeaseCount, leaseListResponse.getLeases().size());
        for (int leaseIndex = 0; leaseIndex < leaseListResponse.getLeases().size(); leaseIndex++) {
            long leaseId = leaseListResponse.getLeases().get(leaseIndex).getLeaseId();
            String expectedKey = keyByLeaseId.get(leaseId);
            assertNotNull(expectedKey);
            assertTrue(leaseListResponse.getLeases().get(leaseIndex).getKeys().contains(expectedKey));
            LeaseTtlResponse leaseTtlResponse = leaseTtlOnLeaderWithRetry(new LeaseTtlRequest(leaseId), 12000L);
            assertNotNull(leaseTtlResponse);
            assertNotNull(leaseTtlResponse.getLease());
            assertTrue(leaseTtlResponse.getLease().getRemainingSeconds() > 0L);
        }
    }

    @Test
    public void shouldKeepLeaseExpirationConsistentWhenLeaderSwitchesNearExpiry() throws Exception {
        String originalLeaderId = startClusterAndAwaitLeader(3, DEFAULT_BOUNDARY_TIMEOUT_MILLIS);
        NodeEndpoint originalLeaderEndpoint = requireEndpoint(originalLeaderId);

        LeaseGrantResponse leaseGrantResponse = leaseGrantOnLeaderWithRetry(new LeaseGrantRequest(0L, 2L), 12000L);
        assertNotNull(leaseGrantResponse);
        long leaseId = leaseGrantResponse.getLease().getLeaseId();

        EtcdRpcResponse<PutResponse> putResponse = EtcdTestSupport.callPutByRpc(
                harness.getTestClient(),
                originalLeaderEndpoint,
                "lease/switch-expire/key",
                "switch-expire-value",
                leaseId);
        assertNotNull(putResponse);
        assertNotNull(putResponse.getHeader());
        assertTrue(putResponse.getHeader().isSuccess());

        harness.awaitValueReplicated("lease/switch-expire/key", "switch-expire-value", harness.getClusterSize(), 12000L);

        Thread.sleep(1200L);
        harness.stopNode(originalLeaderId);

        String newLeaderId = awaitNewLeaderExcluding(originalLeaderId, 12000L);
        assertNotNull(newLeaderId);
        assertFalse(originalLeaderId.equals(newLeaderId));

        harness.awaitKeyDeleted("lease/switch-expire/key", harness.quorumSize(), 15000L);

        EtcdRpcResponse<LeaseTtlResponse> ttlAfterExpireResponse = EtcdTestSupport.callLeaseTtlByRpc(
                harness.getTestClient(),
                harness.awaitLeaderEndpoint(4000L),
                new LeaseTtlRequest(leaseId));
        assertNotNull(ttlAfterExpireResponse);
        assertNotNull(ttlAfterExpireResponse.getHeader());
        assertFalse(ttlAfterExpireResponse.getHeader().isSuccess());

        harness.restartNode(originalLeaderId);
        awaitLeader(15000L);
        harness.awaitKeyDeleted("lease/switch-expire/key", harness.getClusterSize(), 15000L);

        for (int nodeIndex = 0; nodeIndex < harness.getNodeIds().size(); nodeIndex++) {
            String nodeId = harness.getNodeIds().get(nodeIndex);
            NodeEndpoint endpoint = requireEndpoint(nodeId);
            EtcdRpcResponse<GetResponse> getResponse = EtcdTestSupport.callGetByRpc(
                    harness.getTestClient(),
                    endpoint,
                    "lease/switch-expire/key",
                    false);
            assertNotNull(getResponse);
            assertNotNull(getResponse.getHeader());
            assertTrue(getResponse.getHeader().isSuccess());
            assertNotNull(getResponse.getBody());
            assertNull(getResponse.getBody().getValue());
        }
    }

    private void putWithLeaseOnLeaderWithRetry(String key, String value, long leaseId, long timeoutMillis) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        Exception lastException = null;
        while (System.currentTimeMillis() < deadline) {
            try {
                NodeEndpoint leaderEndpoint = harness.awaitLeaderEndpoint(4000L);
                EtcdRpcResponse<PutResponse> putResponse = EtcdTestSupport.callPutByRpc(
                        harness.getTestClient(),
                        leaderEndpoint,
                        key,
                        value,
                        leaseId);
                if (putResponse != null && putResponse.getHeader() != null && putResponse.getHeader().isSuccess()) {
                    return;
                }
            } catch (Exception e) {
                lastException = e;
            }
            Thread.sleep(80L);
        }
        throw new AssertionError("put with lease retry timeout, key=" + key + ", leaseId=" + leaseId, lastException);
    }
}
