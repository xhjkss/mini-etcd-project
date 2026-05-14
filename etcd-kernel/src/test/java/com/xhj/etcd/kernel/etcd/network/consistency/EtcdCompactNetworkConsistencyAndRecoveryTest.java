package com.xhj.etcd.kernel.etcd.network.consistency;

import com.xhj.etcd.kernel.etcd.etcdrpc.CompactRequest;
import com.xhj.etcd.kernel.etcd.etcdrpc.CompactResponse;
import com.xhj.etcd.kernel.etcd.etcdrpc.DeleteResponse;
import com.xhj.etcd.kernel.etcd.etcdrpc.EtcdRpcResponse;
import com.xhj.etcd.kernel.etcd.etcdrpc.GetRequest;
import com.xhj.etcd.kernel.etcd.etcdrpc.GetResponse;
import com.xhj.etcd.kernel.etcd.etcdrpc.PutResponse;
import com.xhj.etcd.kernel.etcd.etcdrpc.PutRequest;
import com.xhj.etcd.kernel.etcd.etcdrpc.RangeRequest;
import com.xhj.etcd.kernel.etcd.etcdrpc.RangeResponse;
import com.xhj.etcd.kernel.etcd.etcdrpc.TxnCompareCondition;
import com.xhj.etcd.kernel.etcd.etcdrpc.TxnCompareOperatorType;
import com.xhj.etcd.kernel.etcd.etcdrpc.TxnOperationRequest;
import com.xhj.etcd.kernel.etcd.etcdrpc.TxnOperationResponse;
import com.xhj.etcd.kernel.etcd.etcdrpc.TxnOperationType;
import com.xhj.etcd.kernel.etcd.etcdrpc.TxnRequest;
import com.xhj.etcd.kernel.etcd.etcdrpc.TxnResponse;
import com.xhj.etcd.kernel.etcd.network.support.EtcdDistributedTestSkeleton;
import com.xhj.etcd.kernel.etcd.network.support.EtcdTestSupport;
import com.xhj.etcd.rpc.NodeEndpoint;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Objects;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * EtcdCompactNetworkConsistencyAndRecoveryTest
 *
 * @author XJks
 * @description Etcd compact 网络一致性与恢复测试。
 */
public class EtcdCompactNetworkConsistencyAndRecoveryTest extends EtcdDistributedTestSkeleton {

    @Test
    public void shouldKeepCompactedBoundaryConsistentAcrossNodes() throws Exception {
        startClusterAndAwaitLeader(5, DEFAULT_BOUNDARY_TIMEOUT_MILLIS);
        NodeEndpoint leaderEndpoint = harness.awaitLeaderEndpoint(12000L);

        EtcdRpcResponse<PutResponse> firstPutResponse = EtcdTestSupport.callPutByRpc(
                harness.getTestClient(),
                leaderEndpoint,
                "compact/network/base",
                "v1");
        EtcdRpcResponse<PutResponse> secondPutResponse = EtcdTestSupport.callPutByRpc(
                harness.getTestClient(),
                leaderEndpoint,
                "compact/network/base",
                "v2");
        EtcdRpcResponse<PutResponse> thirdPutResponse = EtcdTestSupport.callPutByRpc(
                harness.getTestClient(),
                leaderEndpoint,
                "compact/network/base",
                "v3");
        assertTrue(firstPutResponse.getHeader().isSuccess());
        assertTrue(secondPutResponse.getHeader().isSuccess());
        assertTrue(thirdPutResponse.getHeader().isSuccess());

        CompactRequest compactRequest = new CompactRequest();
        compactRequest.setRevision(secondPutResponse.getBody().getRevision());
        CompactResponse compactResponse = compactOnLeaderWithRetry(compactRequest, 15000L);
        assertNotNull(compactResponse);
        assertEquals(secondPutResponse.getBody().getRevision(), compactResponse.getCompactRevision());
        assertEquals(thirdPutResponse.getBody().getRevision(), compactResponse.getCurrentRevision());

        harness.awaitValueReplicated("compact/network/base", "v3", harness.getClusterSize(), 15000L);

        assertCompactedHistoricalReadRejectedOnAllNodes(
                "compact/network/base",
                firstPutResponse.getBody().getRevision(),
                12000L);
    }

    @Test
    public void shouldKeepCompactAvailableAfterLeaderFailover() throws Exception {
        String oldLeaderId = startClusterAndAwaitLeader(5, DEFAULT_BOUNDARY_TIMEOUT_MILLIS);
        NodeEndpoint oldLeaderEndpoint = requireEndpoint(oldLeaderId);

        EtcdRpcResponse<PutResponse> firstPutResponse = EtcdTestSupport.callPutByRpc(
                harness.getTestClient(),
                oldLeaderEndpoint,
                "compact/failover/key",
                "v1");
        EtcdRpcResponse<PutResponse> secondPutResponse = EtcdTestSupport.callPutByRpc(
                harness.getTestClient(),
                oldLeaderEndpoint,
                "compact/failover/key",
                "v2");
        assertTrue(firstPutResponse.getHeader().isSuccess());
        assertTrue(secondPutResponse.getHeader().isSuccess());

        CompactRequest firstCompactRequest = new CompactRequest();
        firstCompactRequest.setRevision(secondPutResponse.getBody().getRevision());
        CompactResponse firstCompactResponse = compactOnLeaderWithRetry(firstCompactRequest, 15000L);
        assertNotNull(firstCompactResponse);
        assertEquals(secondPutResponse.getBody().getRevision(), firstCompactResponse.getCompactRevision());

        harness.stopNode(oldLeaderId);
        String newLeaderId = awaitNewLeaderExcluding(oldLeaderId, 15000L);
        NodeEndpoint newLeaderEndpoint = requireEndpoint(newLeaderId);

        EtcdRpcResponse<PutResponse> thirdPutResponse = EtcdTestSupport.callPutByRpc(
                harness.getTestClient(),
                newLeaderEndpoint,
                "compact/failover/key",
                "v3");
        assertTrue(thirdPutResponse.getHeader().isSuccess());

        CompactRequest secondCompactRequest = new CompactRequest();
        secondCompactRequest.setRevision(thirdPutResponse.getBody().getRevision());
        CompactResponse secondCompactResponse = compactOnLeaderWithRetry(secondCompactRequest, 15000L);
        assertNotNull(secondCompactResponse);
        assertEquals(thirdPutResponse.getBody().getRevision(), secondCompactResponse.getCompactRevision());

        harness.restartNode(oldLeaderId);
        harness.awaitValueVisibleOnNode(oldLeaderId, "compact/failover/key", "v3", 15000L);

        EtcdRpcResponse<GetResponse> historyResponse = EtcdTestSupport.callGetByRpc(
                harness.getTestClient(),
                requireEndpoint(oldLeaderId),
                "compact/failover/key",
                firstPutResponse.getBody().getRevision(),
                false);
        assertNotNull(historyResponse);
        assertNotNull(historyResponse.getHeader());
        assertFalse(historyResponse.getHeader().isSuccess());
        assertTrue(historyResponse.getHeader().getMessage().contains("compacted"));
    }

    @Test
    public void shouldRestoreCompactBoundaryAfterFullClusterRestart() throws Exception {
        startClusterAndAwaitLeader(5, DEFAULT_BOUNDARY_TIMEOUT_MILLIS);
        NodeEndpoint leaderEndpoint = harness.awaitLeaderEndpoint(12000L);

        EtcdRpcResponse<PutResponse> firstPutResponse = EtcdTestSupport.callPutByRpc(
                harness.getTestClient(),
                leaderEndpoint,
                "compact/restart/key",
                "v1");
        EtcdRpcResponse<PutResponse> secondPutResponse = EtcdTestSupport.callPutByRpc(
                harness.getTestClient(),
                leaderEndpoint,
                "compact/restart/key",
                "v2");
        assertTrue(firstPutResponse.getHeader().isSuccess());
        assertTrue(secondPutResponse.getHeader().isSuccess());

        CompactRequest compactRequest = new CompactRequest();
        compactRequest.setRevision(secondPutResponse.getBody().getRevision());
        CompactResponse compactResponse = compactOnLeaderWithRetry(compactRequest, 15000L);
        assertNotNull(compactResponse);

        List<String> nodeIdList = new ArrayList<>(harness.getNodeIds());
        for (String nodeId : nodeIdList) {
            harness.stopNode(nodeId);
        }
        Thread.sleep(1000L);
        for (String nodeId : nodeIdList) {
            harness.restartNode(nodeId);
        }
        awaitLeader(15000L);
        harness.awaitValueReplicated("compact/restart/key", "v2", harness.getClusterSize(), 15000L);

        assertCompactedHistoricalReadRejectedOnAllNodes(
                "compact/restart/key",
                firstPutResponse.getBody().getRevision(),
                15000L);
    }

    @Test
    public void shouldKeepLatestWritesVisibleWhenCompactInterleavesWithWrites() throws Exception {
        startClusterAndAwaitLeader(5, DEFAULT_BOUNDARY_TIMEOUT_MILLIS);
        NodeEndpoint leaderEndpoint = harness.awaitLeaderEndpoint(12000L);

        long compactRevision = 0L;
        long firstRevision = 0L;
        for (int index = 1; index <= 6; index++) {
            EtcdRpcResponse<PutResponse> putResponse = EtcdTestSupport.callPutByRpc(
                    harness.getTestClient(),
                    leaderEndpoint,
                    "compact/interleave/key",
                    "v" + index);
            assertTrue(putResponse.getHeader().isSuccess());
            if (index == 1) {
                firstRevision = putResponse.getBody().getRevision();
            }
            if (index == 3) {
                compactRevision = putResponse.getBody().getRevision();
            }
        }

        CompactRequest compactRequest = new CompactRequest();
        compactRequest.setRevision(compactRevision);
        CompactResponse compactResponse = compactOnLeaderWithRetry(compactRequest, 15000L);
        assertNotNull(compactResponse);
        assertEquals(compactRevision, compactResponse.getCompactRevision());

        for (int index = 7; index <= 10; index++) {
            putOnLeaderWithRetry("compact/interleave/key", "v" + index, 12000L);
        }
        harness.awaitValueReplicated("compact/interleave/key", "v10", harness.getClusterSize(), 15000L);

        EtcdRpcResponse<GetResponse> historyResponse = EtcdTestSupport.callGetByRpc(
                harness.getTestClient(),
                leaderEndpoint,
                "compact/interleave/key",
                firstRevision,
                false);
        assertNotNull(historyResponse);
        assertNotNull(historyResponse.getHeader());
        assertFalse(historyResponse.getHeader().isSuccess());
        assertTrue(historyResponse.getHeader().getMessage().contains("compacted"));
    }

    @Test
    public void shouldKeepSingleKeyLatestValueStableUnderHighFrequencyCompaction() throws Exception {
        startClusterAndAwaitLeader(5, DEFAULT_BOUNDARY_TIMEOUT_MILLIS);

        Random random = new Random(20260514L);
        String key = "compact/hot/key";
        String currentValue = "v0";
        String latestBoundaryValue = currentValue;
        long compactRevision = 0L;

        PutResponse seedResponse = putOnLeaderWithRetryAndResponse(key, currentValue, 12000L);
        assertNotNull(seedResponse);
        long firstRevision = seedResponse.getRevision();

        for (int step = 1; step <= 30; step++) {
            String nextValue = "v" + step;
            PutResponse putResponse = putOnLeaderWithRetryAndResponse(key, nextValue, 12000L);
            assertNotNull(putResponse);
            currentValue = nextValue;
            assertEquals(currentValue, getLinearizableFromLeaderWithRetry(key, 12000L).getValue());

            if (step % 2 == 0) {
                CompactRequest compactRequest = new CompactRequest();
                compactRequest.setRevision(putResponse.getRevision());
                CompactResponse compactResponse = compactOnLeaderWithRetry(compactRequest, 12000L);
                assertNotNull(compactResponse);
                assertEquals(putResponse.getRevision(), compactResponse.getCompactRevision());
                compactRevision = compactResponse.getCompactRevision();
                latestBoundaryValue = currentValue;
                assertBoundaryReadVisibleOnLeader(harness.awaitLeaderEndpoint(12000L), key, compactRevision, latestBoundaryValue, 12000L);
            }

            if (step % 5 == 0) {
                restartRandomFollower(random);
            }
        }

        harness.awaitValueReplicated(key, currentValue, harness.getClusterSize(), 15000L);
        assertCompactedHistoricalReadRejectedOnAllNodes(key, firstRevision, 12000L);
        if (compactRevision > 0L) {
            assertBoundaryReadVisibleOnLeader(harness.awaitLeaderEndpoint(12000L), key, compactRevision, latestBoundaryValue, 12000L);
        }
    }

    @Test
    public void shouldKeepCompactAndTxnInterleavedSemanticsStable() throws Exception {
        startClusterAndAwaitLeader(5, DEFAULT_BOUNDARY_TIMEOUT_MILLIS);

        Random random = new Random(20260514L);
        String mainKey = "compact/txn/main";
        String auditKey = "compact/txn/audit";
        String currentMainValue = "main-v0";
        String currentAuditValue = null;
        String latestBoundaryValue = currentMainValue;
        long compactRevision = 0L;

        List<Long> committedRevisionList = new ArrayList<>();
        Map<Long, String> mainValueByRevision = new HashMap<>();

        PutResponse seedResponse = putOnLeaderWithRetryAndResponse(mainKey, currentMainValue, 12000L);
        assertNotNull(seedResponse);
        committedRevisionList.add(seedResponse.getRevision());
        mainValueByRevision.put(seedResponse.getRevision(), currentMainValue);

        for (int step = 1; step <= 36; step++) {
            if (step % 3 == 1) {
                String nextMainValue = "main-v" + step;
                TxnRequest txnRequest = new TxnRequest();
                txnRequest.getCompareConditions().add(TxnCompareCondition.value(
                        mainKey,
                        TxnCompareOperatorType.EQUAL,
                        currentMainValue));
                txnRequest.getSuccessOperations().add(TxnOperationRequest.put(new PutRequest(mainKey, nextMainValue)));
                txnRequest.getSuccessOperations().add(TxnOperationRequest.get(new GetRequest(mainKey, false)));

                TxnResponse txnResponse = txnOnLeaderWithRetry(txnRequest, 12000L);
                assertNotNull(txnResponse);
                assertTrue(txnResponse.isSucceeded());
                assertEquals(2, txnResponse.getResponses().size());
                assertEquals(TxnOperationType.PUT, txnResponse.getResponses().get(0).getOperationType());
                assertEquals(TxnOperationType.GET, txnResponse.getResponses().get(1).getOperationType());
                GetResponse readBackResponse = txnResponse.getResponses().get(1).dataAs(GetResponse.class);
                assertNotNull(readBackResponse);
                assertEquals(nextMainValue, readBackResponse.getValue());

                currentMainValue = nextMainValue;
                committedRevisionList.add(txnResponse.getRevision());
                mainValueByRevision.put(txnResponse.getRevision(), currentMainValue);
                assertEquals(currentMainValue, getLinearizableFromLeaderWithRetry(mainKey, 12000L).getValue());
            } else if (step % 3 == 2) {
                String nextAuditValue = "audit-v" + step;
                TxnRequest txnRequest = new TxnRequest();
                txnRequest.getCompareConditions().add(TxnCompareCondition.value(
                        mainKey,
                        TxnCompareOperatorType.EQUAL,
                        "__mismatch__" + step));
                txnRequest.getFailureOperations().add(TxnOperationRequest.put(new PutRequest(auditKey, nextAuditValue)));
                txnRequest.getFailureOperations().add(TxnOperationRequest.get(new GetRequest(auditKey, false)));

                TxnResponse txnResponse = txnOnLeaderWithRetry(txnRequest, 12000L);
                assertNotNull(txnResponse);
                assertFalse(txnResponse.isSucceeded());
                assertEquals(2, txnResponse.getResponses().size());
                assertEquals(TxnOperationType.PUT, txnResponse.getResponses().get(0).getOperationType());
                assertEquals(TxnOperationType.GET, txnResponse.getResponses().get(1).getOperationType());
                GetResponse readBackResponse = txnResponse.getResponses().get(1).dataAs(GetResponse.class);
                assertNotNull(readBackResponse);
                assertEquals(nextAuditValue, readBackResponse.getValue());

                currentAuditValue = nextAuditValue;
                committedRevisionList.add(txnResponse.getRevision());
                mainValueByRevision.put(txnResponse.getRevision(), currentMainValue);
                assertEquals(currentAuditValue, getLinearizableFromLeaderWithRetry(auditKey, 12000L).getValue());
            } else {
                if (!committedRevisionList.isEmpty()) {
                    long latestCommittedRevision = committedRevisionList.get(committedRevisionList.size() - 1);
                    if (latestCommittedRevision > compactRevision) {
                        CompactRequest compactRequest = new CompactRequest();
                        compactRequest.setRevision(latestCommittedRevision);
                        CompactResponse compactResponse = compactOnLeaderWithRetry(compactRequest, 12000L);
                        assertNotNull(compactResponse);
                        assertEquals(latestCommittedRevision, compactResponse.getCompactRevision());
                        compactRevision = compactResponse.getCompactRevision();
                        latestBoundaryValue = mainValueByRevision.get(compactRevision);
                        assertBoundaryReadVisibleOnLeader(harness.awaitLeaderEndpoint(12000L), mainKey, compactRevision, latestBoundaryValue, 12000L);
                    }
                }

                if (step % 12 == 0) {
                    restartRandomFollower(random);
                }
            }
        }

        harness.awaitValueReplicated(mainKey, currentMainValue, harness.getClusterSize(), 15000L);
        if (currentAuditValue != null) {
            harness.awaitValueReplicated(auditKey, currentAuditValue, harness.getClusterSize(), 15000L);
        }

        assertCompactedHistoricalReadRejectedOnAllNodes(mainKey, committedRevisionList.get(0), 12000L);
        if (compactRevision > 0L) {
            assertBoundaryReadVisibleOnLeader(harness.awaitLeaderEndpoint(12000L), mainKey, compactRevision, latestBoundaryValue, 12000L);
        }
    }

    @Test
    public void shouldRejectInvalidCompactRequestsAndKeepClusterReadable() throws Exception {
        startClusterAndAwaitLeader(5, DEFAULT_BOUNDARY_TIMEOUT_MILLIS);
        NodeEndpoint leaderEndpoint = harness.awaitLeaderEndpoint(12000L);

        EtcdRpcResponse<PutResponse> putResponse = EtcdTestSupport.callPutByRpc(
                harness.getTestClient(),
                leaderEndpoint,
                "compact/network/invalid",
                "v1");
        assertTrue(putResponse.getHeader().isSuccess());

        CompactRequest nonPositiveRequest = new CompactRequest();
        nonPositiveRequest.setRevision(0L);
        EtcdRpcResponse<CompactResponse> nonPositiveResponse = EtcdTestSupport.callCompactByRpc(
                harness.getTestClient(),
                leaderEndpoint,
                nonPositiveRequest);
        assertNotNull(nonPositiveResponse);
        assertNotNull(nonPositiveResponse.getHeader());
        assertFalse(nonPositiveResponse.getHeader().isSuccess());

        CompactRequest futureRevisionRequest = new CompactRequest();
        futureRevisionRequest.setRevision(putResponse.getBody().getRevision() + 100L);
        EtcdRpcResponse<CompactResponse> futureRevisionResponse = EtcdTestSupport.callCompactByRpc(
                harness.getTestClient(),
                leaderEndpoint,
                futureRevisionRequest);
        assertNotNull(futureRevisionResponse);
        assertNotNull(futureRevisionResponse.getHeader());
        assertFalse(futureRevisionResponse.getHeader().isSuccess());

        harness.awaitValueReplicated("compact/network/invalid", "v1", harness.getClusterSize(), 15000L);
        for (String nodeId : harness.getNodeIds()) {
            EtcdRpcResponse<GetResponse> getResponse = EtcdTestSupport.callGetByRpc(
                    harness.getTestClient(),
                    requireEndpoint(nodeId),
                    "compact/network/invalid",
                    false);
            assertNotNull(getResponse);
            assertNotNull(getResponse.getHeader());
            assertTrue(getResponse.getHeader().isSuccess());
            assertNotNull(getResponse.getBody());
            assertEquals("v1", getResponse.getBody().getValue());
        }
    }

    @Test
    public void shouldRejectAlreadyCompactedRevisionRequestOnLeader() throws Exception {
        startClusterAndAwaitLeader(5, DEFAULT_BOUNDARY_TIMEOUT_MILLIS);
        NodeEndpoint leaderEndpoint = harness.awaitLeaderEndpoint(12000L);

        EtcdRpcResponse<PutResponse> putResponse = EtcdTestSupport.callPutByRpc(
                harness.getTestClient(),
                leaderEndpoint,
                "compact/network/duplicate",
                "v1");
        assertTrue(putResponse.getHeader().isSuccess());

        CompactRequest compactRequest = new CompactRequest();
        compactRequest.setRevision(putResponse.getBody().getRevision());
        EtcdRpcResponse<CompactResponse> firstCompactResponse = EtcdTestSupport.callCompactByRpc(
                harness.getTestClient(),
                leaderEndpoint,
                compactRequest);
        assertNotNull(firstCompactResponse);
        assertNotNull(firstCompactResponse.getHeader());
        assertTrue(firstCompactResponse.getHeader().isSuccess());

        EtcdRpcResponse<CompactResponse> secondCompactResponse = EtcdTestSupport.callCompactByRpc(
                harness.getTestClient(),
                leaderEndpoint,
                compactRequest);
        assertNotNull(secondCompactResponse);
        assertNotNull(secondCompactResponse.getHeader());
        assertFalse(secondCompactResponse.getHeader().isSuccess());
        assertTrue(secondCompactResponse.getHeader().getMessage().contains("compacted"));
    }

    @Test
    public void shouldRejectCompactedHistoricalRangeReadsAcrossNodes() throws Exception {
        startClusterAndAwaitLeader(5, DEFAULT_BOUNDARY_TIMEOUT_MILLIS);
        NodeEndpoint leaderEndpoint = harness.awaitLeaderEndpoint(12000L);

        EtcdRpcResponse<PutResponse> firstPutResponse = EtcdTestSupport.callPutByRpc(
                harness.getTestClient(),
                leaderEndpoint,
                "compact/network/range/a",
                "v1");
        EtcdRpcResponse<PutResponse> secondPutResponse = EtcdTestSupport.callPutByRpc(
                harness.getTestClient(),
                leaderEndpoint,
                "compact/network/range/b",
                "v2");
        assertTrue(firstPutResponse.getHeader().isSuccess());
        assertTrue(secondPutResponse.getHeader().isSuccess());

        CompactRequest compactRequest = new CompactRequest();
        compactRequest.setRevision(secondPutResponse.getBody().getRevision());
        CompactResponse compactResponse = compactOnLeaderWithRetry(compactRequest, 15000L);
        assertNotNull(compactResponse);

        harness.awaitValueReplicated("compact/network/range/b", "v2", harness.getClusterSize(), 15000L);

        assertCompactedHistoricalRangeReadRejectedOnLeader(
                leaderEndpoint,
                "compact/network/range/",
                firstPutResponse.getBody().getRevision(),
                12000L);
    }

    @Test
    public void shouldAllowReadingAtCompactRevisionBoundaryAcrossNodes() throws Exception {
        startClusterAndAwaitLeader(5, DEFAULT_BOUNDARY_TIMEOUT_MILLIS);
        NodeEndpoint leaderEndpoint = harness.awaitLeaderEndpoint(12000L);

        EtcdRpcResponse<PutResponse> firstPutResponse = EtcdTestSupport.callPutByRpc(
                harness.getTestClient(),
                leaderEndpoint,
                "compact/network/boundary",
                "v1");
        EtcdRpcResponse<PutResponse> secondPutResponse = EtcdTestSupport.callPutByRpc(
                harness.getTestClient(),
                leaderEndpoint,
                "compact/network/boundary",
                "v2");
        EtcdRpcResponse<PutResponse> thirdPutResponse = EtcdTestSupport.callPutByRpc(
                harness.getTestClient(),
                leaderEndpoint,
                "compact/network/boundary",
                "v3");
        assertTrue(firstPutResponse.getHeader().isSuccess());
        assertTrue(secondPutResponse.getHeader().isSuccess());
        assertTrue(thirdPutResponse.getHeader().isSuccess());

        CompactRequest compactRequest = new CompactRequest();
        compactRequest.setRevision(secondPutResponse.getBody().getRevision());
        CompactResponse compactResponse = compactOnLeaderWithRetry(compactRequest, 15000L);
        assertNotNull(compactResponse);
        assertEquals(secondPutResponse.getBody().getRevision(), compactResponse.getCompactRevision());

        harness.awaitValueReplicated("compact/network/boundary", "v3", harness.getClusterSize(), 15000L);
        assertCompactedHistoricalReadRejectedOnAllNodes(
                "compact/network/boundary",
                firstPutResponse.getBody().getRevision(),
                12000L);

        for (String nodeId : harness.getNodeIds()) {
            EtcdRpcResponse<GetResponse> boundaryReadResponse = EtcdTestSupport.callGetByRpc(
                    harness.getTestClient(),
                    requireEndpoint(nodeId),
                    "compact/network/boundary",
                    secondPutResponse.getBody().getRevision(),
                    false);
            assertNotNull(boundaryReadResponse);
            assertNotNull(boundaryReadResponse.getHeader());
            assertTrue(boundaryReadResponse.getHeader().isSuccess());
            assertNotNull(boundaryReadResponse.getBody());
            assertEquals("v2", boundaryReadResponse.getBody().getValue());
            assertNull(boundaryReadResponse.getHeader().getMessage());
        }
    }

    @Test
    public void shouldKeepCompactBoundaryAndLatestValuesConsistentUnderRandomOperationsAndRestartInjection() throws Exception {
        startClusterAndAwaitLeader(5, DEFAULT_BOUNDARY_TIMEOUT_MILLIS);

        long randomSeed = 20260514L;
        Random random = new Random(randomSeed);
        List<String> keyList = new ArrayList<>();
        keyList.add("compact/random/k1");
        keyList.add("compact/random/k2");
        keyList.add("compact/random/k3");

        Map<String, String> expectedValueByKey = new HashMap<>();
        List<Long> committedRevisionList = new ArrayList<>();
        long compactRevision = 0L;

        for (int step = 1; step <= 48; step++) {
            String selectedKey = keyList.get(random.nextInt(keyList.size()));
            int operationType = random.nextInt(100);

            if (operationType < 50) {
                String value = "value-" + step + "-" + random.nextInt(1000);
                PutResponse putResponse = putOnLeaderWithRetryAndResponse(selectedKey, value, 12000L);
                expectedValueByKey.put(selectedKey, value);
                committedRevisionList.add(putResponse.getRevision());
            } else if (operationType < 75) {
                DeleteResponse deleteResponse = deleteOnLeaderWithRetry(selectedKey, 12000L);
                if (deleteResponse.getDeletedCount() > 0) {
                    expectedValueByKey.put(selectedKey, null);
                    committedRevisionList.add(deleteResponse.getRevision());
                }
            } else {
                if (!committedRevisionList.isEmpty()) {
                    long selectedRevision = committedRevisionList.get(random.nextInt(committedRevisionList.size()));
                    if (selectedRevision > compactRevision) {
                        CompactRequest compactRequest = new CompactRequest();
                        compactRequest.setRevision(selectedRevision);
                        CompactResponse compactResponse = compactOnLeaderWithRetry(compactRequest, 12000L);
                        compactRevision = compactResponse.getCompactRevision();
                    }
                }
            }

            if (step % 12 == 0) {
                restartRandomFollower(random);
            }
            if (step % 24 == 0) {
                restartAllNodesAndAwaitRecovery();
            }

            assertExpectedValuesOnLeader(expectedValueByKey, keyList, randomSeed, step);
        }

        assertExpectedValuesOnAllNodes(expectedValueByKey, keyList, 15000L);
        if (compactRevision > 1L) {
            assertCompactedHistoricalReadRejectedOnAllNodes(
                    keyList.get(0),
                    compactRevision - 1L,
                    12000L);
        }
    }

    private void assertCompactedHistoricalReadRejectedOnAllNodes(final String key,
                                                                 final long compactedRevision,
                                                                 final long timeoutMillis) throws Exception {
        EtcdTestSupport.awaitTrue(
                () -> {
                    for (String nodeId : harness.getNodeIds()) {
                        EtcdRpcResponse<GetResponse> historyResponse = EtcdTestSupport.callGetByRpc(
                                harness.getTestClient(),
                                requireEndpoint(nodeId),
                                key,
                                compactedRevision,
                                false);
                        if (historyResponse == null
                                || historyResponse.getHeader() == null
                                || historyResponse.getHeader().isSuccess()
                                || historyResponse.getHeader().getMessage() == null
                                || !historyResponse.getHeader().getMessage().contains("compacted")) {
                            return false;
                        }
                    }
                    return true;
                },
                timeoutMillis,
                "compacted historical read is not rejected on all nodes, key=" + key + ", revision=" + compactedRevision);

        for (String nodeId : harness.getNodeIds()) {
            EtcdRpcResponse<GetResponse> historyResponse = EtcdTestSupport.callGetByRpc(
                    harness.getTestClient(),
                    requireEndpoint(nodeId),
                    key,
                    compactedRevision,
                    false);
            assertNotNull(historyResponse);
            assertNotNull(historyResponse.getHeader());
            assertFalse(historyResponse.getHeader().isSuccess());
            assertTrue(historyResponse.getHeader().getMessage().contains("compacted"));
        }
    }

    private void assertBoundaryReadVisibleOnLeader(final NodeEndpoint leaderEndpoint,
                                                   final String key,
                                                   final long revision,
                                                   final String expectedValue,
                                                   final long timeoutMillis) throws Exception {
        EtcdTestSupport.awaitTrue(
                () -> {
                    EtcdRpcResponse<GetResponse> response = EtcdTestSupport.callGetByRpc(
                            harness.getTestClient(),
                            leaderEndpoint,
                            key,
                            revision,
                            false);
                    return response != null
                            && response.getHeader() != null
                            && response.getHeader().isSuccess()
                            && response.getBody() != null
                            && Objects.equals(expectedValue, response.getBody().getValue());
                },
                timeoutMillis,
                "boundary read is not visible on leader, key=" + key + ", revision=" + revision);
    }

    private void assertCompactedHistoricalRangeReadRejectedOnLeader(final NodeEndpoint leaderEndpoint,
                                                                    final String startKey,
                                                                    final long compactedRevision,
                                                                    final long timeoutMillis) throws Exception {
        EtcdTestSupport.awaitTrue(
                () -> {
                    RangeRequest rangeRequest = new RangeRequest();
                    rangeRequest.setStartKey(startKey);
                    rangeRequest.setPrefixMatch(true);
                    rangeRequest.setLinearizableRead(false);
                    rangeRequest.setRevision(compactedRevision);
                    EtcdRpcResponse<RangeResponse> rangeResponse = EtcdTestSupport.callRangeByRpc(
                            harness.getTestClient(),
                            leaderEndpoint,
                            rangeRequest);
                    return rangeResponse != null
                            && rangeResponse.getHeader() != null
                            && !rangeResponse.getHeader().isSuccess()
                            && rangeResponse.getHeader().getMessage() != null
                            && rangeResponse.getHeader().getMessage().contains("compacted");
                },
                timeoutMillis,
                "compacted historical range read is not rejected on leader, startKey=" + startKey + ", revision=" + compactedRevision);
    }

    private PutResponse putOnLeaderWithRetryAndResponse(String key,
                                                        String value,
                                                        long timeoutMillis) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        Exception lastException = null;
        while (System.currentTimeMillis() < deadline) {
            try {
                NodeEndpoint leaderEndpoint = harness.awaitLeaderEndpoint(4000L);
                EtcdRpcResponse<PutResponse> response = EtcdTestSupport.callPutByRpc(
                        harness.getTestClient(),
                        leaderEndpoint,
                        key,
                        value);
                if (response != null && response.getHeader() != null && response.getHeader().isSuccess() && response.getBody() != null) {
                    return response.getBody();
                }
            } catch (Exception e) {
                lastException = e;
            }
            Thread.sleep(80L);
        }
        throw new AssertionError("put retry timeout, key=" + key + ", value=" + value, lastException);
    }

    private void restartRandomFollower(Random random) throws Exception {
        String leaderId = awaitLeader(10000L);
        List<String> followerNodeIdList = chooseFollowerIds(leaderId, harness.getClusterSize() - 1);
        if (followerNodeIdList.isEmpty()) {
            return;
        }
        String selectedFollowerId = followerNodeIdList.get(random.nextInt(followerNodeIdList.size()));
        harness.stopNode(selectedFollowerId);
        Thread.sleep(200L);
        harness.restartNode(selectedFollowerId);
        awaitLeader(12000L);
    }

    private void restartAllNodesAndAwaitRecovery() throws Exception {
        List<String> nodeIdList = new ArrayList<>(harness.getNodeIds());
        for (String nodeId : nodeIdList) {
            harness.stopNode(nodeId);
        }
        Thread.sleep(600L);
        for (String nodeId : nodeIdList) {
            harness.restartNode(nodeId);
        }
        awaitLeader(15000L);
    }

    private void assertExpectedValuesOnLeader(Map<String, String> expectedValueByKey,
                                              List<String> keyList,
                                              long randomSeed,
                                              int step) throws Exception {
        for (String key : keyList) {
            GetResponse getResponse = getLinearizableFromLeaderWithRetry(key, 12000L);
            String expectedValue = expectedValueByKey.get(key);
            assertEquals(
                    "leader value mismatch, key=" + key + ", seed=" + randomSeed + ", step=" + step,
                    expectedValue,
                    getResponse.getValue());
        }
    }

    private void assertExpectedValuesOnAllNodes(Map<String, String> expectedValueByKey,
                                                List<String> keyList,
                                                long timeoutMillis) throws Exception {
        EtcdTestSupport.awaitTrue(
                () -> {
                    for (String nodeId : harness.getNodeIds()) {
                        for (String key : keyList) {
                            EtcdRpcResponse<GetResponse> getResponse = EtcdTestSupport.callGetByRpc(
                                    harness.getTestClient(),
                                    requireEndpoint(nodeId),
                                    key,
                                    false);
                            if (getResponse == null
                                    || getResponse.getHeader() == null
                                    || !getResponse.getHeader().isSuccess()
                                    || getResponse.getBody() == null) {
                                return false;
                            }
                            String expectedValue = expectedValueByKey.get(key);
                            if (!java.util.Objects.equals(expectedValue, getResponse.getBody().getValue())) {
                                return false;
                            }
                        }
                    }
                    return true;
                },
                timeoutMillis,
                "expected values are not converged on all nodes");
    }
}
