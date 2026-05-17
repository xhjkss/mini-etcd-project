package com.xhj.etcd.kernel.etcd.network.client;

import com.xhj.etcd.kernel.etcd.client.EtcdClient;
import com.xhj.etcd.kernel.etcd.client.watch.WatchHandle;
import com.xhj.etcd.kernel.etcd.client.watch.WatchListener;
import com.xhj.etcd.kernel.etcd.etcdrpc.CompactRequest;
import com.xhj.etcd.kernel.etcd.etcdrpc.CompactResponse;
import com.xhj.etcd.kernel.etcd.etcdrpc.KvStateHashRequest;
import com.xhj.etcd.kernel.etcd.etcdrpc.KvStateHashResponse;
import com.xhj.etcd.kernel.etcd.etcdrpc.DeleteRangeRequest;
import com.xhj.etcd.kernel.etcd.etcdrpc.DeleteRangeResponse;
import com.xhj.etcd.kernel.etcd.etcdrpc.DeleteRequest;
import com.xhj.etcd.kernel.etcd.etcdrpc.DeleteResponse;
import com.xhj.etcd.kernel.etcd.etcdrpc.GetRequest;
import com.xhj.etcd.kernel.etcd.etcdrpc.GetResponse;
import com.xhj.etcd.kernel.etcd.etcdrpc.LeaseGrantRequest;
import com.xhj.etcd.kernel.etcd.etcdrpc.LeaseGrantResponse;
import com.xhj.etcd.kernel.etcd.etcdrpc.LeaseKeepAliveRequest;
import com.xhj.etcd.kernel.etcd.etcdrpc.LeaseKeepAliveResponse;
import com.xhj.etcd.kernel.etcd.etcdrpc.LeaseListRequest;
import com.xhj.etcd.kernel.etcd.etcdrpc.LeaseListResponse;
import com.xhj.etcd.kernel.etcd.etcdrpc.LeaseRevokeRequest;
import com.xhj.etcd.kernel.etcd.etcdrpc.LeaseRevokeResponse;
import com.xhj.etcd.kernel.etcd.etcdrpc.LeaseTtlRequest;
import com.xhj.etcd.kernel.etcd.etcdrpc.LeaseTtlResponse;
import com.xhj.etcd.kernel.etcd.etcdrpc.PutRequest;
import com.xhj.etcd.kernel.etcd.etcdrpc.PutResponse;
import com.xhj.etcd.kernel.etcd.etcdrpc.RangeRequest;
import com.xhj.etcd.kernel.etcd.etcdrpc.RangeResponse;
import com.xhj.etcd.kernel.etcd.etcdrpc.NodeStatusRequest;
import com.xhj.etcd.kernel.etcd.etcdrpc.NodeStatusResponse;
import com.xhj.etcd.kernel.etcd.etcdrpc.TxnCompareCondition;
import com.xhj.etcd.kernel.etcd.etcdrpc.TxnCompareOperatorType;
import com.xhj.etcd.kernel.etcd.etcdrpc.TxnOperationRequest;
import com.xhj.etcd.kernel.etcd.etcdrpc.TxnOperationType;
import com.xhj.etcd.kernel.etcd.etcdrpc.TxnRequest;
import com.xhj.etcd.kernel.etcd.etcdrpc.TxnResponse;
import com.xhj.etcd.kernel.etcd.etcdrpc.WatchCancelResponse;
import com.xhj.etcd.kernel.etcd.etcdrpc.WatchSubscribeRequest;
import com.xhj.etcd.kernel.etcd.etcdrpc.WatchSubscribeResponse;
import com.xhj.etcd.kernel.etcd.etcdrpc.WatchEventType;
import com.xhj.etcd.kernel.etcd.etcdrpc.WatchEventView;
import com.xhj.etcd.kernel.etcd.etcdrpc.WatchNotification;
import com.xhj.etcd.kernel.etcd.network.support.EtcdDistributedTestSkeleton;
import com.xhj.etcd.kernel.etcd.network.support.EtcdTestSupport;
import com.xhj.etcd.rpc.NodeEndpoint;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * EtcdClientApiCoverageTest
 *
 * @author XJks
 * @description EtcdClient 端到端覆盖测试，统一验证 KV、Txn、Compact 和 Lease 公共 API。
 */
public class EtcdClientApiCoverageTest extends EtcdDistributedTestSkeleton {

    @Test
    public void shouldCoverKvTxnAndCompactApisThroughClientFacade() throws Exception {
        String leaderId = startClusterAndAwaitLeader(3, DEFAULT_BOUNDARY_TIMEOUT_MILLIS);
        String followerId = chooseFollowerId(leaderId);

        EtcdClient leaderRoutedClient = new EtcdClient(harness.getTestClient(), buildClientEndpoints(followerId));
        EtcdClient followerReadClient = new EtcdClient(harness.getTestClient(), buildClientEndpoints(followerId));

        NodeEndpoint leaderEndpoint = requireEndpoint(leaderId);
        NodeEndpoint followerEndpoint = requireEndpoint(followerId);

        PutResponse putResponse = leaderRoutedClient.put(new PutRequest("client/api/key", "value-1"));
        assertNotNull(putResponse);
        assertTrue(putResponse.getRevision() > 0L);
        assertEquals(leaderEndpoint.endpointKey(), leaderRoutedClient.getCurrentEndpoint().endpointKey());

        harness.awaitValueReplicated("client/api/key", "value-1", harness.quorumSize(), 12000L);

        GetResponse leaderGetResponse = leaderRoutedClient.get(new GetRequest("client/api/key"));
        assertNotNull(leaderGetResponse);
        assertEquals("value-1", leaderGetResponse.getValue());
        assertEquals(putResponse.getRevision(), leaderGetResponse.getRevision());

        GetResponse followerGetResponse = followerReadClient.get(new GetRequest("client/api/key", false));
        assertNotNull(followerGetResponse);
        assertEquals("value-1", followerGetResponse.getValue());
        assertEquals(followerEndpoint.endpointKey(), followerReadClient.getCurrentEndpoint().endpointKey());

        RangeRequest localRangeRequest = new RangeRequest();
        localRangeRequest.setStartKey("client/api/");
        localRangeRequest.setPrefixMatch(true);
        localRangeRequest.setLinearizableRead(false);
        RangeResponse localRangeResponse = followerReadClient.range(localRangeRequest);
        assertNotNull(localRangeResponse);
        assertEquals(1, localRangeResponse.getCount());
        assertEquals("client/api/key", localRangeResponse.getItems().get(0).getKey());
        assertEquals("value-1", localRangeResponse.getItems().get(0).getValue());

        DeleteResponse deleteResponse = leaderRoutedClient.delete(new DeleteRequest("client/api/key"));
        assertNotNull(deleteResponse);
        assertEquals(1, deleteResponse.getDeletedCount());
        assertTrue(deleteResponse.getRevision() > putResponse.getRevision());
        harness.awaitKeyDeleted("client/api/key", 12000L);

        leaderRoutedClient.put(new PutRequest("client/api/range/a", "range-a"));
        leaderRoutedClient.put(new PutRequest("client/api/range/b", "range-b"));
        DeleteRangeRequest deleteRangeRequest = new DeleteRangeRequest();
        deleteRangeRequest.setStartKey("client/api/range/");
        deleteRangeRequest.setPrefixMatch(true);
        deleteRangeRequest.setPrevKv(true);
        DeleteRangeResponse deleteRangeResponse = leaderRoutedClient.deleteRange(deleteRangeRequest);
        assertNotNull(deleteRangeResponse);
        assertEquals(2, deleteRangeResponse.getDeletedCount());
        assertEquals(2, deleteRangeResponse.getPrevItems().size());
        harness.awaitKeyDeleted("client/api/range/a", 12000L);
        harness.awaitKeyDeleted("client/api/range/b", 12000L);

        PutResponse compactKeepPutResponse = leaderRoutedClient.put(new PutRequest("client/api/compact/keep", "compact-value"));
        assertNotNull(compactKeepPutResponse);
        PutResponse compactBumpPutResponse = leaderRoutedClient.put(new PutRequest("client/api/compact/bump", "compact-bump"));
        assertNotNull(compactBumpPutResponse);

        CompactRequest compactRequest = new CompactRequest();
        compactRequest.setRevision(compactBumpPutResponse.getRevision());
        CompactResponse compactResponse = leaderRoutedClient.compact(compactRequest);
        assertNotNull(compactResponse);
        assertEquals(compactBumpPutResponse.getRevision(), compactResponse.getCompactRevision());
        assertEquals(compactBumpPutResponse.getRevision(), compactResponse.getCurrentRevision());

        KvStateHashResponse kvStateHashResponse = leaderRoutedClient.computeKvStateHash(new KvStateHashRequest(0L));
        assertNotNull(kvStateHashResponse);
        assertEquals(2, kvStateHashResponse.getKeyCount());
        assertEquals(compactResponse.getCurrentRevision(), kvStateHashResponse.getRevision());

        NodeStatusResponse nodeStatusResponse = leaderRoutedClient.getNodeStatus(new NodeStatusRequest());
        assertNotNull(nodeStatusResponse);
        assertEquals(leaderId, nodeStatusResponse.getNodeId());
        assertTrue(nodeStatusResponse.getCurrentRevision() >= compactResponse.getCurrentRevision());

        GetResponse compactCurrentGetResponse = leaderRoutedClient.get(new GetRequest("client/api/compact/keep"));
        assertNotNull(compactCurrentGetResponse);
        assertEquals("compact-value", compactCurrentGetResponse.getValue());

        GetResponse compactHistoricalGetResponse = leaderRoutedClient.get(
                new GetRequest("client/api/compact/keep", compactKeepPutResponse.getRevision(), true));
        assertNull(compactHistoricalGetResponse);

        PutResponse txnKeepPutResponse = leaderRoutedClient.put(new PutRequest("client/api/txn/keep", "keep-value"));
        assertNotNull(txnKeepPutResponse);
        leaderRoutedClient.put(new PutRequest("client/api/txn/delete/a", "delete-a"));
        leaderRoutedClient.put(new PutRequest("client/api/txn/delete/b", "delete-b"));
        leaderRoutedClient.put(new PutRequest("client/api/txn/dr/a", "dr-a"));
        leaderRoutedClient.put(new PutRequest("client/api/txn/dr/b", "dr-b"));

        TxnRequest successTxnRequest = new TxnRequest();
        successTxnRequest.getCompareConditions().add(TxnCompareCondition.version(
                "client/api/txn/compare",
                TxnCompareOperatorType.EQUAL,
                0L));
        successTxnRequest.getSuccessOperations().add(TxnOperationRequest.put(new PutRequest("client/api/txn/success", "txn-ok")));
        successTxnRequest.getSuccessOperations().add(TxnOperationRequest.get(new GetRequest("client/api/txn/keep")));
        RangeRequest txnRangeRequest = new RangeRequest();
        txnRangeRequest.setStartKey("client/api/txn/");
        txnRangeRequest.setPrefixMatch(true);
        successTxnRequest.getSuccessOperations().add(TxnOperationRequest.range(txnRangeRequest));
        successTxnRequest.getSuccessOperations().add(TxnOperationRequest.delete(new DeleteRequest("client/api/txn/delete/a")));
        DeleteRangeRequest txnDeleteRangeRequest = new DeleteRangeRequest();
        txnDeleteRangeRequest.setStartKey("client/api/txn/dr/");
        txnDeleteRangeRequest.setPrefixMatch(true);
        txnDeleteRangeRequest.setPrevKv(true);
        successTxnRequest.getSuccessOperations().add(TxnOperationRequest.deleteRange(txnDeleteRangeRequest));

        TxnResponse successTxnResponse = leaderRoutedClient.txn(successTxnRequest);
        assertNotNull(successTxnResponse);
        assertTrue(successTxnResponse.isSucceeded());
        assertEquals(5, successTxnResponse.getResponses().size());
        assertEquals(TxnOperationType.PUT, successTxnResponse.getResponses().get(0).getOperationType());
        assertNotNull(successTxnResponse.getResponses().get(0).dataAs(PutResponse.class));
        assertEquals(TxnOperationType.GET, successTxnResponse.getResponses().get(1).getOperationType());
        assertEquals("keep-value", successTxnResponse.getResponses().get(1).dataAs(GetResponse.class).getValue());
        assertEquals(TxnOperationType.RANGE, successTxnResponse.getResponses().get(2).getOperationType());
        assertTrue(successTxnResponse.getResponses().get(2).dataAs(RangeResponse.class).getCount() >= 6);
        assertEquals(TxnOperationType.DELETE, successTxnResponse.getResponses().get(3).getOperationType());
        assertEquals(1, successTxnResponse.getResponses().get(3).dataAs(DeleteResponse.class).getDeletedCount());
        assertEquals(TxnOperationType.DELETE_RANGE, successTxnResponse.getResponses().get(4).getOperationType());
        assertEquals(2, successTxnResponse.getResponses().get(4).dataAs(DeleteRangeResponse.class).getDeletedCount());

        harness.awaitValueReplicated("client/api/txn/success", "txn-ok", harness.quorumSize(), 12000L);
        harness.awaitKeyDeleted("client/api/txn/delete/a", 12000L);
        harness.awaitKeyDeleted("client/api/txn/dr/a", 12000L);
        harness.awaitKeyDeleted("client/api/txn/dr/b", 12000L);

        TxnRequest failureTxnRequest = new TxnRequest();
        failureTxnRequest.getCompareConditions().add(TxnCompareCondition.version(
                "client/api/txn/keep",
                TxnCompareOperatorType.EQUAL,
                2L));
        failureTxnRequest.getFailureOperations().add(TxnOperationRequest.put(new PutRequest("client/api/txn/failure", "txn-failure")));

        TxnResponse failureTxnResponse = leaderRoutedClient.txn(failureTxnRequest);
        assertNotNull(failureTxnResponse);
        assertFalse(failureTxnResponse.isSucceeded());
        assertEquals(1, failureTxnResponse.getResponses().size());
        assertEquals(TxnOperationType.PUT, failureTxnResponse.getResponses().get(0).getOperationType());
        assertNotNull(failureTxnResponse.getResponses().get(0).dataAs(PutResponse.class));
        harness.awaitValueReplicated("client/api/txn/failure", "txn-failure", harness.quorumSize(), 12000L);
        assertEquals("txn-failure", leaderRoutedClient.get(new GetRequest("client/api/txn/failure")).getValue());
    }

    @Test
    public void shouldCoverLeaseApisThroughClientFacade() throws Exception {
        String leaderId = startClusterAndAwaitLeader(3, DEFAULT_BOUNDARY_TIMEOUT_MILLIS);
        EtcdClient client = new EtcdClient(harness.getTestClient(), buildClientEndpoints(chooseFollowerId(leaderId)));

        LeaseGrantResponse leaseGrantResponse = client.leaseGrant(new LeaseGrantRequest(0L, 5L));
        assertNotNull(leaseGrantResponse);
        assertNotNull(leaseGrantResponse.getLease());
        long leaseId = leaseGrantResponse.getLease().getLeaseId();
        assertTrue(leaseId > 0L);

        PutResponse putResponse = client.put(new PutRequest("client/lease/key", "lease-value", leaseId));
        assertNotNull(putResponse);
        harness.awaitValueReplicated("client/lease/key", "lease-value", harness.quorumSize(), 12000L);

        LeaseKeepAliveResponse keepAliveResponse = client.leaseKeepAlive(new LeaseKeepAliveRequest(leaseId));
        assertNotNull(keepAliveResponse);
        assertNotNull(keepAliveResponse.getLease());
        assertEquals(leaseId, keepAliveResponse.getLease().getLeaseId());

        LeaseTtlResponse ttlResponse = client.leaseTtl(new LeaseTtlRequest(leaseId));
        assertNotNull(ttlResponse);
        assertNotNull(ttlResponse.getLease());
        assertEquals(leaseId, ttlResponse.getLease().getLeaseId());
        assertTrue(ttlResponse.getLease().getRemainingSeconds() > 0L);

        LeaseListResponse leaseListResponse = client.leaseList(new LeaseListRequest());
        assertNotNull(leaseListResponse);
        assertEquals(1, leaseListResponse.getLeases().size());
        assertEquals(leaseId, leaseListResponse.getLeases().get(0).getLeaseId());
        assertTrue(leaseListResponse.getLeases().get(0).getKeys().contains("client/lease/key"));

        LeaseRevokeResponse revokeResponse = client.leaseRevoke(new LeaseRevokeRequest(leaseId));
        assertNotNull(revokeResponse);
        assertEquals(leaseId, revokeResponse.getLeaseId());
        assertEquals(1, revokeResponse.getDeletedCount());

        harness.awaitKeyDeleted("client/lease/key", 12000L);

        LeaseListResponse emptyLeaseListResponse = client.leaseList(new LeaseListRequest());
        assertNotNull(emptyLeaseListResponse);
        assertEquals(0, emptyLeaseListResponse.getLeases().size());
    }

    @Test
    public void shouldCoverWatchApisThroughClientFacade() throws Exception {
        String leaderId = startClusterAndAwaitLeader(3, DEFAULT_BOUNDARY_TIMEOUT_MILLIS);
        EtcdClient client = new EtcdClient(harness.getTestClient(), buildClientEndpoints(chooseFollowerId(leaderId)));

        client.put(new PutRequest("client/watch/key/a", "v1"));
        client.put(new PutRequest("client/watch/key/b", "v2"));
        harness.awaitValueReplicated("client/watch/key/a", "v1", harness.quorumSize(), 12000L);
        harness.awaitValueReplicated("client/watch/key/b", "v2", harness.quorumSize(), 12000L);

        WatchSubscribeRequest subscribeRequest = new WatchSubscribeRequest();
        subscribeRequest.setStartKey("client/watch/key/");
        subscribeRequest.setPrefixMatch(true);
        subscribeRequest.setStartRevision(1L);
        ClientWatchListener listener = new ClientWatchListener();
        WatchHandle streamHandle = client.watch(subscribeRequest, listener);
        assertTrue(listener.awaitSubscribed(5000L));
        WatchSubscribeResponse subscribeResponse = listener.getSubscribeResponse();
        assertNotNull(subscribeResponse);
        assertTrue(subscribeResponse.getWatchId() > 0L);
        assertTrue(subscribeResponse.getEvents().size() >= 2);

        client.put(new PutRequest("client/watch/key/c", "v3"));
        client.delete(new DeleteRequest("client/watch/key/a"));

        EtcdTestSupport.awaitTrue(() -> containsWatchEvent(listener.getEvents(), "client/watch/key/c", WatchEventType.PUT)
                        && containsWatchEvent(listener.getEvents(), "client/watch/key/a", WatchEventType.DELETE),
                8000L,
                "watch notification incremental events not received");

        streamHandle.cancel();
        assertTrue(listener.awaitCanceled(5000L));
        WatchCancelResponse cancelResponse = listener.getCancelResponse();
        assertNotNull(cancelResponse);
        assertTrue(cancelResponse.isCanceled());
        assertTrue(streamHandle.isClosed());
    }

    private List<NodeEndpoint> buildClientEndpoints(String firstNodeId) {
        List<NodeEndpoint> endpoints = new ArrayList<>();
        endpoints.add(requireEndpoint(firstNodeId));
        for (String nodeId : harness.getNodeIds()) {
            if (!firstNodeId.equals(nodeId)) {
                endpoints.add(requireEndpoint(nodeId));
            }
        }
        return endpoints;
    }

    private boolean containsWatchEvent(List<WatchEventView> events, String key, WatchEventType eventType) {
        if (events == null) {
            return false;
        }
        for (WatchEventView event : events) {
            if (event == null || event.getKeyValue() == null) {
                continue;
            }
            if (key.equals(event.getKeyValue().getKey()) && eventType == event.getEventType()) {
                return true;
            }
        }
        return false;
    }

    /**
     * ClientWatchListener
     *
     * @author XJks
     * @description EtcdClient watch API 覆盖测试使用的监听器。
     */
    private static class ClientWatchListener implements WatchListener {

        private final CountDownLatch subscribedLatch = new CountDownLatch(1);

        private final CountDownLatch canceledLatch = new CountDownLatch(1);

        private final List<WatchEventView> events = new CopyOnWriteArrayList<>();

        private volatile WatchSubscribeResponse subscribeResponse;

        private volatile WatchCancelResponse cancelResponse;

        @Override
        public void onSubscribed(WatchSubscribeResponse response) {
            subscribeResponse = response;
            subscribedLatch.countDown();
        }

        @Override
        public void onNotification(WatchNotification response) {
            if (response != null && response.getEvents() != null) {
                events.addAll(response.getEvents());
            }
            if (response != null && response.isCanceled()) {
                canceledLatch.countDown();
            }
        }

        @Override
        public void onCanceled(WatchCancelResponse response) {
            cancelResponse = response;
            canceledLatch.countDown();
        }

        @Override
        public void onError(Throwable cause) {
        }

        private boolean awaitSubscribed(long timeoutMillis) throws InterruptedException {
            return subscribedLatch.await(timeoutMillis, TimeUnit.MILLISECONDS);
        }

        private boolean awaitCanceled(long timeoutMillis) throws InterruptedException {
            return canceledLatch.await(timeoutMillis, TimeUnit.MILLISECONDS);
        }

        private WatchSubscribeResponse getSubscribeResponse() {
            return subscribeResponse;
        }

        private WatchCancelResponse getCancelResponse() {
            return cancelResponse;
        }

        private List<WatchEventView> getEvents() {
            return events;
        }
    }
}
