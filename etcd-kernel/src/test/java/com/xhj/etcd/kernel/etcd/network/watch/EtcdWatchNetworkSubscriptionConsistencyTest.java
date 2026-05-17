package com.xhj.etcd.kernel.etcd.network.watch;

import com.xhj.etcd.kernel.etcd.client.EtcdClient;
import com.xhj.etcd.kernel.etcd.client.watch.WatchHandle;
import com.xhj.etcd.kernel.etcd.client.watch.WatchListener;
import com.xhj.etcd.kernel.etcd.etcdrpc.CompactRequest;
import com.xhj.etcd.kernel.etcd.etcdrpc.DeleteRequest;
import com.xhj.etcd.kernel.etcd.etcdrpc.LeaseGrantRequest;
import com.xhj.etcd.kernel.etcd.etcdrpc.LeaseGrantResponse;
import com.xhj.etcd.kernel.etcd.etcdrpc.PutRequest;
import com.xhj.etcd.kernel.etcd.etcdrpc.TxnCompareCondition;
import com.xhj.etcd.kernel.etcd.etcdrpc.TxnCompareOperatorType;
import com.xhj.etcd.kernel.etcd.etcdrpc.TxnOperationRequest;
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
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * EtcdWatchNetworkSubscriptionConsistencyTest
 *
 * @author XJks
 * @description 基于真实网络的 Watch 长连接测试，覆盖历史回放、实时推送、取消和 compact 取消语义。
 */
public class EtcdWatchNetworkSubscriptionConsistencyTest extends EtcdDistributedTestSkeleton {

    @Test
    public void shouldReplayHistoryAndReceiveIncrementalEventsAcrossCluster() throws Exception {
        String leaderId = startClusterAndAwaitLeader(3, DEFAULT_BOUNDARY_TIMEOUT_MILLIS);

        putOnLeaderWithRetry("watch/network/a", "v1", 12000L);
        putOnLeaderWithRetry("watch/network/b", "v2", 12000L);
        harness.awaitValueReplicated("watch/network/a", "v1", harness.getClusterSize(), 12000L);
        harness.awaitValueReplicated("watch/network/b", "v2", harness.getClusterSize(), 12000L);

        EtcdClient client = new EtcdClient(harness.getTestClient(), buildClientEndpoints(chooseFollowerId(leaderId)));
        RecordingWatchListener listener = new RecordingWatchListener();

        WatchSubscribeRequest subscribeRequest = new WatchSubscribeRequest();
        subscribeRequest.setStartKey("watch/network/");
        subscribeRequest.setPrefixMatch(true);
        subscribeRequest.setStartRevision(1L);
        WatchHandle watchHandle = client.watch(subscribeRequest, listener);

        assertTrue(listener.awaitSubscribed(5000L));
        assertNotNull(listener.getSubscribeResponse());
        assertTrue(listener.getSubscribeResponse().getWatchId() > 0L);

        EtcdTestSupport.awaitTrue(() -> containsWatchEvent(listener.getAllEvents(), "watch/network/a", WatchEventType.PUT)
                        && containsWatchEvent(listener.getAllEvents(), "watch/network/b", WatchEventType.PUT),
                5000L,
                "watch replay events not received");

        client.put(new PutRequest("watch/network/c", "v3"));
        client.delete(new DeleteRequest("watch/network/a"));

        EtcdTestSupport.awaitTrue(() -> containsWatchEvent(listener.getAllEvents(), "watch/network/c", WatchEventType.PUT)
                        && containsWatchEvent(listener.getAllEvents(), "watch/network/a", WatchEventType.DELETE),
                8000L,
                "watch notification incremental events not received");

        watchHandle.cancel();
        assertTrue(listener.awaitCanceled(5000L));
    }

    @Test
    public void shouldStopReceivingEventsAfterCancel() throws Exception {
        String leaderId = startClusterAndAwaitLeader(3, DEFAULT_BOUNDARY_TIMEOUT_MILLIS);

        EtcdClient client = new EtcdClient(harness.getTestClient(), buildClientEndpoints(chooseFollowerId(leaderId)));
        RecordingWatchListener listener = new RecordingWatchListener();

        WatchSubscribeRequest subscribeRequest = new WatchSubscribeRequest();
        subscribeRequest.setStartKey("watch/network/cancel/");
        subscribeRequest.setPrefixMatch(true);
        WatchHandle watchHandle = client.watch(subscribeRequest, listener);

        assertTrue(listener.awaitSubscribed(5000L));

        watchHandle.cancel();
        assertTrue(listener.awaitCanceled(5000L));

        int eventCountBeforePut = listener.getAllEvents().size();
        client.put(new PutRequest("watch/network/cancel/key", "v1"));
        Thread.sleep(1200L);

        assertEquals(eventCountBeforePut, listener.getAllEvents().size());
    }

    @Test
    public void shouldReceiveCompactedCancelEventWhenWatchFallsBehindCompactBoundary() throws Exception {
        String leaderId = startClusterAndAwaitLeader(3, DEFAULT_BOUNDARY_TIMEOUT_MILLIS);

        EtcdClient client = new EtcdClient(harness.getTestClient(), buildClientEndpoints(chooseFollowerId(leaderId)));

        client.put(new PutRequest("watch/network/compact/key", "v1"));
        client.put(new PutRequest("watch/network/compact/key", "v2"));
        client.put(new PutRequest("watch/network/compact/key", "v3"));

        RecordingWatchListener listener = new RecordingWatchListener();
        WatchSubscribeRequest subscribeRequest = new WatchSubscribeRequest();
        subscribeRequest.setStartKey("watch/network/compact/");
        subscribeRequest.setPrefixMatch(true);
        subscribeRequest.setStartRevision(1L);
        subscribeRequest.setMaxEvents(1);
        WatchHandle watchHandle = client.watch(subscribeRequest, listener);

        assertTrue(listener.awaitSubscribed(5000L));
        assertNotNull(listener.getSubscribeResponse());
        assertEquals(1, listener.getSubscribeResponse().getEvents().size());

        CompactRequest compactRequest = new CompactRequest();
        compactRequest.setRevision(3L);
        assertNotNull(client.compact(compactRequest));

        assertTrue(listener.awaitCompacted(8000L));
        assertNotNull(listener.getLatestCompactedResponse());
        assertTrue(listener.getLatestCompactedResponse().isCanceled());
        assertTrue(listener.getLatestCompactedResponse().isCompacted());
        assertEquals(3L, listener.getLatestCompactedResponse().getCompactRevision());

        assertTrue(watchHandle.isClosed());
    }

    @Test
    public void shouldRouteMultipleWatchIdsIndependentlyOnTheSameTcpConnection() throws Exception {
        String leaderId = startClusterAndAwaitLeader(3, DEFAULT_BOUNDARY_TIMEOUT_MILLIS);

        EtcdClient client = new EtcdClient(harness.getTestClient(), buildClientEndpoints(chooseFollowerId(leaderId)));
        RecordingWatchListener leftListener = new RecordingWatchListener();
        RecordingWatchListener rightListener = new RecordingWatchListener();

        WatchSubscribeRequest leftSubscribeRequest = new WatchSubscribeRequest();
        leftSubscribeRequest.setStartKey("watch/network/multi/left/");
        leftSubscribeRequest.setPrefixMatch(true);
        WatchHandle leftHandle = client.watch(leftSubscribeRequest, leftListener);

        WatchSubscribeRequest rightSubscribeRequest = new WatchSubscribeRequest();
        rightSubscribeRequest.setStartKey("watch/network/multi/right/");
        rightSubscribeRequest.setPrefixMatch(true);
        WatchHandle rightHandle = client.watch(rightSubscribeRequest, rightListener);

        assertTrue(leftListener.awaitSubscribed(5000L));
        assertTrue(rightListener.awaitSubscribed(5000L));

        client.put(new PutRequest("watch/network/multi/left/a", "v1"));
        client.put(new PutRequest("watch/network/multi/right/a", "v2"));

        EtcdTestSupport.awaitTrue(() -> containsWatchEvent(leftListener.getAllEvents(), "watch/network/multi/left/a", WatchEventType.PUT)
                        && containsWatchEvent(rightListener.getAllEvents(), "watch/network/multi/right/a", WatchEventType.PUT),
                8000L,
                "multiple watch events not routed independently");

        leftHandle.cancel();
        assertTrue(leftListener.awaitCanceled(5000L));

        int leftEventCountBefore = leftListener.getAllEvents().size();
        int rightEventCountBefore = rightListener.getAllEvents().size();

        client.put(new PutRequest("watch/network/multi/left/b", "v3"));
        client.put(new PutRequest("watch/network/multi/right/b", "v4"));

        EtcdTestSupport.awaitTrue(() -> containsWatchEvent(rightListener.getAllEvents(), "watch/network/multi/right/b", WatchEventType.PUT),
                8000L,
                "right watch did not continue after left cancel");
        Thread.sleep(1000L);
        assertEquals(leftEventCountBefore, leftListener.getAllEvents().size());
        assertTrue(rightListener.getAllEvents().size() > rightEventCountBefore);

        rightHandle.cancel();
        assertTrue(rightListener.awaitCanceled(5000L));
    }

    @Test
    public void shouldCloseAllWatchHandlesBoundToTheSameTcpConnectionWhenEndpointGoesDown() throws Exception {
        String leaderId = startClusterAndAwaitLeader(3, DEFAULT_BOUNDARY_TIMEOUT_MILLIS);
        String followerId = chooseFollowerId(leaderId);

        EtcdClient client = new EtcdClient(harness.getTestClient(), buildClientEndpoints(followerId));
        RecordingWatchListener firstListener = new RecordingWatchListener();
        RecordingWatchListener secondListener = new RecordingWatchListener();

        WatchSubscribeRequest firstRequest = new WatchSubscribeRequest();
        firstRequest.setStartKey("watch/network/same-connection-down/first/");
        firstRequest.setPrefixMatch(true);
        WatchHandle firstHandle = client.watch(firstRequest, firstListener);

        WatchSubscribeRequest secondRequest = new WatchSubscribeRequest();
        secondRequest.setStartKey("watch/network/same-connection-down/second/");
        secondRequest.setPrefixMatch(true);
        WatchHandle secondHandle = client.watch(secondRequest, secondListener);

        assertTrue(firstListener.awaitSubscribed(5000L));
        assertTrue(secondListener.awaitSubscribed(5000L));

        putOnLeaderWithRetry("watch/network/same-connection-down/first/a", "v1", 12000L);
        putOnLeaderWithRetry("watch/network/same-connection-down/second/a", "v2", 12000L);
        EtcdTestSupport.awaitTrue(
                () -> containsWatchEvent(firstListener.getAllEvents(), "watch/network/same-connection-down/first/a", WatchEventType.PUT)
                        && containsWatchEvent(secondListener.getAllEvents(), "watch/network/same-connection-down/second/a", WatchEventType.PUT),
                8000L,
                "both watch flows should receive incremental events before endpoint down");

        // TODO: 两个 watch 都挂在同一个 follower endpoint，底层复用同一个 TCP 连接；连接断开后两条 watch 都应被动关闭。
        harness.stopNode(followerId);

        assertTrue(firstListener.awaitError(8000L));
        assertTrue(secondListener.awaitError(8000L));
        assertTrue(firstHandle.isClosed());
        assertTrue(secondHandle.isClosed());
    }

    @Test
    public void shouldCloseFollowerWatchWhenItsConnectionIsStopped() throws Exception {
        String leaderId = startClusterAndAwaitLeader(3, DEFAULT_BOUNDARY_TIMEOUT_MILLIS);
        String followerId = chooseFollowerId(leaderId);

        EtcdClient client = new EtcdClient(harness.getTestClient(), buildClientEndpoints(followerId));
        RecordingWatchListener listener = new RecordingWatchListener();

        WatchSubscribeRequest subscribeRequest = new WatchSubscribeRequest();
        subscribeRequest.setStartKey("watch/network/close/");
        subscribeRequest.setPrefixMatch(true);
        WatchHandle watchHandle = client.watch(subscribeRequest, listener);

        assertTrue(listener.awaitSubscribed(5000L));

        client.put(new PutRequest("watch/network/close/a", "v1"));
        EtcdTestSupport.awaitTrue(() -> containsWatchEvent(listener.getAllEvents(), "watch/network/close/a", WatchEventType.PUT),
                8000L,
                "watch event before follower stop not received");

        harness.stopNode(followerId);

        assertTrue(listener.awaitError(8000L));
        assertNotNull(listener.getError());
        assertTrue(watchHandle.isClosed());
    }

    @Test
    public void shouldTreatWatchHandleCancelAsIdempotentOnNetworkPath() throws Exception {
        String leaderId = startClusterAndAwaitLeader(3, DEFAULT_BOUNDARY_TIMEOUT_MILLIS);

        EtcdClient client = new EtcdClient(harness.getTestClient(), buildClientEndpoints(chooseFollowerId(leaderId)));
        RecordingWatchListener listener = new RecordingWatchListener();

        WatchSubscribeRequest subscribeRequest = new WatchSubscribeRequest();
        subscribeRequest.setStartKey("watch/network/cancel-idempotent/");
        subscribeRequest.setPrefixMatch(true);
        WatchHandle watchHandle = client.watch(subscribeRequest, listener);

        assertTrue(listener.awaitSubscribed(5000L));

        watchHandle.cancel();
        assertTrue(listener.awaitCanceled(5000L));
        assertTrue(watchHandle.isClosed());

        try {
            watchHandle.cancel();
        } catch (Exception exception) {
            fail("second cancel should be idempotent and must not throw, message=" + exception.getMessage());
        }

        int eventCountBefore = listener.getAllEvents().size();
        client.put(new PutRequest("watch/network/cancel-idempotent/key", "v1"));
        Thread.sleep(1000L);
        assertEquals(eventCountBefore, listener.getAllEvents().size());
    }

    @Test
    public void shouldReplayMissedEventsFromNextRevisionAfterFollowerRestart() throws Exception {
        String leaderId = startClusterAndAwaitLeader(3, DEFAULT_BOUNDARY_TIMEOUT_MILLIS);
        String followerId = chooseFollowerId(leaderId);

        putOnLeaderWithRetry("watch/network/resume/a", "v1", 12000L);
        putOnLeaderWithRetry("watch/network/resume/b", "v2", 12000L);
        harness.awaitValueVisibleOnNode(followerId, "watch/network/resume/a", "v1", 12000L);
        harness.awaitValueVisibleOnNode(followerId, "watch/network/resume/b", "v2", 12000L);

        EtcdClient client = new EtcdClient(harness.getTestClient(), buildClientEndpoints(followerId));
        RecordingWatchListener listener = new RecordingWatchListener();

        WatchSubscribeRequest subscribeRequest = new WatchSubscribeRequest();
        subscribeRequest.setStartKey("watch/network/resume/");
        subscribeRequest.setPrefixMatch(true);
        subscribeRequest.setStartRevision(1L);
        WatchHandle watchHandle = client.watch(subscribeRequest, listener);

        assertTrue(listener.awaitSubscribed(5000L));
        EtcdTestSupport.awaitTrue(() -> containsWatchEvent(listener.getAllEvents(), "watch/network/resume/a", WatchEventType.PUT)
                        && containsWatchEvent(listener.getAllEvents(), "watch/network/resume/b", WatchEventType.PUT),
                8000L,
                "initial replay events not received");

        long resumeRevision = listener.getLatestNextRevision();
        assertTrue("resume revision must be greater than zero", resumeRevision > 0L);

        harness.stopNode(followerId);
        assertTrue(listener.awaitError(8000L));
        assertTrue(watchHandle.isClosed());

        putOnLeaderWithRetry("watch/network/resume/c", "v3", 12000L);
        putOnLeaderWithRetry("watch/network/resume/d", "v4", 12000L);

        harness.restartNode(followerId);
        awaitLeader(15000L);
        harness.awaitValueVisibleOnNode(followerId, "watch/network/resume/d", "v4", 15000L);

        EtcdClient resumedClient = new EtcdClient(harness.getTestClient(), buildClientEndpoints(followerId));
        RecordingWatchListener resumedListener = new RecordingWatchListener();

        WatchSubscribeRequest resumedRequest = new WatchSubscribeRequest();
        resumedRequest.setStartKey("watch/network/resume/");
        resumedRequest.setPrefixMatch(true);
        resumedRequest.setStartRevision(resumeRevision);
        WatchHandle resumedWatchHandle = resumedClient.watch(resumedRequest, resumedListener);

        assertTrue(resumedListener.awaitSubscribed(5000L));
        EtcdTestSupport.awaitTrue(() -> containsWatchEvent(resumedListener.getAllEvents(), "watch/network/resume/c", WatchEventType.PUT)
                        && containsWatchEvent(resumedListener.getAllEvents(), "watch/network/resume/d", WatchEventType.PUT),
                10000L,
                "missed events are not replayed after follower restart");

        resumedWatchHandle.cancel();
        assertTrue(resumedListener.awaitCanceled(5000L));
    }

    @Test
    public void shouldKeepWatchDeliveryStableWhenWritesInterleaveWithCompaction() throws Exception {
        String leaderId = startClusterAndAwaitLeader(3, DEFAULT_BOUNDARY_TIMEOUT_MILLIS);

        EtcdClient client = new EtcdClient(harness.getTestClient(), buildClientEndpoints(chooseFollowerId(leaderId)));
        RecordingWatchListener listener = new RecordingWatchListener();

        WatchSubscribeRequest subscribeRequest = new WatchSubscribeRequest();
        subscribeRequest.setStartKey("watch/network/compact-interleave/");
        subscribeRequest.setPrefixMatch(true);
        WatchHandle watchHandle = client.watch(subscribeRequest, listener);

        assertTrue(listener.awaitSubscribed(5000L));

        long latestCompactedRevision = 0L;
        for (int index = 1; index <= 12; index++) {
            String value = "value-" + index;
            putOnLeaderWithRetry("watch/network/compact-interleave/key", value, 12000L);

            if (index % 3 == 0) {
                long latestRevision = getLinearizableFromLeaderWithRetry("watch/network/compact-interleave/key", 12000L).getRevision();
                long compactRevisionCandidate = Math.max(latestCompactedRevision + 1L, latestRevision - 1L);
                if (compactRevisionCandidate > latestCompactedRevision) {
                    CompactRequest compactRequest = new CompactRequest();
                    compactRequest.setRevision(compactRevisionCandidate);
                    compactOnLeaderWithRetry(compactRequest, 12000L);
                    latestCompactedRevision = compactRevisionCandidate;
                }
            }
        }

        EtcdTestSupport.awaitTrue(() -> containsWatchEvent(listener.getAllEvents(), "watch/network/compact-interleave/key", WatchEventType.PUT)
                        && containsWatchValue(listener.getAllEvents(), "watch/network/compact-interleave/key", "value-12"),
                10000L,
                "watch did not receive the final incremental event under compaction interleave");

        assertFalse(listener.hasCompactedNotification());
        assertFalse(watchHandle.isClosed());

        watchHandle.cancel();
        assertTrue(listener.awaitCanceled(5000L));
    }

    @Test
    public void shouldFanOutManyWatchSubscriptionsOnSingleTcpConnection() throws Exception {
        String leaderId = startClusterAndAwaitLeader(3, DEFAULT_BOUNDARY_TIMEOUT_MILLIS);

        EtcdClient client = new EtcdClient(harness.getTestClient(), buildClientEndpoints(chooseFollowerId(leaderId)));
        List<WatchHandle> watchHandles = new ArrayList<>();
        List<RecordingWatchListener> listeners = new ArrayList<>();

        int watchCount = 20;
        for (int watchIndex = 0; watchIndex < watchCount; watchIndex++) {
            RecordingWatchListener listener = new RecordingWatchListener();
            WatchSubscribeRequest subscribeRequest = new WatchSubscribeRequest();
            subscribeRequest.setStartKey("watch/network/fanout/" + watchIndex + "/");
            subscribeRequest.setPrefixMatch(true);
            watchHandles.add(client.watch(subscribeRequest, listener));
            listeners.add(listener);
        }

        for (RecordingWatchListener listener : listeners) {
            assertTrue(listener.awaitSubscribed(5000L));
        }

        for (int watchIndex = 0; watchIndex < watchCount; watchIndex++) {
            client.put(new PutRequest("watch/network/fanout/" + watchIndex + "/key", "v-" + watchIndex));
        }

        for (int watchIndex = 0; watchIndex < watchCount; watchIndex++) {
            final int currentWatchIndex = watchIndex;
            EtcdTestSupport.awaitTrue(
                    () -> containsWatchEvent(
                            listeners.get(currentWatchIndex).getAllEvents(),
                            "watch/network/fanout/" + currentWatchIndex + "/key",
                            WatchEventType.PUT),
                    10000L,
                    "fanout watch did not receive event, watchIndex=" + currentWatchIndex);
        }

        for (int watchIndex = 0; watchIndex < watchCount; watchIndex++) {
            watchHandles.get(watchIndex).cancel();
            assertTrue("fanout watch should receive cancel callback, watchIndex=" + watchIndex,
                    listeners.get(watchIndex).awaitCanceled(5000L));
        }
    }

    @Test
    public void shouldNotReplayHistoryWhenStartRevisionIsZero() throws Exception {
        String leaderId = startClusterAndAwaitLeader(3, DEFAULT_BOUNDARY_TIMEOUT_MILLIS);

        putOnLeaderWithRetry("watch/network/start-zero/history", "old-value", 12000L);
        harness.awaitValueReplicated("watch/network/start-zero/history", "old-value", harness.getClusterSize(), 12000L);

        EtcdClient client = new EtcdClient(harness.getTestClient(), buildClientEndpoints(chooseFollowerId(leaderId)));
        RecordingWatchListener listener = new RecordingWatchListener();

        WatchSubscribeRequest subscribeRequest = new WatchSubscribeRequest();
        subscribeRequest.setStartKey("watch/network/start-zero/");
        subscribeRequest.setPrefixMatch(true);
        subscribeRequest.setStartRevision(0L);
        WatchHandle watchHandle = client.watch(subscribeRequest, listener);

        assertTrue(listener.awaitSubscribed(5000L));
        Thread.sleep(800L);
        assertFalse(containsWatchEvent(listener.getAllEvents(), "watch/network/start-zero/history", WatchEventType.PUT));

        client.put(new PutRequest("watch/network/start-zero/new", "new-value"));
        EtcdTestSupport.awaitTrue(
                () -> containsWatchEvent(listener.getAllEvents(), "watch/network/start-zero/new", WatchEventType.PUT),
                8000L,
                "watch startRevision=0 should receive only incremental events after subscribe");

        watchHandle.cancel();
        assertTrue(listener.awaitCanceled(5000L));
    }

    @Test
    public void shouldBroadcastSamePrefixEventsToMultipleWatchHandlesOnSingleConnection() throws Exception {
        String leaderId = startClusterAndAwaitLeader(3, DEFAULT_BOUNDARY_TIMEOUT_MILLIS);

        EtcdClient client = new EtcdClient(harness.getTestClient(), buildClientEndpoints(chooseFollowerId(leaderId)));
        RecordingWatchListener firstListener = new RecordingWatchListener();
        RecordingWatchListener secondListener = new RecordingWatchListener();

        WatchSubscribeRequest firstRequest = new WatchSubscribeRequest();
        firstRequest.setStartKey("watch/network/shared-prefix/");
        firstRequest.setPrefixMatch(true);
        WatchHandle firstWatchHandle = client.watch(firstRequest, firstListener);

        WatchSubscribeRequest secondRequest = new WatchSubscribeRequest();
        secondRequest.setStartKey("watch/network/shared-prefix/");
        secondRequest.setPrefixMatch(true);
        WatchHandle secondWatchHandle = client.watch(secondRequest, secondListener);

        assertTrue(firstListener.awaitSubscribed(5000L));
        assertTrue(secondListener.awaitSubscribed(5000L));

        client.put(new PutRequest("watch/network/shared-prefix/key", "v1"));

        EtcdTestSupport.awaitTrue(
                () -> containsWatchEvent(firstListener.getAllEvents(), "watch/network/shared-prefix/key", WatchEventType.PUT)
                        && containsWatchEvent(secondListener.getAllEvents(), "watch/network/shared-prefix/key", WatchEventType.PUT),
                8000L,
                "shared-prefix event should be delivered to all active watch handles");

        firstWatchHandle.cancel();
        secondWatchHandle.cancel();
        assertTrue(firstListener.awaitCanceled(5000L));
        assertTrue(secondListener.awaitCanceled(5000L));
    }

    @Test
    public void shouldCloseWatchStreamWhenClientNotificationHandlerThrowsException() throws Exception {
        String leaderId = startClusterAndAwaitLeader(3, DEFAULT_BOUNDARY_TIMEOUT_MILLIS);

        EtcdClient client = new EtcdClient(harness.getTestClient(), buildClientEndpoints(chooseFollowerId(leaderId)));
        ThrowingWatchListener listener = new ThrowingWatchListener("watch/network/client-fail/trigger");

        WatchSubscribeRequest subscribeRequest = new WatchSubscribeRequest();
        subscribeRequest.setStartKey("watch/network/client-fail/");
        subscribeRequest.setPrefixMatch(true);
        WatchHandle watchHandle = client.watch(subscribeRequest, listener);
        assertTrue(listener.awaitSubscribed(5000L));

        client.put(new PutRequest("watch/network/client-fail/trigger", "v1"));

        assertTrue(listener.awaitError(8000L));
        assertNotNull(listener.getError());
        assertTrue(watchHandle.isClosed());
    }

    @Test
    public void shouldFilterReplayAndIncrementalEventsByExplicitRangeWhenPrefixDisabled() throws Exception {
        String leaderId = startClusterAndAwaitLeader(3, DEFAULT_BOUNDARY_TIMEOUT_MILLIS);

        EtcdClient client = new EtcdClient(harness.getTestClient(), buildClientEndpoints(chooseFollowerId(leaderId)));
        client.put(new PutRequest("watch/network/range/a", "v-a"));
        client.put(new PutRequest("watch/network/range/b", "v-b"));
        client.put(new PutRequest("watch/network/range/c", "v-c"));
        client.put(new PutRequest("watch/network/range/z", "v-z"));
        harness.awaitValueReplicated("watch/network/range/z", "v-z", harness.getClusterSize(), 12000L);

        RecordingWatchListener listener = new RecordingWatchListener();
        WatchSubscribeRequest subscribeRequest = new WatchSubscribeRequest();
        subscribeRequest.setStartKey("watch/network/range/b");
        subscribeRequest.setEndKeyExclusive("watch/network/range/z");
        subscribeRequest.setPrefixMatch(false);
        subscribeRequest.setStartRevision(1L);
        WatchHandle watchHandle = client.watch(subscribeRequest, listener);
        assertTrue(listener.awaitSubscribed(5000L));

        Set<String> replayedKeys = new HashSet<>();
        for (int index = 0; index < listener.getSubscribeResponse().getEvents().size(); index++) {
            replayedKeys.add(listener.getSubscribeResponse().getEvents().get(index).getKeyValue().getKey());
        }
        assertTrue(replayedKeys.contains("watch/network/range/b"));
        assertTrue(replayedKeys.contains("watch/network/range/c"));
        assertFalse(replayedKeys.contains("watch/network/range/a"));
        assertFalse(replayedKeys.contains("watch/network/range/z"));

        client.put(new PutRequest("watch/network/range/y", "v-y"));
        client.put(new PutRequest("watch/network/range/z", "v-z-2"));
        EtcdTestSupport.awaitTrue(
                () -> containsWatchEvent(listener.getAllEvents(), "watch/network/range/y", WatchEventType.PUT),
                8000L,
                "range watch did not receive in-range incremental event");
        Thread.sleep(1000L);
        assertFalse(containsWatchEvent(listener.getAllEvents(), "watch/network/range/z", WatchEventType.PUT));

        watchHandle.cancel();
        assertTrue(listener.awaitCanceled(5000L));
    }

    @Test
    public void shouldKeepWatchStableWhenTxnWritesAndCompactionInterleave() throws Exception {
        String leaderId = startClusterAndAwaitLeader(3, DEFAULT_BOUNDARY_TIMEOUT_MILLIS);

        EtcdClient client = new EtcdClient(harness.getTestClient(), buildClientEndpoints(chooseFollowerId(leaderId)));
        RecordingWatchListener listener = new RecordingWatchListener();

        WatchSubscribeRequest subscribeRequest = new WatchSubscribeRequest();
        subscribeRequest.setStartKey("watch/network/mix/triple/");
        subscribeRequest.setPrefixMatch(true);
        WatchHandle watchHandle = client.watch(subscribeRequest, listener);
        assertTrue(listener.awaitSubscribed(5000L));

        client.put(new PutRequest("watch/network/mix/triple/guard", "on"));

        long latestCompactedRevision = 0L;
        for (int index = 1; index <= 12; index++) {
            // 1) 交替制造 success/failure 分支，确保 txn 写路径都能触发 watch 事件。
            if (index % 4 == 0) {
                client.put(new PutRequest("watch/network/mix/triple/guard", "off"));
            } else if (index % 4 == 1) {
                client.put(new PutRequest("watch/network/mix/triple/guard", "on"));
            }

            TxnRequest txnRequest = new TxnRequest();
            txnRequest.getCompareConditions().add(TxnCompareCondition.value(
                    "watch/network/mix/triple/guard",
                    TxnCompareOperatorType.EQUAL,
                    "on"));
            txnRequest.getSuccessOperations().add(TxnOperationRequest.put(
                    new PutRequest("watch/network/mix/triple/txn-success-" + index, "sv-" + index)));
            txnRequest.getFailureOperations().add(TxnOperationRequest.put(
                    new PutRequest("watch/network/mix/triple/txn-failure-" + index, "fv-" + index)));

            TxnResponse txnResponse = client.txn(txnRequest);
            assertNotNull(txnResponse);

            // 2) 插入普通写请求，验证 txn 与非 txn 写路径交错下 watch 仍然稳定。
            client.put(new PutRequest("watch/network/mix/triple/direct-" + index, "dv-" + index));

            // 3) 周期 compact，验证 watch 在持续追实时不会被误判为 compacted/canceled。
            if (index % 3 == 0) {
                long latestRevision = getLinearizableFromLeaderWithRetry("watch/network/mix/triple/guard", 12000L).getRevision();
                long compactRevisionCandidate = Math.max(latestCompactedRevision + 1L, latestRevision - 1L);
                if (compactRevisionCandidate > latestCompactedRevision) {
                    CompactRequest compactRequest = new CompactRequest();
                    compactRequest.setRevision(compactRevisionCandidate);
                    compactOnLeaderWithRetry(compactRequest, 12000L);
                    latestCompactedRevision = compactRevisionCandidate;
                }
            }
        }

        EtcdTestSupport.awaitTrue(
                () -> containsWatchEvent(listener.getAllEvents(), "watch/network/mix/triple/txn-success-1", WatchEventType.PUT)
                        && containsWatchEvent(listener.getAllEvents(), "watch/network/mix/triple/txn-failure-4", WatchEventType.PUT)
                        && containsWatchEvent(listener.getAllEvents(), "watch/network/mix/triple/direct-12", WatchEventType.PUT),
                12000L,
                "watch should receive txn success/failure and direct put events under compaction interleave");

        assertFalse(listener.hasCompactedNotification());
        assertFalse(watchHandle.isClosed());

        watchHandle.cancel();
        assertTrue(listener.awaitCanceled(5000L));
    }

    @Test
    public void shouldDeliverWatchEventsForTxnSuccessAndFailureBranches() throws Exception {
        String leaderId = startClusterAndAwaitLeader(3, DEFAULT_BOUNDARY_TIMEOUT_MILLIS);

        EtcdClient client = new EtcdClient(harness.getTestClient(), buildClientEndpoints(chooseFollowerId(leaderId)));
        RecordingWatchListener listener = new RecordingWatchListener();

        WatchSubscribeRequest subscribeRequest = new WatchSubscribeRequest();
        subscribeRequest.setStartKey("watch/network/mix/txn/");
        subscribeRequest.setPrefixMatch(true);
        WatchHandle watchHandle = client.watch(subscribeRequest, listener);
        assertTrue(listener.awaitSubscribed(5000L));

        client.put(new PutRequest("watch/network/mix/txn/guard", "allow"));

        TxnRequest successTxnRequest = new TxnRequest();
        successTxnRequest.getCompareConditions().add(TxnCompareCondition.value(
                "watch/network/mix/txn/guard",
                TxnCompareOperatorType.EQUAL,
                "allow"));
        successTxnRequest.getSuccessOperations().add(TxnOperationRequest.put(
                new PutRequest("watch/network/mix/txn/success-key", "success-value")));
        TxnResponse successTxnResponse = client.txn(successTxnRequest);
        assertNotNull(successTxnResponse);
        assertTrue(successTxnResponse.isSucceeded());

        TxnRequest failureTxnRequest = new TxnRequest();
        failureTxnRequest.getCompareConditions().add(TxnCompareCondition.value(
                "watch/network/mix/txn/guard",
                TxnCompareOperatorType.EQUAL,
                "mismatch"));
        failureTxnRequest.getFailureOperations().add(TxnOperationRequest.put(
                new PutRequest("watch/network/mix/txn/failure-key", "failure-value")));
        TxnResponse failureTxnResponse = client.txn(failureTxnRequest);
        assertNotNull(failureTxnResponse);
        assertFalse(failureTxnResponse.isSucceeded());

        EtcdTestSupport.awaitTrue(
                () -> containsWatchEvent(listener.getAllEvents(), "watch/network/mix/txn/success-key", WatchEventType.PUT)
                        && containsWatchEvent(listener.getAllEvents(), "watch/network/mix/txn/failure-key", WatchEventType.PUT),
                10000L,
                "watch should receive put events from txn success/failure branches");

        watchHandle.cancel();
        assertTrue(listener.awaitCanceled(5000L));
    }

    @Test
    public void shouldDeliverDeleteEventWhenLeaseBoundKeyExpires() throws Exception {
        String leaderId = startClusterAndAwaitLeader(3, DEFAULT_BOUNDARY_TIMEOUT_MILLIS);

        EtcdClient client = new EtcdClient(harness.getTestClient(), buildClientEndpoints(chooseFollowerId(leaderId)));
        RecordingWatchListener listener = new RecordingWatchListener();

        WatchSubscribeRequest subscribeRequest = new WatchSubscribeRequest();
        subscribeRequest.setStartKey("watch/network/mix/lease/");
        subscribeRequest.setPrefixMatch(true);
        WatchHandle watchHandle = client.watch(subscribeRequest, listener);
        assertTrue(listener.awaitSubscribed(5000L));

        LeaseGrantResponse leaseGrantResponse = client.leaseGrant(new LeaseGrantRequest(0L, 2L));
        assertNotNull(leaseGrantResponse);
        assertNotNull(leaseGrantResponse.getLease());
        long leaseId = leaseGrantResponse.getLease().getLeaseId();

        PutRequest leasePutRequest = new PutRequest("watch/network/mix/lease/key", "lease-value", leaseId);
        assertNotNull(client.put(leasePutRequest));

        EtcdTestSupport.awaitTrue(
                () -> containsWatchEvent(listener.getAllEvents(), "watch/network/mix/lease/key", WatchEventType.PUT),
                8000L,
                "watch should receive lease-bound put event");

        EtcdTestSupport.awaitTrue(
                () -> containsWatchEvent(listener.getAllEvents(), "watch/network/mix/lease/key", WatchEventType.DELETE),
                15000L,
                "watch should receive delete event after lease expiration");

        if (!watchHandle.isClosed()) {
            watchHandle.cancel();
            assertTrue(listener.awaitCanceled(5000L));
        }
    }

    @Test
    public void shouldRejectLeaderOnlyRequestWhenEndpointIsExplicitlySpecified() throws Exception {
        String leaderId = startClusterAndAwaitLeader(3, DEFAULT_BOUNDARY_TIMEOUT_MILLIS);
        String followerId = chooseFollowerId(leaderId);

        EtcdClient client = new EtcdClient(harness.getTestClient(), buildClientEndpoints(followerId));
        RecordingWatchListener listener = new RecordingWatchListener();

        WatchSubscribeRequest subscribeRequest = new WatchSubscribeRequest();
        subscribeRequest.setStartKey("watch/network/reject/leader-only/");
        subscribeRequest.setPrefixMatch(true);
        subscribeRequest.setLeaderOnly(true);

        try {
            client.watch(subscribeRequest, requireEndpoint(followerId), listener);
            fail("watch(request, endpoint, listener) should reject leaderOnly=true");
        } catch (IllegalArgumentException expected) {
            assertTrue(expected.getMessage().contains("leaderOnly=true"));
        }
    }

    @Test
    public void shouldRedirectToLeaderForDefaultWatchWhenFirstEndpointIsFollower() throws Exception {
        String leaderId = startClusterAndAwaitLeader(3, DEFAULT_BOUNDARY_TIMEOUT_MILLIS);
        String followerId = chooseFollowerId(leaderId);

        EtcdClient client = new EtcdClient(harness.getTestClient(), buildClientEndpoints(followerId));
        RecordingWatchListener listener = new RecordingWatchListener();

        WatchSubscribeRequest subscribeRequest = new WatchSubscribeRequest();
        subscribeRequest.setStartKey("watch/network/default-leader/");
        subscribeRequest.setPrefixMatch(true);
        WatchHandle watchHandle = client.watch(subscribeRequest, listener);

        assertTrue(listener.awaitSubscribed(5000L));
        assertNotNull(client.getCurrentEndpoint());
        assertEquals(requireEndpoint(leaderId).endpointKey(), client.getCurrentEndpoint().endpointKey());

        client.put(new PutRequest("watch/network/default-leader/key", "v1"));
        EtcdTestSupport.awaitTrue(
                () -> containsWatchEvent(listener.getAllEvents(), "watch/network/default-leader/key", WatchEventType.PUT),
                8000L,
                "default watch should keep delivering events after leader redirect");

        watchHandle.cancel();
        assertTrue(listener.awaitCanceled(5000L));
    }

    private List<com.xhj.etcd.rpc.NodeEndpoint> buildClientEndpoints(String firstNodeId) {
        List<com.xhj.etcd.rpc.NodeEndpoint> endpoints = new ArrayList<>();
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

    private boolean containsWatchValue(List<WatchEventView> events, String key, String value) {
        if (events == null) {
            return false;
        }
        for (WatchEventView event : events) {
            if (event == null || event.getKeyValue() == null) {
                continue;
            }
            if (key.equals(event.getKeyValue().getKey()) && value.equals(event.getKeyValue().getValue())) {
                return true;
            }
        }
        return false;
    }

    /**
     * RecordingWatchListener
     *
     * @author XJks
     * @description 记录 watch notification 回调事件，便于断言实时推送行为。
     */
    private static class RecordingWatchListener implements WatchListener {

        private final CountDownLatch subscribedLatch = new CountDownLatch(1);

        private final CountDownLatch canceledLatch = new CountDownLatch(1);

        private final CountDownLatch compactedLatch = new CountDownLatch(1);

        private final CountDownLatch errorLatch = new CountDownLatch(1);

        private final List<WatchEventView> allEvents = new CopyOnWriteArrayList<>();

        private volatile WatchSubscribeResponse subscribeResponse;

        private volatile WatchCancelResponse cancelResponse;

        private volatile WatchNotification latestCompactedResponse;

        private volatile Throwable error;

        private volatile long latestNextRevision;

        @Override
        public void onSubscribed(WatchSubscribeResponse response) {
            this.subscribeResponse = response;
            if (response != null && response.getNextRevision() > 0L) {
                latestNextRevision = response.getNextRevision();
            }
            subscribedLatch.countDown();
        }

        @Override
        public void onNotification(WatchNotification response) {
            if (response != null && response.getEvents() != null) {
                allEvents.addAll(response.getEvents());
            }
            if (response != null && response.getNextRevision() > 0L) {
                latestNextRevision = response.getNextRevision();
            }
            if (response != null && response.isCompacted()) {
                latestCompactedResponse = response;
                compactedLatch.countDown();
            }
            if (response != null && response.isCanceled() && !response.isCompacted()) {
                canceledLatch.countDown();
            }
        }

        @Override
        public void onCanceled(WatchCancelResponse response) {
            this.cancelResponse = response;
            canceledLatch.countDown();
        }

        @Override
        public void onError(Throwable cause) {
            this.error = cause;
            errorLatch.countDown();
        }

        protected boolean awaitSubscribed(long timeoutMillis) throws InterruptedException {
            return subscribedLatch.await(timeoutMillis, TimeUnit.MILLISECONDS);
        }

        private boolean awaitCanceled(long timeoutMillis) throws InterruptedException {
            return canceledLatch.await(timeoutMillis, TimeUnit.MILLISECONDS);
        }

        private boolean awaitCompacted(long timeoutMillis) throws InterruptedException {
            return compactedLatch.await(timeoutMillis, TimeUnit.MILLISECONDS);
        }

        protected boolean awaitError(long timeoutMillis) throws InterruptedException {
            return errorLatch.await(timeoutMillis, TimeUnit.MILLISECONDS);
        }

        private WatchSubscribeResponse getSubscribeResponse() {
            return subscribeResponse;
        }

        private WatchCancelResponse getCancelResponse() {
            return cancelResponse;
        }

        private WatchNotification getLatestCompactedResponse() {
            return latestCompactedResponse;
        }

        private List<WatchEventView> getAllEvents() {
            return allEvents;
        }

        protected Throwable getError() {
            return error;
        }

        private long getLatestNextRevision() {
            return latestNextRevision;
        }

        private boolean hasCompactedNotification() {
            return latestCompactedResponse != null;
        }
    }

    /**
     * ThrowingWatchListener
     *
     * @author XJks
     * @description 命中指定 key 时模拟客户端处理异常，用于验证 watch 流会被关闭。
     */
    private static class ThrowingWatchListener extends RecordingWatchListener {

        private final String failKey;

        private ThrowingWatchListener(String failKey) {
            this.failKey = failKey;
        }

        @Override
        public void onNotification(WatchNotification response) {
            super.onNotification(response);
            if (response == null || response.getEvents() == null) {
                return;
            }
            for (WatchEventView event : response.getEvents()) {
                if (event == null || event.getKeyValue() == null) {
                    continue;
                }
                if (failKey.equals(event.getKeyValue().getKey())) {
                    throw new IllegalStateException("simulate client handler failure");
                }
            }
        }
    }
}
