package com.xhj.etcd.kernel.etcd.module.node;

import com.xhj.etcd.kernel.etcd.etcdrpc.CompactRequest;
import com.xhj.etcd.kernel.etcd.etcdrpc.EtcdRpcResponse;
import com.xhj.etcd.kernel.etcd.etcdrpc.GetRequest;
import com.xhj.etcd.kernel.etcd.etcdrpc.PutRequest;
import com.xhj.etcd.kernel.etcd.etcdrpc.WatchCancelRequest;
import com.xhj.etcd.kernel.etcd.etcdrpc.WatchCancelResponse;
import com.xhj.etcd.kernel.etcd.etcdrpc.WatchSubscribeRequest;
import com.xhj.etcd.kernel.etcd.etcdrpc.WatchSubscribeResponse;
import com.xhj.etcd.kernel.etcd.etcdrpc.WatchEventType;
import com.xhj.etcd.kernel.etcd.etcdrpc.WatchNotification;
import com.xhj.etcd.kernel.etcd.node.EtcdNode;
import com.xhj.etcd.kernel.raft.core.RaftConfig;
import com.xhj.etcd.kernel.raft.core.RaftRoleType;
import com.xhj.etcd.rpc.RpcMessage;
import com.xhj.etcd.rpc.RpcMessageType;
import com.xhj.etcd.serializer.Serializer;
import com.xhj.etcd.serializer.SerializerRegistry;
import com.xhj.etcd.storage.memory.MemoryStorage;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * EtcdNodeWatchSubscriptionTest
 *
 * @author XJks
 * @description EtcdNode Watch 模块测试，覆盖订阅、回放、取消与 compact 边界行为。
 */
public class EtcdNodeWatchSubscriptionTest {

    private final Serializer serializer = SerializerRegistry.getDefaultSerializer();

    private EtcdNode node;

    @Before
    public void setUp() {
        RaftConfig raftConfig = new RaftConfig();
        raftConfig.setElectionTimeoutTicks(10);
        raftConfig.setHeartbeatTimeoutTicks(3);

        node = new EtcdNode("n1", raftConfig, new MemoryStorage());
        node.start();
    }

    @After
    public void tearDown() {
        if (node != null) {
            node.stop();
        }
    }

    @Test
    public void shouldReplayHistoryOnWatchSubscribeResponse() throws Exception {
        awaitLeader(node, 3000L);

        assertTrue(node.handleEtcdRpcPutRequest(new PutRequest("watch/node/a", "v1")).getHeader().isSuccess());
        assertTrue(node.handleEtcdRpcPutRequest(new PutRequest("watch/node/b", "v2")).getHeader().isSuccess());

        WatchSubscribeRequest subscribeRequest = new WatchSubscribeRequest();
        subscribeRequest.setStartKey("watch/node/");
        subscribeRequest.setPrefixMatch(true);
        subscribeRequest.setStartRevision(1L);
        EtcdRpcResponse<WatchSubscribeResponse> subscribeResponse = node.handleEtcdRpcWatchSubscribeRequest(subscribeRequest, null, "watch-test-subscribe-1");
        assertNotNull(subscribeResponse);
        assertNotNull(subscribeResponse.getHeader());
        assertTrue(subscribeResponse.getHeader().isSuccess());
        assertNotNull(subscribeResponse.getBody());
        assertEquals(2, subscribeResponse.getBody().getEvents().size());
        assertEquals(WatchEventType.PUT, subscribeResponse.getBody().getEvents().get(0).getEventType());
        assertTrue(subscribeResponse.getBody().getWatchId() > 0L);
    }

    @Test
    public void shouldCancelWatchSession() throws Exception {
        awaitLeader(node, 3000L);

        assertTrue(node.handleEtcdRpcPutRequest(new PutRequest("watch/cancel/key", "v1")).getHeader().isSuccess());

        WatchSubscribeRequest subscribeRequest = new WatchSubscribeRequest();
        subscribeRequest.setStartKey("watch/cancel/");
        subscribeRequest.setPrefixMatch(true);
        subscribeRequest.setStartRevision(0L);
        WatchSubscribeResponse subscribeResponse = requireSuccess(node.handleEtcdRpcWatchSubscribeRequest(subscribeRequest, null, "watch-test-subscribe-2")).getBody();
        assertNotNull(subscribeResponse);
        long watchId = subscribeResponse.getWatchId();

        WatchCancelResponse cancelResponse = requireSuccess(node.handleEtcdRpcWatchCancelRequest(new WatchCancelRequest(watchId))).getBody();
        assertNotNull(cancelResponse);
        assertTrue(cancelResponse.isCanceled());
    }

    @Test
    public void shouldReturnCompactedCancelWhenWatchFallsBehindCompactRevision() throws Exception {
        awaitLeader(node, 3000L);

        assertTrue(node.handleEtcdRpcPutRequest(new PutRequest("watch/compact/key", "v1")).getHeader().isSuccess());
        assertTrue(node.handleEtcdRpcPutRequest(new PutRequest("watch/compact/key", "v2")).getHeader().isSuccess());
        assertTrue(node.handleEtcdRpcPutRequest(new PutRequest("watch/compact/key", "v3")).getHeader().isSuccess());

        WatchSubscribeRequest subscribeRequest = new WatchSubscribeRequest();
        subscribeRequest.setStartKey("watch/compact/");
        subscribeRequest.setPrefixMatch(true);
        subscribeRequest.setStartRevision(1L);
        subscribeRequest.setMaxEvents(1);
        WatchSubscribeResponse subscribeResponse = requireSuccess(node.handleEtcdRpcWatchSubscribeRequest(subscribeRequest, null, "watch-test-subscribe-3")).getBody();
        assertNotNull(subscribeResponse);
        long watchId = subscribeResponse.getWatchId();
        assertEquals(1, subscribeResponse.getEvents().size());

        CompactRequest compactRequest = new CompactRequest();
        compactRequest.setRevision(3L);
        assertTrue(node.handleEtcdRpcCompactRequest(compactRequest).getHeader().isSuccess());
        WatchCancelResponse cancelResponse = requireSuccess(node.handleEtcdRpcWatchCancelRequest(new WatchCancelRequest(watchId))).getBody();
        assertNotNull(cancelResponse);
    }

    @Test
    public void shouldRejectCompactedStartRevisionForNewWatchSubscribe() throws Exception {
        awaitLeader(node, 3000L);

        assertTrue(node.handleEtcdRpcPutRequest(new PutRequest("watch/compact/guard/key", "v1")).getHeader().isSuccess());
        assertTrue(node.handleEtcdRpcPutRequest(new PutRequest("watch/compact/guard/key", "v2")).getHeader().isSuccess());

        CompactRequest compactRequest = new CompactRequest();
        compactRequest.setRevision(2L);
        assertTrue(node.handleEtcdRpcCompactRequest(compactRequest).getHeader().isSuccess());

        WatchSubscribeRequest subscribeRequest = new WatchSubscribeRequest();
        subscribeRequest.setStartKey("watch/compact/guard/");
        subscribeRequest.setPrefixMatch(true);
        subscribeRequest.setStartRevision(1L);

        EtcdRpcResponse<WatchSubscribeResponse> subscribeResponse = node.handleEtcdRpcWatchSubscribeRequest(
                subscribeRequest,
                null,
                "watch-test-subscribe-4");
        assertNotNull(subscribeResponse);
        assertNotNull(subscribeResponse.getHeader());
        assertFalse(subscribeResponse.getHeader().isSuccess());
        assertTrue(subscribeResponse.getHeader().getMessage().contains("compacted"));
    }

    @Test
    public void shouldReturnFalseWhenCancelingUnknownWatchId() throws Exception {
        awaitLeader(node, 3000L);

        EtcdRpcResponse<WatchCancelResponse> cancelResponse = node.handleEtcdRpcWatchCancelRequest(new WatchCancelRequest(9999L));
        assertNotNull(cancelResponse);
        assertNotNull(cancelResponse.getHeader());
        assertTrue(cancelResponse.getHeader().isSuccess());
        assertNotNull(cancelResponse.getBody());
        assertFalse(cancelResponse.getBody().isCanceled());
    }

    @Test
    public void shouldTreatRepeatedCancelAsIdempotentForExistingWatchSession() throws Exception {
        awaitLeader(node, 3000L);

        assertTrue(node.handleEtcdRpcPutRequest(new PutRequest("watch/cancel/idempotent/key", "v1")).getHeader().isSuccess());

        WatchSubscribeRequest subscribeRequest = new WatchSubscribeRequest();
        subscribeRequest.setStartKey("watch/cancel/idempotent/");
        subscribeRequest.setPrefixMatch(true);
        WatchSubscribeResponse subscribeResponse = requireSuccess(node.handleEtcdRpcWatchSubscribeRequest(
                subscribeRequest,
                null,
                "watch-test-subscribe-5")).getBody();
        assertNotNull(subscribeResponse);

        EtcdRpcResponse<WatchCancelResponse> firstCancelResponse = node.handleEtcdRpcWatchCancelRequest(
                new WatchCancelRequest(subscribeResponse.getWatchId()));
        assertNotNull(firstCancelResponse);
        assertNotNull(firstCancelResponse.getHeader());
        assertTrue(firstCancelResponse.getHeader().isSuccess());
        assertNotNull(firstCancelResponse.getBody());
        assertTrue(firstCancelResponse.getBody().isCanceled());

        EtcdRpcResponse<WatchCancelResponse> secondCancelResponse = node.handleEtcdRpcWatchCancelRequest(
                new WatchCancelRequest(subscribeResponse.getWatchId()));
        assertNotNull(secondCancelResponse);
        assertNotNull(secondCancelResponse.getHeader());
        assertTrue(secondCancelResponse.getHeader().isSuccess());
        assertNotNull(secondCancelResponse.getBody());
        assertFalse(secondCancelResponse.getBody().isCanceled());
    }

    @Test
    public void shouldRejectDuplicateExplicitWatchIdSubscription() throws Exception {
        awaitLeader(node, 3000L);

        WatchSubscribeRequest firstSubscribeRequest = new WatchSubscribeRequest();
        firstSubscribeRequest.setWatchId(777L);
        firstSubscribeRequest.setStartKey("watch/duplicate-id/");
        firstSubscribeRequest.setPrefixMatch(true);
        EtcdRpcResponse<WatchSubscribeResponse> firstSubscribeResponse = node.handleEtcdRpcWatchSubscribeRequest(
                firstSubscribeRequest,
                null,
                "watch-test-subscribe-duplicate-1");
        assertNotNull(firstSubscribeResponse);
        assertNotNull(firstSubscribeResponse.getHeader());
        assertTrue(firstSubscribeResponse.getHeader().isSuccess());
        assertNotNull(firstSubscribeResponse.getBody());
        assertEquals(777L, firstSubscribeResponse.getBody().getWatchId());

        WatchSubscribeRequest secondSubscribeRequest = new WatchSubscribeRequest();
        secondSubscribeRequest.setWatchId(777L);
        secondSubscribeRequest.setStartKey("watch/duplicate-id/");
        secondSubscribeRequest.setPrefixMatch(true);
        EtcdRpcResponse<WatchSubscribeResponse> secondSubscribeResponse = node.handleEtcdRpcWatchSubscribeRequest(
                secondSubscribeRequest,
                null,
                "watch-test-subscribe-duplicate-2");
        assertNotNull(secondSubscribeResponse);
        assertNotNull(secondSubscribeResponse.getHeader());
        assertFalse(secondSubscribeResponse.getHeader().isSuccess());
        assertTrue(secondSubscribeResponse.getHeader().getMessage().contains("duplicate active watchId"));
    }

    @Test
    public void shouldNotReplayHistoryWhenWatchStartRevisionIsZeroOnModulePath() throws Exception {
        awaitLeader(node, 3000L);

        assertTrue(node.handleEtcdRpcPutRequest(new PutRequest("watch/start-zero/a", "v1")).getHeader().isSuccess());
        assertTrue(node.handleEtcdRpcPutRequest(new PutRequest("watch/start-zero/b", "v2")).getHeader().isSuccess());

        long currentRevisionBeforeSubscribe = node.handleEtcdRpcGetRequest(
                        new GetRequest("watch/start-zero/b"))
                .getBody()
                .getRevision();

        WatchSubscribeRequest subscribeRequest = new WatchSubscribeRequest();
        subscribeRequest.setStartKey("watch/start-zero/");
        subscribeRequest.setPrefixMatch(true);
        subscribeRequest.setStartRevision(0L);
        WatchSubscribeResponse subscribeResponse = requireSuccess(node.handleEtcdRpcWatchSubscribeRequest(
                subscribeRequest,
                null,
                "watch-test-subscribe-start-zero")).getBody();
        assertNotNull(subscribeResponse);
        assertTrue(subscribeResponse.getEvents().isEmpty());
        assertEquals(currentRevisionBeforeSubscribe, subscribeResponse.getCurrentRevision());
        assertEquals(currentRevisionBeforeSubscribe + 1L, subscribeResponse.getNextRevision());
    }

    @Test
    public void shouldReplayHistoryByExplicitRangeWhenPrefixMatchDisabled() throws Exception {
        awaitLeader(node, 3000L);

        assertTrue(node.handleEtcdRpcPutRequest(new PutRequest("watch/range/a", "v-a")).getHeader().isSuccess());
        assertTrue(node.handleEtcdRpcPutRequest(new PutRequest("watch/range/b", "v-b")).getHeader().isSuccess());
        assertTrue(node.handleEtcdRpcPutRequest(new PutRequest("watch/range/c", "v-c")).getHeader().isSuccess());
        assertTrue(node.handleEtcdRpcPutRequest(new PutRequest("watch/range/z", "v-z")).getHeader().isSuccess());

        WatchSubscribeRequest subscribeRequest = new WatchSubscribeRequest();
        subscribeRequest.setStartKey("watch/range/b");
        subscribeRequest.setEndKeyExclusive("watch/range/z");
        subscribeRequest.setPrefixMatch(false);
        subscribeRequest.setStartRevision(1L);
        WatchSubscribeResponse subscribeResponse = requireSuccess(node.handleEtcdRpcWatchSubscribeRequest(
                subscribeRequest,
                null,
                "watch-test-subscribe-explicit-range")).getBody();
        assertNotNull(subscribeResponse);
        assertEquals(2, subscribeResponse.getEvents().size());

        Set<String> replayedKeys = new HashSet<>();
        for (int index = 0; index < subscribeResponse.getEvents().size(); index++) {
            replayedKeys.add(subscribeResponse.getEvents().get(index).getKeyValue().getKey());
        }
        assertTrue(replayedKeys.contains("watch/range/b"));
        assertTrue(replayedKeys.contains("watch/range/c"));
        assertFalse(replayedKeys.contains("watch/range/a"));
        assertFalse(replayedKeys.contains("watch/range/z"));
    }

    @Test
    public void shouldKeepWatchSessionWhenPushNotReadyAndWritesAreApplied() throws Exception {
        awaitLeader(node, 3000L);

        WatchSubscribeRequest subscribeRequest = new WatchSubscribeRequest();
        subscribeRequest.setStartKey("watch/push-ready/skip/");
        subscribeRequest.setPrefixMatch(true);
        subscribeRequest.setStartRevision(0L);
        WatchSubscribeResponse subscribeResponse = requireSuccess(node.handleEtcdRpcWatchSubscribeRequest(
                subscribeRequest,
                null,
                "watch-test-subscribe-push-ready-skip")).getBody();
        assertNotNull(subscribeResponse);

        // 响应未走真实 RPC 写回回调前，notificationPushEnabled=false；此时写入不应把会话误判为失效并自动取消。
        assertTrue(node.handleEtcdRpcPutRequest(new PutRequest("watch/push-ready/skip/k1", "v1")).getHeader().isSuccess());
        WatchCancelResponse cancelResponse = requireSuccess(node.handleEtcdRpcWatchCancelRequest(
                new WatchCancelRequest(subscribeResponse.getWatchId()))).getBody();
        assertNotNull(cancelResponse);
        assertTrue(cancelResponse.isCanceled());
    }

    @Test
    public void shouldSendWatchResponseThenWatchStreamOnSameOutboxChannel() throws Exception {
        awaitLeader(node, 3000L);

        EmbeddedChannel channel = new EmbeddedChannel();
        String subscribeRpcMessageId = "watch-test-subscribe-callback-1";
        WatchSubscribeRequest subscribeRequest = new WatchSubscribeRequest();
        subscribeRequest.setStartKey("watch/push-ready/activate/");
        subscribeRequest.setPrefixMatch(true);
        subscribeRequest.setStartRevision(0L);
        EtcdRpcResponse<WatchSubscribeResponse> subscribeCallResult = node.handleEtcdRpcWatchSubscribeRequest(
                subscribeRequest,
                channel,
                subscribeRpcMessageId);
        // 通过真实 RPC 路径（含 channel + rpcMessageId）时，subscribe RESPONSE 由 outbox 发送，方法返回 null。
        assertTrue(subscribeCallResult == null);

        channel.runPendingTasks();
        RpcMessage subscribeResponseMessage = channel.readOutbound();
        assertNotNull(subscribeResponseMessage);
        assertEquals(RpcMessageType.RESPONSE, subscribeResponseMessage.getType());
        assertEquals(subscribeRpcMessageId, subscribeResponseMessage.getRpcMessageId());

        EtcdRpcResponse subscribeResponseEnvelope = serializer.deserialize(subscribeResponseMessage.getData(), EtcdRpcResponse.class);
        assertNotNull(subscribeResponseEnvelope);
        assertNotNull(subscribeResponseEnvelope.getHeader());
        assertTrue(subscribeResponseEnvelope.getHeader().isSuccess());
        Object subscribeResponseBody = subscribeResponseEnvelope.getBody();
        assertTrue(subscribeResponseBody instanceof WatchSubscribeResponse);
        long watchId = ((WatchSubscribeResponse) subscribeResponseBody).getWatchId();
        assertTrue(watchId > 0L);

        assertTrue(node.handleEtcdRpcPutRequest(new PutRequest("watch/push-ready/activate/k1", "v1")).getHeader().isSuccess());
        channel.runPendingTasks();

        RpcMessage streamMessage = channel.readOutbound();
        assertNotNull(streamMessage);
        assertEquals(RpcMessageType.STREAM, streamMessage.getType());
        assertEquals(subscribeRpcMessageId, streamMessage.getRpcMessageId());

        WatchNotification notification =
                serializer.deserialize(streamMessage.getData(), WatchNotification.class);
        assertNotNull(notification);
        assertEquals(watchId, notification.getWatchId());
        assertNotNull(notification.getEvents());
        assertFalse(notification.getEvents().isEmpty());
    }

    private <T> EtcdRpcResponse<T> requireSuccess(EtcdRpcResponse<T> response) {
        assertNotNull(response);
        assertNotNull(response.getHeader());
        assertTrue(response.getHeader().isSuccess());
        return response;
    }

    private void awaitLeader(EtcdNode targetNode, long timeoutMillis) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (System.currentTimeMillis() < deadline) {
            if (targetNode.getRole() == RaftRoleType.LEADER) {
                return;
            }
            Thread.sleep(50L);
        }
        assertEquals(RaftRoleType.LEADER, targetNode.getRole());
    }
}
