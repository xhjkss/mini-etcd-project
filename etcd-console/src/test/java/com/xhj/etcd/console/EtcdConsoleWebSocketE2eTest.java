package com.xhj.etcd.console;

import com.fasterxml.jackson.databind.JsonNode;
import com.xhj.etcd.console.e2e.support.AbstractEtcdConsoleE2eTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketHttpHeaders;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.client.standard.StandardWebSocketClient;
import org.springframework.web.socket.handler.TextWebSocketHandler;

import java.net.URI;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * EtcdConsoleWebSocketE2eTest
 *
 * @author XJks
 * @description Console WebSocket 端到端测试，验证 watch 事件能通过 `/ws/console` 实时推送到客户端。
 */
@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {
                "mini-etcd.console.browser-watch-enabled=false"
        })
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
public class EtcdConsoleWebSocketE2eTest extends AbstractEtcdConsoleE2eTest {

    /**
     * WebSocket + Watch 推送链路回归。
     */
    @Test
    public void shouldPushWatchEventsOverWebSocketAfterKvChanges() throws Exception {
        connectAllNodes();
        String leaderNodeId = awaitLeaderNodeId();

        StandardWebSocketClient webSocketClient = new StandardWebSocketClient();
        CountDownLatch watchEventLatch = new CountDownLatch(2);
        AtomicReference<WebSocketSession> sessionReference = new AtomicReference<>();
        List<JsonNode> receivedMessages = new CopyOnWriteArrayList<>();

        TextWebSocketHandler handler = new TextWebSocketHandler() {
            @Override
            public void afterConnectionEstablished(WebSocketSession session) {
                sessionReference.set(session);
            }

            @Override
            protected void handleTextMessage(WebSocketSession session, TextMessage message) throws Exception {
                JsonNode payload = objectMapper.readTree(message.getPayload());
                receivedMessages.add(payload);
                if (payload != null && payload.has("messageType")
                        && "WATCH_EVENT".equals(payload.get("messageType").asText())) {
                    watchEventLatch.countDown();
                }
            }

            @Override
            public void afterConnectionClosed(WebSocketSession session, CloseStatus status) {
                sessionReference.set(null);
            }
        };

        webSocketClient.doHandshake(handler,
                new WebSocketHttpHeaders(),
                URI.create("ws://127.0.0.1:" + serverPort + "/ws/console")).get(5, TimeUnit.SECONDS);

        JsonNode createWatchResponse = postJson("/api/watch/start?" + endpointQueryByNodeId(leaderNodeId),
                objectMapper.writeValueAsString(watchCreateBody("ws/e2e/")));
        assertSuccess(createWatchResponse);
        long watchId = createWatchResponse.get("data").get("watchId").asLong();

        JsonNode putResponse1 = postJson("/api/mvcc/put",
                objectMapper.writeValueAsString(kvPutBody("ws/e2e/k1", "v1")));
        assertSuccess(putResponse1);

        JsonNode putResponse2 = postJson("/api/mvcc/put",
                objectMapper.writeValueAsString(kvPutBody("ws/e2e/k2", "v2")));
        assertSuccess(putResponse2);

        boolean watchEventArrived = watchEventLatch.await(8, TimeUnit.SECONDS);
        assertTrue(watchEventArrived);

        JsonNode cancelResponse = deleteJson("/api/watch?watchId=" + watchId);
        assertSuccess(cancelResponse);

        boolean hasWatchCreated = false;
        boolean hasWatchEvent = false;
        for (JsonNode message : receivedMessages) {
            if (message == null || !message.has("messageType")) {
                continue;
            }
            String messageType = message.get("messageType").asText();
            if ("WATCH_CREATED".equals(messageType)) {
                hasWatchCreated = true;
            }
            if ("WATCH_EVENT".equals(messageType)) {
                hasWatchEvent = true;
            }
        }
        assertTrue(hasWatchCreated);
        assertTrue(hasWatchEvent);

        WebSocketSession session = sessionReference.get();
        if (session != null && session.isOpen()) {
            session.close();
        }
    }

    /**
     * 双 WebSocket 客户端 + 同节点多 watch 会话回归。
     *
     * <p>覆盖点：</p>
     * <ol>
     *     <li>两个浏览器会话同时接收同一节点下多个 watch 的事件。</li>
     *     <li>取消某个 watch 后，仅该 watch 停止推送，其他 watch 继续工作。</li>
     * </ol>
     */
    @Test
    public void shouldBroadcastMultiWatchEventsToMultipleWebSocketClientsAndKeepOtherWatchAliveAfterCancel() throws Exception {
        connectAllNodes();
        String leaderNodeId = awaitLeaderNodeId();

        long watchIdA;
        long watchIdB;

        StandardWebSocketClient webSocketClient = new StandardWebSocketClient();
        WatchEventCollectorHandler handlerOne = new WatchEventCollectorHandler(objectMapper, 2);
        WatchEventCollectorHandler handlerTwo = new WatchEventCollectorHandler(objectMapper, 2);

        webSocketClient.doHandshake(handlerOne,
                new WebSocketHttpHeaders(),
                URI.create("ws://127.0.0.1:" + serverPort + "/ws/console")).get(5, TimeUnit.SECONDS);
        webSocketClient.doHandshake(handlerTwo,
                new WebSocketHttpHeaders(),
                URI.create("ws://127.0.0.1:" + serverPort + "/ws/console")).get(5, TimeUnit.SECONDS);

        JsonNode createWatchAResponse = postJson("/api/watch/start?" + endpointQueryByNodeId(leaderNodeId),
                objectMapper.writeValueAsString(watchCreateBody("ws/multi/a/")));
        assertSuccess(createWatchAResponse);
        watchIdA = createWatchAResponse.get("data").get("watchId").asLong();
        JsonNode createWatchBResponse = postJson("/api/watch/start?" + endpointQueryByNodeId(leaderNodeId),
                objectMapper.writeValueAsString(watchCreateBody("ws/multi/b/")));
        assertSuccess(createWatchBResponse);
        watchIdB = createWatchBResponse.get("data").get("watchId").asLong();

        JsonNode putResponseA1 = postJson("/api/mvcc/put",
                objectMapper.writeValueAsString(kvPutBody("ws/multi/a/k1", "v-a-1")));
        assertSuccess(putResponseA1);
        JsonNode putResponseB1 = postJson("/api/mvcc/put",
                objectMapper.writeValueAsString(kvPutBody("ws/multi/b/k1", "v-b-1")));
        assertSuccess(putResponseB1);

        assertTrue(handlerOne.awaitWatchEvents(8, TimeUnit.SECONDS));
        assertTrue(handlerTwo.awaitWatchEvents(8, TimeUnit.SECONDS));
        assertTrue(handlerOne.watchEventCount(watchIdA) >= 1);
        assertTrue(handlerOne.watchEventCount(watchIdB) >= 1);
        assertTrue(handlerTwo.watchEventCount(watchIdA) >= 1);
        assertTrue(handlerTwo.watchEventCount(watchIdB) >= 1);

        int watchAHandlerOneBeforeCancel = handlerOne.watchEventCount(watchIdA);
        int watchAHandlerTwoBeforeCancel = handlerTwo.watchEventCount(watchIdA);
        int watchBHandlerOneBeforeCancel = handlerOne.watchEventCount(watchIdB);
        int watchBHandlerTwoBeforeCancel = handlerTwo.watchEventCount(watchIdB);

        JsonNode cancelWatchAResponse = deleteJson("/api/watch?watchId=" + watchIdA);
        assertSuccess(cancelWatchAResponse);

        JsonNode putResponseA2 = postJson("/api/mvcc/put",
                objectMapper.writeValueAsString(kvPutBody("ws/multi/a/k2", "v-a-2")));
        assertSuccess(putResponseA2);
        JsonNode putResponseB2 = postJson("/api/mvcc/put",
                objectMapper.writeValueAsString(kvPutBody("ws/multi/b/k2", "v-b-2")));
        assertSuccess(putResponseB2);

        assertTrue(waitUntilWatchEventCountAtLeast(handlerOne, watchIdB, watchBHandlerOneBeforeCancel + 1, 3000));
        assertTrue(waitUntilWatchEventCountAtLeast(handlerTwo, watchIdB, watchBHandlerTwoBeforeCancel + 1, 3000));
        Thread.sleep(500L);
        assertEquals(watchAHandlerOneBeforeCancel, handlerOne.watchEventCount(watchIdA));
        assertEquals(watchAHandlerTwoBeforeCancel, handlerTwo.watchEventCount(watchIdA));

        handlerOne.closeIfOpen();
        handlerTwo.closeIfOpen();
    }

    /**
     * 单 WebSocket 会话下大量 watch 创建/取消抖动回归。
     *
     * <p>覆盖点：</p>
     * <ol>
     *     <li>同一 WebSocket 下并发存在多条 watch 分发能力。</li>
     *     <li>取消子集后，被取消 watch 不再增长，保留 watch 可继续接收事件。</li>
     * </ol>
     */
    @Test
    public void shouldKeepLargeWatchSetStableUnderCreateCancelChurnOnSingleWebSocketSession() throws Exception {
        connectAllNodes();
        String leaderNodeId = awaitLeaderNodeId();

        StandardWebSocketClient webSocketClient = new StandardWebSocketClient();
        int watchCount = 12;
        WatchEventCollectorHandler collectorHandler = new WatchEventCollectorHandler(objectMapper, watchCount * 2);
        webSocketClient.doHandshake(collectorHandler,
                new WebSocketHttpHeaders(),
                URI.create("ws://127.0.0.1:" + serverPort + "/ws/console")).get(5, TimeUnit.SECONDS);

        List<Long> watchIdList = new ArrayList<>();
        for (int index = 0; index < watchCount; index++) {
            JsonNode createWatchResponse = postJson("/api/watch/start?" + endpointQueryByNodeId(leaderNodeId),
                    objectMapper.writeValueAsString(watchCreateBody("ws/churn/" + index + "/")));
            assertSuccess(createWatchResponse);
            long watchId = createWatchResponse.get("data").get("watchId").asLong();
            watchIdList.add(watchId);
        }

        for (int index = 0; index < watchCount; index++) {
            JsonNode putResponse = postJson("/api/mvcc/put",
                    objectMapper.writeValueAsString(kvPutBody("ws/churn/" + index + "/k1", "v1")));
            assertSuccess(putResponse);
            JsonNode putResponseSecond = postJson("/api/mvcc/put",
                    objectMapper.writeValueAsString(kvPutBody("ws/churn/" + index + "/k2", "v2")));
            assertSuccess(putResponseSecond);
        }

        assertTrue(collectorHandler.awaitWatchEvents(10, TimeUnit.SECONDS));
        assertTrue(waitUntilAllWatchIdsObserved(collectorHandler, watchIdList, 1, 5000));

        List<Long> canceledWatchIdList = watchIdList.subList(0, watchCount / 2);
        List<Long> activeWatchIdList = watchIdList.subList(watchCount / 2, watchCount);
        Map<Long, Integer> canceledWatchEventCountBeforeCancel = snapshotWatchEventCount(collectorHandler, canceledWatchIdList);
        Map<Long, Integer> activeWatchEventCountBeforeCancel = snapshotWatchEventCount(collectorHandler, activeWatchIdList);

        for (Long canceledWatchId : canceledWatchIdList) {
            JsonNode cancelWatchResponse = deleteJson("/api/watch?watchId=" + canceledWatchId);
            assertSuccess(cancelWatchResponse);
        }

        for (int index = watchCount / 2; index < watchCount; index++) {
            JsonNode putResponse = postJson("/api/mvcc/put",
                    objectMapper.writeValueAsString(kvPutBody("ws/churn/" + index + "/k3", "v3")));
            assertSuccess(putResponse);
        }

        for (Long activeWatchId : activeWatchIdList) {
            int beforeCount = activeWatchEventCountBeforeCancel.get(activeWatchId);
            assertTrue(waitUntilWatchEventCountAtLeast(collectorHandler, activeWatchId, beforeCount + 1, 3000));
        }
        Thread.sleep(800L);
        for (Long canceledWatchId : canceledWatchIdList) {
            assertEquals(canceledWatchEventCountBeforeCancel.get(canceledWatchId).intValue(),
                    collectorHandler.watchEventCount(canceledWatchId));
        }

        collectorHandler.closeIfOpen();
    }

    /**
     * 多轮随机 watch create/cancel + put/delete 混合回归。
     *
     * <p>覆盖点：</p>
     * <ol>
     *     <li>随机抖动下 watch 会话生命周期稳定，无异常中断。</li>
     *     <li>取消后的 watch 不再增长；仍激活的 watch 能持续接收事件。</li>
     * </ol>
     */
    @Test
    public void shouldKeepWatchSessionStableUnderRandomCreateCancelAndPutDeleteSequence() throws Exception {
        runRandomWatchCreateCancelAndPutDeleteRound(20260621L, 220);
    }

    /**
     * 多 seed 参数化随机回归，失败时可直接定位具体 seed。
     */
    @ParameterizedTest
    @ValueSource(longs = {20260621L, 20260622L, 20260623L})
    public void shouldKeepWatchSessionStableUnderRandomCreateCancelAndPutDeleteSequenceWithMultipleSeeds(long seed) throws Exception {
        runRandomWatchCreateCancelAndPutDeleteRound(seed, 220);
    }

    /**
     * Lease 会话创建/更新/关闭 WebSocket 推送回归。
     */
    @Test
    public void shouldPushLeaseSessionLifecycleEventsOverWebSocket() throws Exception {
        connectAllNodes();

        StandardWebSocketClient webSocketClient = new StandardWebSocketClient();
        LeaseSessionCollectorHandler leaseSessionCollectorHandler = new LeaseSessionCollectorHandler(objectMapper);
        webSocketClient.doHandshake(leaseSessionCollectorHandler,
                new WebSocketHttpHeaders(),
                URI.create("ws://127.0.0.1:" + serverPort + "/ws/console")).get(5, TimeUnit.SECONDS);

        JsonNode grantStartResponse = postJson("/api/lease/session/grant-start",
                objectMapper.writeValueAsString(leaseSessionGrantStartBody(0L, 8L)));
        assertSuccess(grantStartResponse);
        long leaseId = grantStartResponse.get("data").get("leaseId").asLong();
        assertTrue(leaseId > 0L);

        assertTrue(leaseSessionCollectorHandler.awaitCreated(leaseId, 8, TimeUnit.SECONDS));
        assertTrue(leaseSessionCollectorHandler.awaitUpdated(leaseId, 8, TimeUnit.SECONDS));

        JsonNode stopResponse = deleteJson("/api/lease/session?leaseId=" + leaseId);
        assertSuccess(stopResponse);
        assertTrue(leaseSessionCollectorHandler.awaitClosed(leaseId, 8, TimeUnit.SECONDS));

        leaseSessionCollectorHandler.closeIfOpen();
    }

    /**
     * 执行一轮随机 watch create/cancel + put/delete 混合回归。
     */
    private void runRandomWatchCreateCancelAndPutDeleteRound(long randomSeed, int steps) throws Exception {
        connectAllNodes();
        String leaderNodeId = awaitLeaderNodeId();

        StandardWebSocketClient webSocketClient = new StandardWebSocketClient();
        WatchEventCollectorHandler collectorHandler = new WatchEventCollectorHandler(objectMapper, 1);
        webSocketClient.doHandshake(collectorHandler,
                new WebSocketHttpHeaders(),
                URI.create("ws://127.0.0.1:" + serverPort + "/ws/console")).get(5, TimeUnit.SECONDS);

        List<String> prefixList = new ArrayList<>();
        prefixList.add("ws/rand/a/");
        prefixList.add("ws/rand/b/");
        prefixList.add("ws/rand/c/");
        prefixList.add("ws/rand/d/");

        Map<Long, String> activePrefixByWatchId = new LinkedHashMap<>();
        Map<Long, String> canceledPrefixByWatchId = new LinkedHashMap<>();
        Map<Long, Integer> canceledEventCountSnapshotByWatchId = new LinkedHashMap<>();
        List<String> knownKeyList = new ArrayList<>();

        Random random = new Random(randomSeed);
        for (int step = 0; step < steps; step++) {
            int operationType = random.nextInt(100);
            if (activePrefixByWatchId.isEmpty() || operationType < 35) {
                String prefix = prefixList.get(random.nextInt(prefixList.size()));
                JsonNode createWatchResponse = postJson("/api/watch/start?" + endpointQueryByNodeId(leaderNodeId),
                        objectMapper.writeValueAsString(watchCreateBody(prefix)));
                assertSuccess(createWatchResponse);
                long watchId = createWatchResponse.get("data").get("watchId").asLong();
                activePrefixByWatchId.put(watchId, prefix);
                continue;
            }

            if (operationType < 55) {
                Long watchIdToCancel = pickWatchId(random, activePrefixByWatchId);
                String prefix = activePrefixByWatchId.remove(watchIdToCancel);
                JsonNode cancelWatchResponse = deleteJson("/api/watch?watchId=" + watchIdToCancel);
                assertSuccess(cancelWatchResponse);
                canceledPrefixByWatchId.put(watchIdToCancel, prefix);
                canceledEventCountSnapshotByWatchId.put(watchIdToCancel, collectorHandler.watchEventCount(watchIdToCancel));
                continue;
            }

            String prefix = prefixList.get(random.nextInt(prefixList.size()));
            String key = prefix + "k-" + random.nextInt(40);
            if (!knownKeyList.contains(key)) {
                knownKeyList.add(key);
            }
            if (operationType < 85) {
                JsonNode putResponse = postJson("/api/mvcc/put",
                        objectMapper.writeValueAsString(kvPutBody(key, "rv-" + step)));
                assertSuccess(putResponse);
                continue;
            }

            String deleteKey;
            if (!knownKeyList.isEmpty() && random.nextBoolean()) {
                deleteKey = knownKeyList.get(random.nextInt(knownKeyList.size()));
            } else {
                deleteKey = key;
            }
            JsonNode deleteResponse = postJson("/api/mvcc/delete",
                    objectMapper.writeValueAsString(deleteBody(deleteKey)));
            assertSuccess(deleteResponse);
        }

        // canceled 校验：对每个已取消 watch 再写同前缀 key，确认计数不继续增长。
        for (Map.Entry<Long, String> canceledEntry : canceledPrefixByWatchId.entrySet()) {
            long canceledWatchId = canceledEntry.getKey();
            String canceledPrefix = canceledEntry.getValue();
            int beforeCount = canceledEventCountSnapshotByWatchId.get(canceledWatchId);
            JsonNode putResponse = postJson("/api/mvcc/put",
                    objectMapper.writeValueAsString(kvPutBody(canceledPrefix + "post-cancel-" + canceledWatchId, "v-cancel")));
            assertSuccess(putResponse);
            Thread.sleep(120L);
            assertEquals(beforeCount, collectorHandler.watchEventCount(canceledWatchId));
        }

        // active 校验：每个激活 watch 都应在补一条同前缀写入后观察到至少 +1 事件。
        for (Map.Entry<Long, String> activeEntry : activePrefixByWatchId.entrySet()) {
            long activeWatchId = activeEntry.getKey();
            String activePrefix = activeEntry.getValue();
            int beforeCount = collectorHandler.watchEventCount(activeWatchId);
            JsonNode putResponse = postJson("/api/mvcc/put",
                    objectMapper.writeValueAsString(kvPutBody(activePrefix + "post-active-" + activeWatchId, "v-active")));
            assertSuccess(putResponse);
            assertTrue(waitUntilWatchEventCountAtLeast(collectorHandler, activeWatchId, beforeCount + 1, 4000));
        }

        // 收尾：取消遗留活跃 watch。
        for (Long activeWatchId : new ArrayList<>(activePrefixByWatchId.keySet())) {
            JsonNode cancelResponse = deleteJson("/api/watch?watchId=" + activeWatchId);
            assertSuccess(cancelResponse);
        }
        collectorHandler.closeIfOpen();
    }

    /**
     * 构造 watch 创建请求体。
     */
    private Map<String, Object> watchCreateBody(String startKey) {
        java.util.Map<String, Object> body = new java.util.LinkedHashMap<>();
        body.put("startKey", startKey);
        body.put("endKeyExclusive", "");
        body.put("prefixMatch", true);
        body.put("startRevision", 0);
        body.put("maxEvents", 64);
        body.put("leaderOnly", false);
        return body;
    }

    /**
     * 构造 PUT 请求体。
     */
    private Map<String, Object> kvPutBody(String key, String value) {
        java.util.Map<String, Object> body = new java.util.LinkedHashMap<>();
        body.put("key", key);
        body.put("value", value);
        body.put("leaseId", 0);
        return body;
    }

    /**
     * 构造 DELETE 请求体。
     */
    private Map<String, Object> deleteBody(String key) {
        java.util.Map<String, Object> body = new java.util.LinkedHashMap<>();
        body.put("key", key);
        return body;
    }

    /**
     * 构造 grant-start 请求体。
     */
    private Map<String, Object> leaseSessionGrantStartBody(long leaseId, long ttlSeconds) {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("leaseId", leaseId);
        body.put("ttlSeconds", ttlSeconds);
        return body;
    }

    /**
     * 随机挑选一个 watchId。
     */
    private Long pickWatchId(Random random, Map<Long, String> prefixByWatchId) {
        int targetIndex = random.nextInt(prefixByWatchId.size());
        int index = 0;
        for (Long watchId : prefixByWatchId.keySet()) {
            if (index == targetIndex) {
                return watchId;
            }
            index++;
        }
        throw new IllegalStateException("watchId selection failed");
    }

    /**
     * 等待指定 watchId 的事件数达到目标值。
     */
    private boolean waitUntilWatchEventCountAtLeast(WatchEventCollectorHandler handler,
                                                    long watchId,
                                                    int expectedCount,
                                                    long timeoutMillis) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (System.currentTimeMillis() < deadline) {
            if (handler.watchEventCount(watchId) >= expectedCount) {
                return true;
            }
            Thread.sleep(50L);
        }
        return false;
    }

    /**
     * 等待给定 watchId 集合均至少接收到指定数量事件。
     */
    private boolean waitUntilAllWatchIdsObserved(WatchEventCollectorHandler handler,
                                                 List<Long> watchIdList,
                                                 int expectedCount,
                                                 long timeoutMillis) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (System.currentTimeMillis() < deadline) {
            boolean allObserved = true;
            for (Long watchId : watchIdList) {
                if (handler.watchEventCount(watchId) < expectedCount) {
                    allObserved = false;
                    break;
                }
            }
            if (allObserved) {
                return true;
            }
            Thread.sleep(50L);
        }
        return false;
    }

    /**
     * 抓取 watch 事件计数快照。
     */
    private Map<Long, Integer> snapshotWatchEventCount(WatchEventCollectorHandler handler, List<Long> watchIdList) {
        Map<Long, Integer> watchEventCountMap = new LinkedHashMap<>();
        for (Long watchId : watchIdList) {
            watchEventCountMap.put(watchId, handler.watchEventCount(watchId));
        }
        return watchEventCountMap;
    }

    /**
     * WatchEventCollectorHandler
     *
     * @author XJks
     * @description WebSocket 消息收集器，按 watchId 统计 WATCH_EVENT 数量，便于断言多会话分发行为。
     */
    private static class WatchEventCollectorHandler extends TextWebSocketHandler {

        private final com.fasterxml.jackson.databind.ObjectMapper objectMapper;

        private final CountDownLatch watchEventLatch;

        private final ConcurrentMap<Long, AtomicInteger> watchEventCountByWatchId = new ConcurrentHashMap<>();

        private final List<JsonNode> messageList = new CopyOnWriteArrayList<>();

        private final AtomicReference<WebSocketSession> webSocketSessionReference = new AtomicReference<>();

        private WatchEventCollectorHandler(com.fasterxml.jackson.databind.ObjectMapper objectMapper, int expectedWatchEventCount) {
            this.objectMapper = objectMapper;
            this.watchEventLatch = new CountDownLatch(expectedWatchEventCount);
        }

        @Override
        public void afterConnectionEstablished(WebSocketSession session) {
            webSocketSessionReference.set(session);
        }

        @Override
        protected void handleTextMessage(WebSocketSession session, TextMessage message) throws Exception {
            JsonNode messageNode = objectMapper.readTree(message.getPayload());
            messageList.add(messageNode);
            if (messageNode == null || !messageNode.has("messageType")
                    || !"WATCH_EVENT".equals(messageNode.get("messageType").asText())) {
                return;
            }

            long watchId = resolveWatchId(messageNode);
            if (watchId > 0L) {
                watchEventCountByWatchId.computeIfAbsent(watchId, ignoredWatchId -> new AtomicInteger()).incrementAndGet();
            }
            watchEventLatch.countDown();
        }

        @Override
        public void afterConnectionClosed(WebSocketSession session, CloseStatus status) {
            webSocketSessionReference.set(null);
        }

        private long resolveWatchId(JsonNode messageNode) {
            JsonNode payloadNode = messageNode.get("payload");
            if (payloadNode == null || payloadNode.isNull()) {
                return -1L;
            }
            JsonNode watchSessionNode = payloadNode.get("watchSessionResponse");
            if (watchSessionNode != null && watchSessionNode.has("watchId")) {
                return watchSessionNode.get("watchId").asLong(-1L);
            }
            JsonNode watchNotificationNode = payloadNode.get("watchNotification");
            if (watchNotificationNode != null && watchNotificationNode.has("watchId")) {
                return watchNotificationNode.get("watchId").asLong(-1L);
            }
            return -1L;
        }

        private int watchEventCount(long watchId) {
            AtomicInteger eventCount = watchEventCountByWatchId.get(watchId);
            return eventCount == null ? 0 : eventCount.get();
        }

        private boolean awaitWatchEvents(long timeout, TimeUnit unit) throws InterruptedException {
            return watchEventLatch.await(timeout, unit);
        }

        private void closeIfOpen() throws Exception {
            WebSocketSession session = webSocketSessionReference.get();
            if (session != null && session.isOpen()) {
                session.close();
            }
        }
    }

    /**
     * LeaseSessionCollectorHandler
     *
     * @author XJks
     * @description Lease 会话 WS 事件收集器。
     */
    private static class LeaseSessionCollectorHandler extends TextWebSocketHandler {

        private final com.fasterxml.jackson.databind.ObjectMapper objectMapper;

        private final AtomicReference<WebSocketSession> webSocketSessionReference = new AtomicReference<>();

        private final ConcurrentMap<Long, CountDownLatch> createdLatchByLeaseId = new ConcurrentHashMap<>();

        private final ConcurrentMap<Long, CountDownLatch> updatedLatchByLeaseId = new ConcurrentHashMap<>();

        private final ConcurrentMap<Long, CountDownLatch> closedLatchByLeaseId = new ConcurrentHashMap<>();

        private LeaseSessionCollectorHandler(com.fasterxml.jackson.databind.ObjectMapper objectMapper) {
            this.objectMapper = objectMapper;
        }

        @Override
        public void afterConnectionEstablished(WebSocketSession session) {
            webSocketSessionReference.set(session);
        }

        @Override
        protected void handleTextMessage(WebSocketSession session, TextMessage message) throws Exception {
            JsonNode messageNode = objectMapper.readTree(message.getPayload());
            if (messageNode == null || !messageNode.has("messageType")) {
                return;
            }
            String messageType = messageNode.get("messageType").asText();
            JsonNode payloadNode = messageNode.get("payload");
            long leaseId = payloadNode != null && payloadNode.has("leaseId") ? payloadNode.get("leaseId").asLong(0L) : 0L;
            if (leaseId <= 0L) {
                return;
            }

            if ("LEASE_SESSION_CREATED".equals(messageType)) {
                createdLatchByLeaseId.computeIfAbsent(leaseId, ignoredLeaseId -> new CountDownLatch(1)).countDown();
            } else if ("LEASE_SESSION_UPDATED".equals(messageType)) {
                updatedLatchByLeaseId.computeIfAbsent(leaseId, ignoredLeaseId -> new CountDownLatch(1)).countDown();
            } else if ("LEASE_SESSION_CLOSED".equals(messageType)) {
                closedLatchByLeaseId.computeIfAbsent(leaseId, ignoredLeaseId -> new CountDownLatch(1)).countDown();
            }
        }

        private boolean awaitCreated(long leaseId, long timeout, TimeUnit unit) throws InterruptedException {
            return createdLatchByLeaseId.computeIfAbsent(leaseId, ignoredLeaseId -> new CountDownLatch(1)).await(timeout, unit);
        }

        private boolean awaitUpdated(long leaseId, long timeout, TimeUnit unit) throws InterruptedException {
            return updatedLatchByLeaseId.computeIfAbsent(leaseId, ignoredLeaseId -> new CountDownLatch(1)).await(timeout, unit);
        }

        private boolean awaitClosed(long leaseId, long timeout, TimeUnit unit) throws InterruptedException {
            return closedLatchByLeaseId.computeIfAbsent(leaseId, ignoredLeaseId -> new CountDownLatch(1)).await(timeout, unit);
        }

        private void closeIfOpen() throws Exception {
            WebSocketSession session = webSocketSessionReference.get();
            if (session != null && session.isOpen()) {
                session.close();
            }
        }
    }
}
