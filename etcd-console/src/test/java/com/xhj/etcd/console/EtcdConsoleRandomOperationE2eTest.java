package com.xhj.etcd.console;

import com.fasterxml.jackson.databind.JsonNode;
import com.xhj.etcd.console.e2e.support.AbstractEtcdConsoleE2eTest;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * EtcdConsoleRandomOperationE2eTest
 *
 * @author XJks
 * @description Console 端到端随机回归测试，覆盖 MVCC/Lease/Watch/Compact/Diagnostic 混合链路。
 */
@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {
                "mini-etcd.console.browser-watch-enabled=false"
        })
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
public class EtcdConsoleRandomOperationE2eTest extends AbstractEtcdConsoleE2eTest {

    /**
     * 新 seed 随机回归，固定序列保证可复现。
     */
    @Test
    public void shouldKeepMixedOperationsStableUnderDeterministicRandomSeeds() throws Exception {
        connectAllNodes();
        awaitLeaderNodeId();

        int[] seeds = new int[]{20260531, 20260601, 20260602};
        for (int seed : seeds) {
            runRandomRound(seed, 180);
        }
    }

    /**
     * 连接断开重连 + lease 到期删除 + lease 撤销删除边界。
     */
    @Test
    public void shouldKeepConnectionRecoveryAndLeaseBoundaryWorking() throws Exception {
        connectAllNodes();
        String leaderNodeId = awaitLeaderNodeId();

        JsonNode putResponse = postJson("/api/mvcc/put", jsonString(bodyMap("key", "boundary/connection/k1", "value", "v1", "leaseId", 0)));
        assertSuccess(putResponse);

        JsonNode disconnectResponse = deleteJson("/api/connections", disconnectBodyByNodeId("n2"));
        assertSuccess(disconnectResponse);

        JsonNode getWhenDisconnected = postJson("/api/mvcc/get?" + endpointQueryByNodeId("n2"),
                jsonString(bodyMap("key", "boundary/connection/k1", "revision", 0, "linearizableRead", false)));
        assertTrue(getWhenDisconnected.get("code").asInt() != 0);

        JsonNode reconnectResponse = postJson("/api/connections", connectBody(endpointByNodeId("n2").getPort()));
        assertSuccess(reconnectResponse);

        JsonNode getAfterReconnect = postJson("/api/mvcc/get?" + endpointQueryByNodeId("n2"),
                jsonString(bodyMap("key", "boundary/connection/k1", "revision", 0, "linearizableRead", false)));
        assertSuccess(getAfterReconnect);
        assertEquals("v1", getAfterReconnect.get("data").get("value").asText());

        JsonNode leaseGrantResponse = postJson("/api/lease/grant", jsonString(bodyMap("leaseId", 0, "ttlSeconds", 3)));
        assertSuccess(leaseGrantResponse);
        long leaseId = leaseGrantResponse.get("data").get("lease").get("leaseId").asLong();
        assertTrue(leaseId > 0L);

        JsonNode leasePutResponse = postJson("/api/mvcc/put",
                jsonString(bodyMap("key", "boundary/lease/expire", "value", "v-expire", "leaseId", leaseId)));
        assertSuccess(leasePutResponse);

        Thread.sleep(5000L);
        JsonNode leaseExpiredGetResponse = postJson("/api/mvcc/get?" + endpointQueryByNodeId(leaderNodeId),
                jsonString(bodyMap("key", "boundary/lease/expire", "revision", 0, "linearizableRead", true)));
        assertSuccess(leaseExpiredGetResponse);
        assertTrue(leaseExpiredGetResponse.get("data").get("value").isNull());

        JsonNode leaseGrantResponse2 = postJson("/api/lease/grant", jsonString(bodyMap("leaseId", 0, "ttlSeconds", 20)));
        assertSuccess(leaseGrantResponse2);
        long leaseId2 = leaseGrantResponse2.get("data").get("lease").get("leaseId").asLong();
        assertTrue(leaseId2 > 0L);

        JsonNode leasePutResponse2 = postJson("/api/mvcc/put",
                jsonString(bodyMap("key", "boundary/lease/revoke", "value", "v-revoke", "leaseId", leaseId2)));
        assertSuccess(leasePutResponse2);

        JsonNode revokeResponse = postJson("/api/lease/revoke", jsonString(bodyMap("leaseId", leaseId2)));
        assertSuccess(revokeResponse);

        JsonNode revokeGetResponse = postJson("/api/mvcc/get?" + endpointQueryByNodeId(leaderNodeId),
                jsonString(bodyMap("key", "boundary/lease/revoke", "revision", 0, "linearizableRead", true)));
        assertSuccess(revokeGetResponse);
        assertTrue(revokeGetResponse.get("data").get("value").isNull());
    }

    /**
     * 单轮随机混合操作。
     */
    private void runRandomRound(int seed, int steps) throws Exception {
        Random random = new Random(seed);
        List<String> keys = new ArrayList<>();
        List<Long> leaseIds = new ArrayList<>();
        List<Long> watchIds = new ArrayList<>();
        List<String> nodeIds = new ArrayList<>();
        nodeIds.add("n1");
        nodeIds.add("n2");
        nodeIds.add("n3");

        long maxRevision = 0L;
        for (int step = 1; step <= steps; step++) {
            int operationType = random.nextInt(14);
            switch (operationType) {
                case 0:
                    maxRevision = Math.max(maxRevision, runPutAndTrackKey(random, seed, step, keys));
                    break;
                case 1:
                    runGet(random, keys, nodeIds);
                    break;
                case 2:
                    runRange(random, nodeIds);
                    break;
                case 3:
                    runDelete(random, keys);
                    break;
                case 4:
                    runDeleteRange(random);
                    break;
                case 5:
                    runLeaseGrant(random, leaseIds);
                    break;
                case 6:
                    runLeaseTtl(random, leaseIds);
                    break;
                case 7:
                    runLeaseRevoke(random, leaseIds);
                    break;
                case 8:
                    assertSuccess(postJson("/api/lease/list", jsonString(new LinkedHashMap<String, Object>())));
                    break;
                case 9:
                    runCompact(random, maxRevision);
                    break;
                case 10:
                    assertSuccess(getJson("/api/cluster/node-status/on-all-nodes"));
                    break;
                case 11:
                    String nodeIdForStatus = pickNodeId(random, nodeIds);
                    assertSuccess(getJson("/api/cluster/node-status/on-node?" + endpointQueryByNodeId(nodeIdForStatus)));
                    break;
                case 12:
                    String nodeIdForHash = pickNodeId(random, nodeIds);
                    assertSuccess(postJson("/api/cluster/kv-state-hash/on-node?" + endpointQueryByNodeId(nodeIdForHash),
                            jsonString(bodyMap("revision", 0))));
                    break;
                case 13:
                    runWatchCreateOrCancel(random, seed, step, nodeIds, watchIds);
                    break;
                default:
                    break;
            }
        }

        for (Long watchId : watchIds) {
            JsonNode cancelResponse = deleteJson("/api/watch?watchId=" + watchId);
            assertSuccess(cancelResponse);
        }
    }

    /**
     * PUT 并跟踪 key。
     */
    private long runPutAndTrackKey(Random random, int seed, int step, List<String> keys) throws Exception {
        String key = pickKey(random, keys);
        String value = "rv-" + seed + "-" + step;
        JsonNode putResponse = postJson("/api/mvcc/put", jsonString(bodyMap("key", key, "value", value, "leaseId", 0)));
        assertSuccess(putResponse);
        if (!keys.contains(key)) {
            keys.add(key);
        }
        return putResponse.get("data").get("revision").asLong();
    }

    /**
     * GET 操作。
     */
    private void runGet(Random random, List<String> keys, List<String> nodeIds) throws Exception {
        String key = pickKey(random, keys);
        String nodeIdForGet = pickNodeId(random, nodeIds);
        JsonNode getResponse = postJson("/api/mvcc/get?" + endpointQueryByNodeId(nodeIdForGet),
                jsonString(bodyMap("key", key, "revision", 0, "linearizableRead", random.nextBoolean())));
        assertSuccess(getResponse);
    }

    /**
     * RANGE 操作。
     */
    private void runRange(Random random, List<String> nodeIds) throws Exception {
        List<String> prefixes = new ArrayList<>();
        prefixes.add("rand/a/");
        prefixes.add("rand/b/");
        prefixes.add("rand/c/");
        prefixes.add("");
        String startKey = prefixes.get(random.nextInt(prefixes.size()));

        Map<String, Object> body = new LinkedHashMap<>();
        body.put("startKey", startKey);
        body.put("endKeyExclusive", "");
        body.put("prefixMatch", !startKey.isEmpty());
        body.put("limit", 0);
        body.put("keysOnly", random.nextBoolean());
        body.put("countOnly", random.nextBoolean());
        body.put("revision", 0);
        body.put("linearizableRead", random.nextBoolean());

        String nodeIdForRange = pickNodeId(random, nodeIds);
        JsonNode rangeResponse = postJson("/api/mvcc/range?" + endpointQueryByNodeId(nodeIdForRange), jsonString(body));
        assertSuccess(rangeResponse);
    }

    /**
     * DELETE 单 key。
     */
    private void runDelete(Random random, List<String> keys) throws Exception {
        String key = pickKey(random, keys);
        JsonNode deleteResponse = postJson("/api/mvcc/delete", jsonString(bodyMap("key", key)));
        assertSuccess(deleteResponse);
    }

    /**
     * DELETE_RANGE 前缀删除。
     */
    private void runDeleteRange(Random random) throws Exception {
        List<String> prefixes = new ArrayList<>();
        prefixes.add("rand/a/");
        prefixes.add("rand/b/");
        prefixes.add("rand/c/");
        String prefix = prefixes.get(random.nextInt(prefixes.size()));
        JsonNode deleteRangeResponse = postJson("/api/mvcc/delete-range",
                jsonString(bodyMap("startKey", prefix, "endKeyExclusive", "", "prefixMatch", true, "prevKv", random.nextBoolean())));
        assertSuccess(deleteRangeResponse);
    }

    /**
     * LeaseGrant。
     */
    private void runLeaseGrant(Random random, List<Long> leaseIds) throws Exception {
        JsonNode response = postJson("/api/lease/grant", jsonString(bodyMap("leaseId", 0, "ttlSeconds", 5 + random.nextInt(20))));
        assertSuccess(response);
        long leaseId = response.get("data").get("lease").get("leaseId").asLong();
        if (leaseId > 0L) {
            leaseIds.add(leaseId);
        }
    }

    /**
     * LeaseTtl。
     */
    private void runLeaseTtl(Random random, List<Long> leaseIds) throws Exception {
        if (leaseIds.isEmpty()) {
            return;
        }
        long leaseId = leaseIds.get(random.nextInt(leaseIds.size()));
        JsonNode response = postJson("/api/lease/ttl", jsonString(bodyMap("leaseId", leaseId)));
        assertSuccess(response);
    }

    /**
     * LeaseRevoke。
     */
    private void runLeaseRevoke(Random random, List<Long> leaseIds) throws Exception {
        if (leaseIds.isEmpty()) {
            return;
        }
        int index = random.nextInt(leaseIds.size());
        long leaseId = leaseIds.get(index);
        JsonNode response = postJson("/api/lease/revoke", jsonString(bodyMap("leaseId", leaseId)));
        assertSuccess(response);
        leaseIds.remove(index);
    }

    /**
     * Compact，revision 取已知上界内，避免大量无意义失败。
     */
    private void runCompact(Random random, long maxRevision) throws Exception {
        if (maxRevision <= 1L) {
            return;
        }
        long targetRevision = 1L + random.nextInt((int) Math.min(maxRevision, Integer.MAX_VALUE - 1L));
        JsonNode response = postJson("/api/compact", jsonString(bodyMap("revision", targetRevision)));
        if (response != null && response.get("code") != null && response.get("code").asInt() == 0) {
            return;
        }
        String message = response == null || response.get("message") == null ? "" : response.get("message").asText();
        if (message.contains("compacted") || message.contains("revision")) {
            return;
        }
        assertSuccess(response);
    }

    /**
     * Watch 创建或取消。
     */
    private void runWatchCreateOrCancel(Random random,
                                        int seed,
                                        int step,
                                        List<String> nodeIds,
                                        List<Long> watchIds) throws Exception {
        if (watchIds.isEmpty() || random.nextBoolean()) {
            long watchId = ((long) seed) * 100000L + step;
            String nodeIdForWatch = pickNodeId(random, nodeIds);
            JsonNode response = postJson("/api/watch/start?" + endpointQueryByNodeId(nodeIdForWatch),
                    jsonString(bodyMap("watchId", watchId,
                            "startKey", "rand/",
                            "endKeyExclusive", "",
                            "prefixMatch", true,
                            "startRevision", 0,
                            "maxEvents", 64,
                            "leaderOnly", false)));
            assertSuccess(response);
            watchIds.add(response.get("data").get("watchId").asLong());
            return;
        }

        int index = random.nextInt(watchIds.size());
        long watchId = watchIds.get(index);
        JsonNode response = deleteJson("/api/watch?watchId=" + watchId);
        assertSuccess(response);
        watchIds.remove(index);
    }

    /**
     * 选取 nodeId。
     */
    private String pickNodeId(Random random, List<String> nodeIds) {
        return nodeIds.get(random.nextInt(nodeIds.size()));
    }

    /**
     * 选取或生成 key。
     */
    private String pickKey(Random random, List<String> keys) {
        if (keys.isEmpty() || random.nextDouble() < 0.45D) {
            char[] keyGroup = new char[]{'a', 'b', 'c'};
            return "rand/" + keyGroup[random.nextInt(keyGroup.length)] + "/" + random.nextInt(40);
        }
        return keys.get(random.nextInt(keys.size()));
    }

    /**
     * 便捷构造 Map。
     */
    private Map<String, Object> bodyMap(Object... keyValues) {
        Map<String, Object> body = new LinkedHashMap<>();
        for (int index = 0; index + 1 < keyValues.length; index += 2) {
            body.put(String.valueOf(keyValues[index]), keyValues[index + 1]);
        }
        return body;
    }

    /**
     * JSON 序列化。
     */
    private String jsonString(Map<String, Object> body) throws Exception {
        return objectMapper.writeValueAsString(body);
    }

}
