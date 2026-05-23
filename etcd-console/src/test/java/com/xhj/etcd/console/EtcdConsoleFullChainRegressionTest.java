package com.xhj.etcd.console;

import com.fasterxml.jackson.databind.JsonNode;
import com.xhj.etcd.console.e2e.support.AbstractEtcdConsoleE2eTest;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * EtcdConsoleFullChainRegressionTest
 *
 * @author XJks
 * @description Console 全链路回归测试，覆盖连接、KV、Lease、Watch、Compact、诊断接口。
 */
@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {
                "mini-etcd.console.browser-watch-enabled=false"
        })
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
public class EtcdConsoleFullChainRegressionTest extends AbstractEtcdConsoleE2eTest {

    /**
     * 回归完整链路。
     */
    @Test
    public void shouldKeepConsoleFullChainWorking() throws Exception {
        connectAllNodes();
        String leaderNodeId = awaitLeaderNodeId();
        assertNotNull(leaderNodeId);

        JsonNode putResponse = postJson("/api/mvcc/put", "{\"key\":\"app/a\",\"value\":\"v1\"}");
        assertSuccess(putResponse);
        assertTrue(putResponse.get("data").get("revision").asLong() > 0L);

        JsonNode getResponse = postJson("/api/mvcc/get?" + endpointQueryByNodeId(leaderNodeId), "{\"key\":\"app/a\",\"revision\":0,\"linearizableRead\":true}");
        assertSuccess(getResponse);
        assertEquals("v1", getResponse.get("data").get("value").asText());

        JsonNode leaseGrantResponse = postJson("/api/lease/grant", "{\"leaseId\":0,\"ttlSeconds\":20}");
        assertSuccess(leaseGrantResponse);
        assertTrue(leaseGrantResponse.get("data").get("lease").get("leaseId").asLong() > 0L);

        JsonNode firstWatchResponse = postJson("/api/watch/start?" + endpointQueryByNodeId(leaderNodeId),
                "{\"startKey\":\"app/\",\"prefixMatch\":true,\"startRevision\":0,\"leaderOnly\":false}");
        assertSuccess(firstWatchResponse);
        long firstWatchId = firstWatchResponse.get("data").get("watchId").asLong();
        assertTrue(firstWatchId > 0L);

        JsonNode secondWatchResponse = postJson("/api/watch/start?" + endpointQueryByNodeId(leaderNodeId),
                "{\"startKey\":\"app/b\",\"prefixMatch\":false,\"startRevision\":0,\"leaderOnly\":false}");
        assertSuccess(secondWatchResponse);
        long secondWatchId = secondWatchResponse.get("data").get("watchId").asLong();
        assertTrue(secondWatchId > 0L);
        assertTrue(firstWatchId != secondWatchId);

        JsonNode watchListResponse = getJson("/api/watch");
        assertSuccess(watchListResponse);
        assertEquals(2, watchListResponse.get("data").size());

        JsonNode secondPutResponse = postJson("/api/mvcc/put", "{\"key\":\"app/b\",\"value\":\"v2\"}");
        assertSuccess(secondPutResponse);
        assertTrue(secondPutResponse.get("data").get("revision").asLong() > 0L);

        JsonNode statusResponse = getJson("/api/cluster/node-status/on-all-nodes");
        assertSuccess(statusResponse);
        assertTrue(statusResponse.get("data").size() >= 3);

        JsonNode hashResponse = postJson("/api/cluster/kv-state-hash/on-node?" + endpointQueryByNodeId(leaderNodeId), "{\"revision\":0}");
        assertSuccess(hashResponse);
        assertNotNull(hashResponse.get("data").get("hash"));

        JsonNode compactResponse = postJson("/api/compact", "{\"revision\":1}");
        assertSuccess(compactResponse);
        assertTrue(compactResponse.get("data").get("compactRevision").asLong() > 0L);

        JsonNode cancelResponse = deleteJson("/api/watch?watchId=" + firstWatchId);
        assertSuccess(cancelResponse);

        JsonNode watchListAfterCancel = getJson("/api/watch");
        assertSuccess(watchListAfterCancel);
        assertEquals(1, watchListAfterCancel.get("data").size());
        assertEquals(secondWatchId, watchListAfterCancel.get("data").get(0).get("watchId").asLong());
    }
}
