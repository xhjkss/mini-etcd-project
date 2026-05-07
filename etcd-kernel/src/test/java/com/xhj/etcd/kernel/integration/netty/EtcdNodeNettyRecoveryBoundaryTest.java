package com.xhj.etcd.kernel.integration.netty;

import com.xhj.etcd.kernel.server.etcdrpc.EtcdRpcResponse;
import com.xhj.etcd.kernel.server.etcdrpc.PutResponse;
import com.xhj.etcd.rpc.NodeEndpoint;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * EtcdNodeNettyRecoveryBoundaryTest
 *
 * @author XJks
 * @description 参考 MIT6.824 的崩溃恢复、snapshot 安装和多数派恢复测试模式编写的边界测试。
 */
public class EtcdNodeNettyRecoveryBoundaryTest {

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

    /**
     * 借鉴 MIT6.824 的 partition / rejoin / persist 场景：
     * leader 在失去多数派时不能继续提交写请求；当多数派恢复后，写请求应再次可提交并复制到可用节点。
     */
    @Test
    public void shouldRejectWritesWhileMajorityIsLostAndResumeAfterFollowersReturn() throws Exception {
        harness.startCluster(5);
        String leaderId = harness.awaitLeaderElected(12000L);
        NodeEndpoint initialLeaderEndpoint = harness.getEndpoint(leaderId);

        List<String> stoppedFollowerIds = new ArrayList<String>();
        for (String nodeId : harness.getNodeIds()) {
            if (nodeId.equals(leaderId)) {
                continue;
            }
            harness.stopNode(nodeId);
            stoppedFollowerIds.add(nodeId);
            if (stoppedFollowerIds.size() == 3) {
                break;
            }
        }

        EtcdRpcResponse<PutResponse> failedResponse = tryPutOnce(initialLeaderEndpoint, "tdd-majority-key", "before-majority", 4000L);
        if (failedResponse != null && failedResponse.getHeader() != null) {
            assertTrue(!failedResponse.getHeader().isSuccess());
        }

        harness.restartNode(stoppedFollowerIds.get(0));
        harness.restartNode(stoppedFollowerIds.get(1));

        harness.awaitLeaderElected(15000L);
        putOnLeaderWithRetry("tdd-majority-key", "after-majority", 15000L);
        harness.awaitValueReplicated("tdd-majority-key", "after-majority", harness.quorumSize(), 15000L);

        NodeEndpoint recoveredLeaderEndpoint = harness.awaitLeaderEndpoint(15000L);
        EtcdRpcResponse<com.xhj.etcd.kernel.server.etcdrpc.GetResponse> readResponse = EtcdNettyTestSupport.callGetByRpc(
                harness.getTestClient(),
                recoveredLeaderEndpoint,
                "tdd-majority-key");
        assertNotNull(readResponse);
        assertNotNull(readResponse.getBody());
        String leaderValue = readResponse.getBody().getValue();
        assertEquals("after-majority", leaderValue);
    }

    /**
     * 借鉴 MIT6.824 的 snapshot RPC / snapshot recover 场景：
     * follower 长时间离线后重启，若 leader 已经压缩日志，则 follower 应通过 snapshot 追平并恢复可读状态。
     */
    @Test
    public void shouldInstallSnapshotAndRecoverLaggingFollowerAfterRestart() throws Exception {
        harness.setSnapshotTriggerLogCount(2);
        harness.startCluster(3);

        String leaderId = harness.awaitLeaderElected(12000L);
        String laggingFollowerId = chooseFollowerId(leaderId);
        harness.stopNode(laggingFollowerId);

        String latestKey = null;
        for (int i = 1; i <= 12; i++) {
            latestKey = "tdd-snapshot-key-" + i;
            putOnLeaderWithRetry(latestKey, "value-" + i, 12000L);
        }

        harness.awaitPersistedSnapshotOnNode(leaderId, 15000L);
        assertTrue(harness.hasPersistedSnapshot(leaderId));

        harness.restartNode(laggingFollowerId);
        harness.awaitPersistedSnapshotOnNode(laggingFollowerId, 15000L);
        assertTrue(harness.hasPersistedSnapshot(laggingFollowerId));
        assertNotNull(harness.getPersistentState(laggingFollowerId).getSnapshot());
        assertTrue(harness.getPersistentState(laggingFollowerId).getSnapshot().getLastIncludedIndex() > 0L);

        harness.awaitValueVisibleOnNode(laggingFollowerId, latestKey, "value-12", 15000L);
        harness.awaitValueReplicated(latestKey, "value-12", harness.quorumSize(), 15000L);
    }

    private String chooseFollowerId(String leaderId) {
        for (String nodeId : harness.getNodeIds()) {
            if (!nodeId.equals(leaderId)) {
                return nodeId;
            }
        }
        throw new IllegalStateException("follower node not found");
    }

    private EtcdRpcResponse<PutResponse> tryPutOnce(NodeEndpoint endpoint, String key, String value, long timeoutMillis) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (System.currentTimeMillis() < deadline) {
            try {
                return EtcdNettyTestSupport.callPutByRpc(harness.getTestClient(), endpoint, key, value);
            } catch (Exception e) {
                // 预期失败路径：当前没有可用多数派或 leader 正在切换时，直接重试即可。
            }
            Thread.sleep(80L);
        }
        return null;
    }

    private void putOnLeaderWithRetry(String key, String value, long timeoutMillis) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        Exception lastException = null;
        while (System.currentTimeMillis() < deadline) {
            try {
                NodeEndpoint leaderEndpoint = harness.awaitLeaderEndpoint(4000L);
                EtcdRpcResponse<PutResponse> response = EtcdNettyTestSupport.callPutByRpc(
                        harness.getTestClient(),
                        leaderEndpoint,
                        key,
                        value);
                if (response != null && response.getHeader() != null && response.getHeader().isSuccess()) {
                    return;
                }
            } catch (Exception e) {
                lastException = e;
            }
            Thread.sleep(100L);
        }
        throw new AssertionError("put retry timeout, key=" + key + ", value=" + value, lastException);
    }
}
