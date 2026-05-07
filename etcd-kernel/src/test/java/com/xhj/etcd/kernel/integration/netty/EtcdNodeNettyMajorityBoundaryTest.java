package com.xhj.etcd.kernel.integration.netty;

import com.xhj.etcd.kernel.server.etcdrpc.EtcdRpcResponse;
import com.xhj.etcd.kernel.server.etcdrpc.PutResponse;
import com.xhj.etcd.rpc.NodeEndpoint;
import com.xhj.etcd.rpc.RpcException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * EtcdNodeNettyMajorityBoundaryTest
 *
 * @author XJks
 * @description EtcdNode Netty 联调多数派边界测试。
 */
public class EtcdNodeNettyMajorityBoundaryTest {

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

    @Test
    public void shouldFailWriteWhenLeaderLosesMajorityInFiveNodeCluster() throws Exception {
        harness.startCluster(5);

        String leaderId = harness.awaitLeaderElected(10000L);

        int stopped = 0;
        for (String nodeId : harness.getNodeIds()) {
            if (nodeId.equals(leaderId)) {
                continue;
            }
            harness.stopNode(nodeId);
            stopped++;
            if (stopped == 3) {
                break;
            }
        }

        NodeEndpoint leaderEndpoint = harness.getEndpoint(leaderId);
        try {
            EtcdRpcResponse<PutResponse> response = EtcdNettyTestSupport.callPutByRpc(harness.getTestClient(), leaderEndpoint, "k-no-majority", "v");
            assertNotNull(response);
            assertNotNull(response.getHeader());
            assertFalse(response.getHeader().isSuccess());
        } catch (RpcException expected) {
            assertNotNull(expected.getMessage());
        }
    }

    @Test
    public void shouldSucceedWriteWhenMajorityIsStillAvailableInFiveNodeCluster() throws Exception {
        harness.startCluster(5);

        String leaderId = harness.awaitLeaderElected(10000L);

        int stopped = 0;
        for (String nodeId : harness.getNodeIds()) {
            if (nodeId.equals(leaderId)) {
                continue;
            }
            harness.stopNode(nodeId);
            stopped++;
            if (stopped == 2) {
                break;
            }
        }

        NodeEndpoint leaderEndpoint = harness.getEndpoint(leaderId);
        EtcdRpcResponse<PutResponse> response = EtcdNettyTestSupport.callPutByRpc(harness.getTestClient(), leaderEndpoint, "k-still-majority", "ok");

        assertNotNull(response);
        assertNotNull(response.getHeader());
        assertTrue(response.getHeader().isSuccess());

        harness.awaitValueReplicated("k-still-majority", "ok", 8000L);
    }

    @Test
    public void shouldRecoverWriteAfterRestartingFollowersToRestoreMajority() throws Exception {
        harness.startCluster(5);

        String leaderId = harness.awaitLeaderElected(10000L);

        String[] stoppedFollowerIds = new String[3];
        int stopIndex = 0;
        for (String nodeId : harness.getNodeIds()) {
            if (nodeId.equals(leaderId)) {
                continue;
            }
            harness.stopNode(nodeId);
            stoppedFollowerIds[stopIndex++] = nodeId;
            if (stopIndex == 3) {
                break;
            }
        }

        NodeEndpoint leaderEndpoint = harness.getEndpoint(leaderId);
        try {
            EtcdNettyTestSupport.callPutByRpc(harness.getTestClient(), leaderEndpoint, "k-recover-majority", "v1");
        } catch (RpcException ignore) {
        }

        harness.restartNode(stoppedFollowerIds[0]);
        harness.restartNode(stoppedFollowerIds[1]);

        EtcdNettyTestSupport.awaitTrue(new java.util.concurrent.Callable<Boolean>() {
            @Override
            public Boolean call() {
                try {
                    EtcdRpcResponse<PutResponse> retryResponse = EtcdNettyTestSupport.callPutByRpc(
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
}

