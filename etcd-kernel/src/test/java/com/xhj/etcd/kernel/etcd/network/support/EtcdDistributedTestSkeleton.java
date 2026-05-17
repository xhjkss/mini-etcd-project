package com.xhj.etcd.kernel.etcd.network.support;

import com.xhj.etcd.kernel.etcd.etcdrpc.DeleteRangeRequest;
import com.xhj.etcd.kernel.etcd.etcdrpc.DeleteRangeResponse;
import com.xhj.etcd.kernel.etcd.etcdrpc.DeleteResponse;
import com.xhj.etcd.kernel.etcd.etcdrpc.CompactRequest;
import com.xhj.etcd.kernel.etcd.etcdrpc.CompactResponse;
import com.xhj.etcd.kernel.etcd.etcdrpc.EtcdRpcResponse;
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
import com.xhj.etcd.kernel.etcd.etcdrpc.PutResponse;
import com.xhj.etcd.kernel.etcd.etcdrpc.RangeRequest;
import com.xhj.etcd.kernel.etcd.etcdrpc.RangeResponse;
import com.xhj.etcd.kernel.etcd.etcdrpc.TxnRequest;
import com.xhj.etcd.kernel.etcd.etcdrpc.TxnResponse;
import com.xhj.etcd.kernel.etcd.etcdrpc.WatchCancelRequest;
import com.xhj.etcd.kernel.etcd.etcdrpc.WatchCancelResponse;
import com.xhj.etcd.kernel.etcd.etcdrpc.WatchSubscribeRequest;
import com.xhj.etcd.kernel.etcd.etcdrpc.WatchSubscribeResponse;
import com.xhj.etcd.kernel.testsupport.network.DistributedNetworkTestSkeleton;
import com.xhj.etcd.rpc.NodeEndpoint;

import java.util.List;

/**
 * EtcdDistributedTestSkeleton
 *
 * @author XJks
 * @description Etcd 真实网络测试骨架，复用通用分布式场景驱动并补充 Etcd RPC 重试与集群操作。
 */
public abstract class EtcdDistributedTestSkeleton
        extends DistributedNetworkTestSkeleton<EtcdClusterTestHarness> {

    protected static final long DEFAULT_ELECTION_TIMEOUT_MILLIS = 10000L;
    protected static final long DEFAULT_BOUNDARY_TIMEOUT_MILLIS = 12000L;

    @Override
    protected EtcdClusterTestHarness createHarness() {
        return new EtcdClusterTestHarness();
    }

    protected String startClusterAndAwaitLeader(int nodeCount, long timeoutMillis) throws Exception {
        harness.startCluster(nodeCount);
        return harness.awaitLeaderElected(timeoutMillis);
    }

    protected String awaitLeader(long timeoutMillis) throws Exception {
        return harness.awaitLeaderElected(timeoutMillis);
    }

    protected String awaitNewLeaderExcluding(String excludedLeaderId, long timeoutMillis) throws Exception {
        return harness.awaitLeaderElectedExcluding(excludedLeaderId, timeoutMillis);
    }

    protected NodeEndpoint requireEndpoint(String nodeId) {
        return harness.requireEndpoint(nodeId);
    }

    protected String chooseFollowerId(String leaderId) {
        return harness.chooseFollowerId(leaderId);
    }

    protected List<String> chooseFollowerIds(String leaderId, int maxCount) {
        return harness.chooseFollowerIds(leaderId, maxCount);
    }

    protected List<String> stopFollowers(String leaderId, int maxCount) {
        return harness.stopFollowers(leaderId, maxCount);
    }

    protected void setSnapshotTriggerLogCount(int snapshotTriggerLogCount) {
        harness.setSnapshotTriggerLogCount(snapshotTriggerLogCount);
    }

    protected void isolateNodeBidirectional(String isolatedNodeId) throws Exception {
        harness.isolateNodeBidirectional(isolatedNodeId);
    }

    protected void isolateBidirectional(List<String> leftNodeIds, List<String> rightNodeIds) throws Exception {
        harness.isolateBidirectional(leftNodeIds, rightNodeIds);
    }

    protected void healAllNetworkIsolation() {
        harness.healAllNetworkIsolation();
    }

    protected EtcdRpcResponse<PutResponse> tryPutOnce(NodeEndpoint endpoint, String key, String value, long timeoutMillis) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (System.currentTimeMillis() < deadline) {
            try {
                return EtcdTestSupport.callPutByRpc(harness.getTestClient(), endpoint, key, value);
            } catch (Exception ignore) {
            }
            Thread.sleep(80L);
        }
        return null;
    }

    protected void putOnLeaderWithRetry(String key, String value, long timeoutMillis) throws Exception {
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

    protected DeleteResponse deleteOnLeaderWithRetry(String key, long timeoutMillis) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        Exception lastException = null;
        while (System.currentTimeMillis() < deadline) {
            try {
                NodeEndpoint leaderEndpoint = harness.awaitLeaderEndpoint(4000L);
                EtcdRpcResponse<DeleteResponse> response = EtcdTestSupport.callDeleteByRpc(
                        harness.getTestClient(),
                        leaderEndpoint,
                        key);
                if (response != null && response.getHeader() != null && response.getHeader().isSuccess() && response.getBody() != null) {
                    return response.getBody();
                }
            } catch (Exception e) {
                lastException = e;
            }
            Thread.sleep(100L);
        }
        throw new AssertionError("delete retry timeout, key=" + key, lastException);
    }

    protected RangeResponse rangeOnLeaderWithRetry(RangeRequest request, long timeoutMillis) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        Exception lastException = null;
        while (System.currentTimeMillis() < deadline) {
            try {
                NodeEndpoint leaderEndpoint = harness.awaitLeaderEndpoint(4000L);
                request.setLinearizableRead(true);
                EtcdRpcResponse<RangeResponse> response = EtcdTestSupport.callRangeByRpc(
                        harness.getTestClient(),
                        leaderEndpoint,
                        request);
                if (response != null && response.getHeader() != null && response.getHeader().isSuccess() && response.getBody() != null) {
                    return response.getBody();
                }
            } catch (Exception e) {
                lastException = e;
            }
            Thread.sleep(100L);
        }
        throw new AssertionError("range retry timeout", lastException);
    }

    protected DeleteRangeResponse deleteRangeOnLeaderWithRetry(DeleteRangeRequest request, long timeoutMillis) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        Exception lastException = null;
        while (System.currentTimeMillis() < deadline) {
            try {
                NodeEndpoint leaderEndpoint = harness.awaitLeaderEndpoint(4000L);
                EtcdRpcResponse<DeleteRangeResponse> response = EtcdTestSupport.callDeleteRangeByRpc(
                        harness.getTestClient(),
                        leaderEndpoint,
                        request);
                if (response != null && response.getHeader() != null && response.getHeader().isSuccess() && response.getBody() != null) {
                    return response.getBody();
                }
            } catch (Exception e) {
                lastException = e;
            }
            Thread.sleep(100L);
        }
        throw new AssertionError("delete-range retry timeout", lastException);
    }

    protected GetResponse getLinearizableFromLeaderWithRetry(String key, long timeoutMillis) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        Exception lastException = null;
        while (System.currentTimeMillis() < deadline) {
            try {
                NodeEndpoint leaderEndpoint = harness.awaitLeaderEndpoint(4000L);
                EtcdRpcResponse<GetResponse> response = EtcdTestSupport.callGetByRpc(
                        harness.getTestClient(),
                        leaderEndpoint,
                        key,
                        true);
                if (response != null && response.getHeader() != null && response.getHeader().isSuccess() && response.getBody() != null) {
                    return response.getBody();
                }
            } catch (Exception e) {
                lastException = e;
            }
            Thread.sleep(80L);
        }
        throw new AssertionError("linearizable get retry timeout, key=" + key, lastException);
    }

    protected TxnResponse txnOnLeaderWithRetry(TxnRequest request, long timeoutMillis) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        Exception lastException = null;
        while (System.currentTimeMillis() < deadline) {
            try {
                NodeEndpoint leaderEndpoint = harness.awaitLeaderEndpoint(4000L);
                EtcdRpcResponse<TxnResponse> response = EtcdTestSupport.callTxnByRpc(
                        harness.getTestClient(),
                        leaderEndpoint,
                        request);
                if (response != null && response.getHeader() != null && response.getHeader().isSuccess() && response.getBody() != null) {
                    return response.getBody();
                }
            } catch (Exception e) {
                lastException = e;
            }
            Thread.sleep(100L);
        }
        throw new AssertionError("txn retry timeout", lastException);
    }

    protected CompactResponse compactOnLeaderWithRetry(CompactRequest request, long timeoutMillis) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        Exception lastException = null;
        while (System.currentTimeMillis() < deadline) {
            try {
                NodeEndpoint leaderEndpoint = harness.awaitLeaderEndpoint(4000L);
                EtcdRpcResponse<CompactResponse> response = EtcdTestSupport.callCompactByRpc(
                        harness.getTestClient(),
                        leaderEndpoint,
                        request);
                if (response != null && response.getHeader() != null && response.getHeader().isSuccess() && response.getBody() != null) {
                    return response.getBody();
                }
            } catch (Exception e) {
                lastException = e;
            }
            Thread.sleep(100L);
        }
        throw new AssertionError("compact retry timeout, requestedRevision="
                + (request == null ? "null" : request.getRevision()), lastException);
    }

    protected LeaseGrantResponse leaseGrantOnLeaderWithRetry(LeaseGrantRequest request, long timeoutMillis) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        Exception lastException = null;
        while (System.currentTimeMillis() < deadline) {
            try {
                NodeEndpoint leaderEndpoint = harness.awaitLeaderEndpoint(4000L);
                EtcdRpcResponse<LeaseGrantResponse> response = EtcdTestSupport.callLeaseGrantByRpc(
                        harness.getTestClient(),
                        leaderEndpoint,
                        request);
                if (response != null && response.getHeader() != null && response.getHeader().isSuccess() && response.getBody() != null) {
                    return response.getBody();
                }
            } catch (Exception e) {
                lastException = e;
            }
            Thread.sleep(100L);
        }
        throw new AssertionError("lease grant retry timeout", lastException);
    }

    protected LeaseKeepAliveResponse leaseKeepAliveOnLeaderWithRetry(LeaseKeepAliveRequest request, long timeoutMillis) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        Exception lastException = null;
        while (System.currentTimeMillis() < deadline) {
            try {
                NodeEndpoint leaderEndpoint = harness.awaitLeaderEndpoint(4000L);
                EtcdRpcResponse<LeaseKeepAliveResponse> response = EtcdTestSupport.callLeaseKeepAliveByRpc(
                        harness.getTestClient(),
                        leaderEndpoint,
                        request);
                if (response != null && response.getHeader() != null && response.getHeader().isSuccess() && response.getBody() != null) {
                    return response.getBody();
                }
            } catch (Exception e) {
                lastException = e;
            }
            Thread.sleep(100L);
        }
        throw new AssertionError("lease keepalive retry timeout", lastException);
    }

    protected LeaseRevokeResponse leaseRevokeOnLeaderWithRetry(LeaseRevokeRequest request, long timeoutMillis) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        Exception lastException = null;
        while (System.currentTimeMillis() < deadline) {
            try {
                NodeEndpoint leaderEndpoint = harness.awaitLeaderEndpoint(4000L);
                EtcdRpcResponse<LeaseRevokeResponse> response = EtcdTestSupport.callLeaseRevokeByRpc(
                        harness.getTestClient(),
                        leaderEndpoint,
                        request);
                if (response != null && response.getHeader() != null && response.getHeader().isSuccess() && response.getBody() != null) {
                    return response.getBody();
                }
            } catch (Exception e) {
                lastException = e;
            }
            Thread.sleep(100L);
        }
        throw new AssertionError("lease revoke retry timeout", lastException);
    }

    protected LeaseTtlResponse leaseTtlOnLeaderWithRetry(LeaseTtlRequest request, long timeoutMillis) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        Exception lastException = null;
        while (System.currentTimeMillis() < deadline) {
            try {
                NodeEndpoint leaderEndpoint = harness.awaitLeaderEndpoint(4000L);
                EtcdRpcResponse<LeaseTtlResponse> response = EtcdTestSupport.callLeaseTtlByRpc(
                        harness.getTestClient(),
                        leaderEndpoint,
                        request);
                if (response != null && response.getHeader() != null && response.getHeader().isSuccess() && response.getBody() != null) {
                    return response.getBody();
                }
            } catch (Exception e) {
                lastException = e;
            }
            Thread.sleep(100L);
        }
        throw new AssertionError("lease ttl retry timeout", lastException);
    }

    protected LeaseListResponse leaseListOnLeaderWithRetry(LeaseListRequest request, long timeoutMillis) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        Exception lastException = null;
        while (System.currentTimeMillis() < deadline) {
            try {
                NodeEndpoint leaderEndpoint = harness.awaitLeaderEndpoint(4000L);
                EtcdRpcResponse<LeaseListResponse> response = EtcdTestSupport.callLeaseListByRpc(
                        harness.getTestClient(),
                        leaderEndpoint,
                        request);
                if (response != null && response.getHeader() != null && response.getHeader().isSuccess() && response.getBody() != null) {
                    return response.getBody();
                }
            } catch (Exception e) {
                lastException = e;
            }
            Thread.sleep(100L);
        }
        throw new AssertionError("lease list retry timeout", lastException);
    }

    protected WatchSubscribeResponse watchSubscribeOnLeaderWithRetry(WatchSubscribeRequest request, long timeoutMillis) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        Exception lastException = null;
        while (System.currentTimeMillis() < deadline) {
            try {
                NodeEndpoint leaderEndpoint = harness.awaitLeaderEndpoint(4000L);
                EtcdRpcResponse<WatchSubscribeResponse> response = EtcdTestSupport.callWatchSubscribeByRpc(
                        harness.getTestClient(),
                        leaderEndpoint,
                        request);
                if (response != null && response.getHeader() != null && response.getHeader().isSuccess() && response.getBody() != null) {
                    return response.getBody();
                }
            } catch (Exception e) {
                lastException = e;
            }
            Thread.sleep(100L);
        }
        throw new AssertionError("watch subscribe retry timeout", lastException);
    }

    protected WatchCancelResponse watchCancelOnLeaderWithRetry(WatchCancelRequest request, long timeoutMillis) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        Exception lastException = null;
        while (System.currentTimeMillis() < deadline) {
            try {
                NodeEndpoint leaderEndpoint = harness.awaitLeaderEndpoint(4000L);
                EtcdRpcResponse<WatchCancelResponse> response = EtcdTestSupport.callWatchCancelByRpc(
                        harness.getTestClient(),
                        leaderEndpoint,
                        request);
                if (response != null && response.getHeader() != null && response.getHeader().isSuccess() && response.getBody() != null) {
                    return response.getBody();
                }
            } catch (Exception e) {
                lastException = e;
            }
            Thread.sleep(100L);
        }
        throw new AssertionError("watch cancel retry timeout", lastException);
    }
}

