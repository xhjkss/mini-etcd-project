package com.xhj.etcd.kernel.etcd.network.support;

import com.xhj.etcd.kernel.etcd.node.EtcdNode;
import com.xhj.etcd.kernel.etcd.etcdrpc.CompactRequest;
import com.xhj.etcd.kernel.etcd.etcdrpc.CompactResponse;
import com.xhj.etcd.kernel.etcd.etcdrpc.DeleteRequest;
import com.xhj.etcd.kernel.etcd.etcdrpc.DeleteRangeRequest;
import com.xhj.etcd.kernel.etcd.etcdrpc.DeleteRangeResponse;
import com.xhj.etcd.kernel.etcd.etcdrpc.DeleteResponse;
import com.xhj.etcd.kernel.etcd.etcdrpc.EtcdRpcResponse;
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
import com.xhj.etcd.kernel.etcd.etcdrpc.TxnRequest;
import com.xhj.etcd.kernel.etcd.etcdrpc.TxnResponse;
import com.xhj.etcd.rpc.NodeEndpoint;
import com.xhj.etcd.rpc.RpcClient;

import java.net.ServerSocket;
import java.util.concurrent.Callable;

import static org.junit.Assert.assertNotNull;

/**
 * EtcdTestSupport
 *
 * @author XJks
 * @description 分布式真实网络联调测试辅助工具。
 */
public final class EtcdTestSupport {

    private EtcdTestSupport() {
    }

    public static int findFreePort() throws Exception {
        ServerSocket socket = new ServerSocket(0);
        try {
            return socket.getLocalPort();
        } finally {
            socket.close();
        }
    }

    public static void awaitTrue(Callable<Boolean> condition, long timeoutMillis, String failMessage) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (System.currentTimeMillis() < deadline) {
            if (Boolean.TRUE.equals(condition.call())) {
                return;
            }
            Thread.sleep(50L);
        }
        throw new AssertionError(failMessage);
    }


    /**
     * 通过 RPC 调用 PUT。
     *
     * @param client   RPC 客户端
     * @param endpoint 目标节点
     * @param key      key
     * @param value    value
     * @return PUT 响应
     */
    public static EtcdRpcResponse<PutResponse> callPutByRpc(RpcClient client, NodeEndpoint endpoint, String key, String value) {
        PutRequest request = new PutRequest();
        request.setKey(key);
        request.setValue(value);
        return client.call(endpoint,
                EtcdNode.RPC_SERVICE_NAME,
                EtcdNode.HANDLE_ETCD_RPC_PUT_REQUEST_METHOD_NAME,
                request,
                EtcdRpcResponse.class);
    }

    /**
     * 通过 RPC 调用带 leaseId 的 PUT。
     *
     * @param client   RPC 客户端
     * @param endpoint 目标节点
     * @param key      key
     * @param value    value
     * @param leaseId  leaseId
     * @return PUT 响应
     */
    public static EtcdRpcResponse<PutResponse> callPutByRpc(RpcClient client, NodeEndpoint endpoint, String key, String value, long leaseId) {
        PutRequest request = new PutRequest();
        request.setKey(key);
        request.setValue(value);
        request.setLeaseId(leaseId);
        return client.call(endpoint,
                EtcdNode.RPC_SERVICE_NAME,
                EtcdNode.HANDLE_ETCD_RPC_PUT_REQUEST_METHOD_NAME,
                request,
                EtcdRpcResponse.class);
    }

    public static EtcdRpcResponse<GetResponse> callGetByRpc(RpcClient client, NodeEndpoint endpoint, String key) {
        return callGetByRpc(client, endpoint, key, true);
    }

    /**
     * 通过 RPC 调用 GET。
     *
     * @param client           RPC 客户端
     * @param endpoint         目标节点
     * @param key              key
     * @param linearizableRead true 表示线性一致读；false 表示本地读
     * @return GET 响应
     */
    public static EtcdRpcResponse<GetResponse> callGetByRpc(RpcClient client, NodeEndpoint endpoint, String key, boolean linearizableRead) {
        GetRequest request = new GetRequest();
        request.setKey(key);
        request.setLinearizableRead(linearizableRead);
        return client.call(endpoint,
                EtcdNode.RPC_SERVICE_NAME,
                EtcdNode.HANDLE_ETCD_RPC_GET_REQUEST_METHOD_NAME,
                request,
                EtcdRpcResponse.class);
    }

    /**
     * 通过 RPC 调用带 revision 的 GET。
     *
     * @param client           RPC 客户端
     * @param endpoint         目标节点
     * @param key              key
     * @param revision         目标 revision（0 表示当前最新）
     * @param linearizableRead true 表示线性一致读；false 表示本地读
     * @return GET 响应
     */
    public static EtcdRpcResponse<GetResponse> callGetByRpc(RpcClient client, NodeEndpoint endpoint, String key, long revision, boolean linearizableRead) {
        GetRequest request = new GetRequest();
        request.setKey(key);
        request.setRevision(revision);
        request.setLinearizableRead(linearizableRead);
        return client.call(endpoint,
                EtcdNode.RPC_SERVICE_NAME,
                EtcdNode.HANDLE_ETCD_RPC_GET_REQUEST_METHOD_NAME,
                request,
                EtcdRpcResponse.class);
    }

    /**
     * 通过 RPC 调用 DELETE。
     *
     * @param client   RPC 客户端
     * @param endpoint 目标节点
     * @param key      key
     * @return DELETE 响应
     */
    public static EtcdRpcResponse<DeleteResponse> callDeleteByRpc(RpcClient client, NodeEndpoint endpoint, String key) {
        DeleteRequest request = new DeleteRequest();
        request.setKey(key);
        return client.call(endpoint,
                EtcdNode.RPC_SERVICE_NAME,
                EtcdNode.HANDLE_ETCD_RPC_DELETE_REQUEST_METHOD_NAME,
                request,
                EtcdRpcResponse.class);
    }

    /**
     * 通过 RPC 调用 RANGE。
     *
     * @param client   RPC 客户端
     * @param endpoint 目标节点
     * @param request  RANGE 请求
     * @return RANGE 响应
     */
    public static EtcdRpcResponse<RangeResponse> callRangeByRpc(RpcClient client, NodeEndpoint endpoint, RangeRequest request) {
        return client.call(endpoint,
                EtcdNode.RPC_SERVICE_NAME,
                EtcdNode.HANDLE_ETCD_RPC_RANGE_REQUEST_METHOD_NAME,
                request,
                EtcdRpcResponse.class);
    }

    /**
     * 通过 RPC 调用 DELETE_RANGE。
     *
     * @param client   RPC 客户端
     * @param endpoint 目标节点
     * @param request  DELETE_RANGE 请求
     * @return DELETE_RANGE 响应
     */
    public static EtcdRpcResponse<DeleteRangeResponse> callDeleteRangeByRpc(RpcClient client, NodeEndpoint endpoint, DeleteRangeRequest request) {
        return client.call(endpoint,
                EtcdNode.RPC_SERVICE_NAME,
                EtcdNode.HANDLE_ETCD_RPC_DELETE_RANGE_REQUEST_METHOD_NAME,
                request,
                EtcdRpcResponse.class);
    }

    /**
     * 通过 RPC 调用 TXN。
     *
     * @param client   RPC 客户端
     * @param endpoint 目标节点
     * @param request  TXN 请求
     * @return TXN 响应
     */
    public static EtcdRpcResponse<TxnResponse> callTxnByRpc(RpcClient client, NodeEndpoint endpoint, TxnRequest request) {
        return client.call(endpoint,
                EtcdNode.RPC_SERVICE_NAME,
                EtcdNode.HANDLE_ETCD_RPC_TXN_REQUEST_METHOD_NAME,
                request,
                EtcdRpcResponse.class);
    }

    /**
     * 通过 RPC 调用 COMPACT。
     *
     * @param client   RPC 客户端
     * @param endpoint 目标节点
     * @param request  COMPACT 请求
     * @return COMPACT 响应
     */
    public static EtcdRpcResponse<CompactResponse> callCompactByRpc(RpcClient client, NodeEndpoint endpoint, CompactRequest request) {
        return client.call(endpoint,
                EtcdNode.RPC_SERVICE_NAME,
                EtcdNode.HANDLE_ETCD_RPC_COMPACT_REQUEST_METHOD_NAME,
                request,
                EtcdRpcResponse.class);
    }

    /**
     * 通过 RPC 调用 LEASE_GRANT。
     *
     * @param client   RPC 客户端
     * @param endpoint 目标节点
     * @param request  LEASE_GRANT 请求
     * @return LEASE_GRANT 响应
     */
    public static EtcdRpcResponse<LeaseGrantResponse> callLeaseGrantByRpc(RpcClient client, NodeEndpoint endpoint, LeaseGrantRequest request) {
        return client.call(endpoint,
                EtcdNode.RPC_SERVICE_NAME,
                EtcdNode.HANDLE_ETCD_RPC_LEASE_GRANT_REQUEST_METHOD_NAME,
                request,
                EtcdRpcResponse.class);
    }

    /**
     * 通过 RPC 调用 LEASE_KEEP_ALIVE。
     *
     * @param client   RPC 客户端
     * @param endpoint 目标节点
     * @param request  LEASE_KEEP_ALIVE 请求
     * @return LEASE_KEEP_ALIVE 响应
     */
    public static EtcdRpcResponse<LeaseKeepAliveResponse> callLeaseKeepAliveByRpc(RpcClient client, NodeEndpoint endpoint, LeaseKeepAliveRequest request) {
        return client.call(endpoint,
                EtcdNode.RPC_SERVICE_NAME,
                EtcdNode.HANDLE_ETCD_RPC_LEASE_KEEP_ALIVE_REQUEST_METHOD_NAME,
                request,
                EtcdRpcResponse.class);
    }

    /**
     * 通过 RPC 调用 LEASE_REVOKE。
     *
     * @param client   RPC 客户端
     * @param endpoint 目标节点
     * @param request  LEASE_REVOKE 请求
     * @return LEASE_REVOKE 响应
     */
    public static EtcdRpcResponse<LeaseRevokeResponse> callLeaseRevokeByRpc(RpcClient client, NodeEndpoint endpoint, LeaseRevokeRequest request) {
        return client.call(endpoint,
                EtcdNode.RPC_SERVICE_NAME,
                EtcdNode.HANDLE_ETCD_RPC_LEASE_REVOKE_REQUEST_METHOD_NAME,
                request,
                EtcdRpcResponse.class);
    }

    /**
     * 通过 RPC 调用 LEASE_TTL。
     *
     * @param client   RPC 客户端
     * @param endpoint 目标节点
     * @param request  LEASE_TTL 请求
     * @return LEASE_TTL 响应
     */
    public static EtcdRpcResponse<LeaseTtlResponse> callLeaseTtlByRpc(RpcClient client, NodeEndpoint endpoint, LeaseTtlRequest request) {
        return client.call(endpoint,
                EtcdNode.RPC_SERVICE_NAME,
                EtcdNode.HANDLE_ETCD_RPC_LEASE_TTL_REQUEST_METHOD_NAME,
                request,
                EtcdRpcResponse.class);
    }

    /**
     * 通过 RPC 调用 LEASE_LIST。
     *
     * @param client   RPC 客户端
     * @param endpoint 目标节点
     * @param request  LEASE_LIST 请求
     * @return LEASE_LIST 响应
     */
    public static EtcdRpcResponse<LeaseListResponse> callLeaseListByRpc(RpcClient client, NodeEndpoint endpoint, LeaseListRequest request) {
        return client.call(endpoint,
                EtcdNode.RPC_SERVICE_NAME,
                EtcdNode.HANDLE_ETCD_RPC_LEASE_LIST_REQUEST_METHOD_NAME,
                request,
                EtcdRpcResponse.class);
    }

    public static void assertLeaderHint(EtcdRpcResponse<?> response) {
        assertNotNull(response);
        assertNotNull(response.getHeader());
        assertNotNull(response.getHeader().getLeaderId());
    }
}
