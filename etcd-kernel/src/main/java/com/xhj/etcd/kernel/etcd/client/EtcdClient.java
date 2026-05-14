package com.xhj.etcd.kernel.etcd.client;

import com.xhj.etcd.kernel.etcd.etcdrpc.DeleteRangeRequest;
import com.xhj.etcd.kernel.etcd.etcdrpc.DeleteRangeResponse;
import com.xhj.etcd.kernel.etcd.etcdrpc.DeleteRequest;
import com.xhj.etcd.kernel.etcd.etcdrpc.DeleteResponse;
import com.xhj.etcd.kernel.etcd.etcdrpc.CompactRequest;
import com.xhj.etcd.kernel.etcd.etcdrpc.CompactResponse;
import com.xhj.etcd.kernel.etcd.etcdrpc.EtcdRpcResponse;
import com.xhj.etcd.kernel.etcd.etcdrpc.GetRequest;
import com.xhj.etcd.kernel.etcd.etcdrpc.GetResponse;
import com.xhj.etcd.kernel.etcd.etcdrpc.PutRequest;
import com.xhj.etcd.kernel.etcd.etcdrpc.PutResponse;
import com.xhj.etcd.kernel.etcd.etcdrpc.RangeRequest;
import com.xhj.etcd.kernel.etcd.etcdrpc.RangeResponse;
import com.xhj.etcd.kernel.etcd.etcdrpc.TxnRequest;
import com.xhj.etcd.kernel.etcd.etcdrpc.TxnResponse;
import com.xhj.etcd.kernel.etcd.node.EtcdNode;
import com.xhj.etcd.rpc.NodeEndpoint;
import com.xhj.etcd.rpc.RpcClient;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * EtcdClient
 *
 * @author XJks
 * @description 当前阶段 Etcd 客户端，覆盖 MVCC KV 核心 API（含历史压缩）。
 */
public class EtcdClient {

    // ==================== 基础依赖 ====================

    /**
     * RPC 客户端。
     */
    private final RpcClient rpcClient;

    /**
     * 客户端已知的节点地址表。
     */
    private final Map<String, NodeEndpoint> endpointMap = new LinkedHashMap<>();

    /**
     * Leader 路由最大重试次数。
     */
    private final int maxLeaderRetryTimes;

    /**
     * 当前优先请求的节点地址。
     */
    private volatile NodeEndpoint currentEndpoint;

    /**
     * 使用单节点地址构造客户端。
     */
    public EtcdClient(RpcClient rpcClient, NodeEndpoint endpoint) {
        this(rpcClient, singletonEndpointList(endpoint));
    }

    /**
     * 使用多节点地址构造客户端。
     *
     * <p>客户端会先登记 endpointMap，再默认使用第一个地址作为初始 currentEndpoint。</p>
     */
    public EtcdClient(RpcClient rpcClient, List<NodeEndpoint> endpoints) {
        if (rpcClient == null) {
            throw new IllegalArgumentException("rpcClient must not be null");
        }
        if (endpoints == null || endpoints.isEmpty()) {
            throw new IllegalArgumentException("endpoints must not be empty");
        }

        this.rpcClient = rpcClient;
        for (NodeEndpoint endpoint : endpoints) {
            registerClientEndpoint(endpoint);
        }
        this.currentEndpoint = endpoints.get(0);
        this.maxLeaderRetryTimes = Math.max(1, this.endpointMap.size());
    }

    /**
     * 获取当前客户端优先访问的节点地址。
     */
    public NodeEndpoint getCurrentEndpoint() {
        return currentEndpoint;
    }

    // ==================== KV 操作 ====================

    /**
     * PUT 操作。
     *
     * <p>PUT 是写请求，必须路由到 Leader，并由服务端通过 EtcdEvent 转 EtcdCommand 后进入 Raft。</p>
     */
    public PutResponse put(PutRequest request) {
        return callLeaderRoutedEtcdRequest(EtcdNode.HANDLE_ETCD_RPC_PUT_REQUEST_METHOD_NAME, request, PutResponse.class);
    }

    /**
     * GET 操作。
     *
     * <p>request.linearizableRead=true 时走 Leader 路由；false 时只请求 currentEndpoint，由服务端本地读。</p>
     */
    public GetResponse get(GetRequest request) {
        if (request != null && !request.isLinearizableRead()) {
            return callCurrentEtcdRequest(EtcdNode.HANDLE_ETCD_RPC_GET_REQUEST_METHOD_NAME, request, GetResponse.class);
        }
        return callLeaderRoutedEtcdRequest(EtcdNode.HANDLE_ETCD_RPC_GET_REQUEST_METHOD_NAME, request, GetResponse.class);
    }

    /**
     * DELETE 操作。
     */
    public DeleteResponse delete(DeleteRequest request) {
        return callLeaderRoutedEtcdRequest(EtcdNode.HANDLE_ETCD_RPC_DELETE_REQUEST_METHOD_NAME, request, DeleteResponse.class);
    }

    /**
     * Range 操作。
     */
    public RangeResponse range(RangeRequest request) {
        if (request != null && !request.isLinearizableRead()) {
            return callCurrentEtcdRequest(EtcdNode.HANDLE_ETCD_RPC_RANGE_REQUEST_METHOD_NAME, request, RangeResponse.class);
        }
        return callLeaderRoutedEtcdRequest(EtcdNode.HANDLE_ETCD_RPC_RANGE_REQUEST_METHOD_NAME, request, RangeResponse.class);
    }

    /**
     * DELETE RANGE 操作。
     */
    public DeleteRangeResponse deleteRange(DeleteRangeRequest request) {
        return callLeaderRoutedEtcdRequest(EtcdNode.HANDLE_ETCD_RPC_DELETE_RANGE_REQUEST_METHOD_NAME, request, DeleteRangeResponse.class);
    }

    /**
     * COMPACT 操作。
     *
     * <p>Compact 会改变整个状态机的历史可读窗口，必须统一路由到 Leader 经 Raft apply 执行。</p>
     */
    public CompactResponse compact(CompactRequest request) {
        return callLeaderRoutedEtcdRequest(EtcdNode.HANDLE_ETCD_RPC_COMPACT_REQUEST_METHOD_NAME, request, CompactResponse.class);
    }

    /**
     * TXN 操作。
     *
     * <p>Txn compare + 分支执行语义必须由 Leader 经 Raft apply 串行执行，因此统一走 Leader 路由。</p>
     */
    public TxnResponse txn(TxnRequest request) {
        return callLeaderRoutedEtcdRequest(EtcdNode.HANDLE_ETCD_RPC_TXN_REQUEST_METHOD_NAME, request, TxnResponse.class);
    }

    // ==================== RPC 调用工具 ====================

    /**
     * 调用当前节点的 Etcd RPC。
     */
    private <T> T callCurrentEtcdRequest(String methodName, Object request, Class<T> responseClass) {
        EtcdRpcResponse<T> response = callEtcdRpcResponse(currentEndpoint, methodName, request, responseClass);
        return response.getBody();
    }

    /**
     * 调用需要 Leader 路由的 Etcd RPC。
     *
     * <p>如果服务端返回 notLeader + leaderId，客户端会在已知 endpointMap 中查找 Leader 并重试。
     * 找不到 Leader 或达到最大重试次数时，返回最后一次响应体。</p>
     */
    private <T> T callLeaderRoutedEtcdRequest(String methodName, Object request, Class<T> responseClass) {
        NodeEndpoint endpoint = currentEndpoint;
        EtcdRpcResponse<T> lastResponse = null;

        for (int retryIndex = 0; retryIndex < maxLeaderRetryTimes; retryIndex++) {
            lastResponse = callEtcdRpcResponse(endpoint, methodName, request, responseClass);
            if (!lastResponse.shouldRetryLeader()) {
                currentEndpoint = endpoint;
                return lastResponse.getBody();
            }

            NodeEndpoint leaderEndpoint = endpointMap.get(lastResponse.getLeaderId());
            if (leaderEndpoint == null || leaderEndpoint.endpointKey().equals(endpoint.endpointKey())) {
                return lastResponse.getBody();
            }
            endpoint = leaderEndpoint;
            currentEndpoint = leaderEndpoint;
        }
        return lastResponse == null ? null : lastResponse.getBody();
    }

    /**
     * 执行底层 Etcd RPC 调用，并校验响应信封和响应 body 类型。
     */
    private <T> EtcdRpcResponse<T> callEtcdRpcResponse(NodeEndpoint endpoint,
                                                       String methodName,
                                                       Object request,
                                                       Class<T> responseClass) {
        Object responseObject = rpcClient.call(
                endpoint,
                EtcdNode.RPC_SERVICE_NAME,
                methodName,
                request,
                EtcdRpcResponse.class);
        if (!(responseObject instanceof EtcdRpcResponse)) {
            throw new IllegalStateException("etcd rpc response must be EtcdRpcResponse, method=" + methodName);
        }

        EtcdRpcResponse<?> response = (EtcdRpcResponse<?>) responseObject;
        Object body = response.getBody();
        if (body != null && !responseClass.isInstance(body)) {
            throw new IllegalStateException("unexpected etcd rpc response body type, expected=" + responseClass.getName() + ", actual=" + body.getClass().getName());
        }
        return EtcdRpcResponse.of(response.getHeader(), responseClass.cast(body));
    }

    /**
     * 注册客户端节点地址。
     */
    private void registerClientEndpoint(NodeEndpoint endpoint) {
        if (endpoint == null) {
            throw new IllegalArgumentException("endpoint must not be null");
        }
        endpointMap.put(endpoint.getNodeId(), endpoint);
    }

    /**
     * 构造只包含一个节点地址的列表。
     */
    private static List<NodeEndpoint> singletonEndpointList(NodeEndpoint endpoint) {
        List<NodeEndpoint> endpoints = new ArrayList<>();
        endpoints.add(endpoint);
        return endpoints;
    }
}
