package com.xhj.etcd.kernel.server.client;

import com.xhj.etcd.kernel.server.etcdrpc.DeleteRequest;
import com.xhj.etcd.kernel.server.etcdrpc.DeleteResponse;
import com.xhj.etcd.kernel.server.etcdrpc.EtcdRpcResponse;
import com.xhj.etcd.kernel.server.etcdrpc.GetRequest;
import com.xhj.etcd.kernel.server.etcdrpc.GetResponse;
import com.xhj.etcd.kernel.server.etcdrpc.ListKeysRequest;
import com.xhj.etcd.kernel.server.etcdrpc.ListKeysResponse;
import com.xhj.etcd.kernel.server.etcdrpc.PutRequest;
import com.xhj.etcd.kernel.server.etcdrpc.PutResponse;
import com.xhj.etcd.kernel.server.node.EtcdNode;
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
 * @description 当前阶段临时 Etcd 客户端，用于临时 KV 服务与 Raft / RPC 层联调闭环。
 *
 * <p>
 * TODO:
 *  该类是当前 Phase 的临时客户端实现，不是未来完整 etcd-sdk。
 *  它只服务于 PutRequest、DeleteRequest、GetRequest、ListKeysRequest 四类临时 KV 请求，重点用于验证：
 *  Etcd RPC 入口 -> EtcdEvent -> etcd-event-loop -> EtcdCommand -> Raft propose -> Raft apply -> 状态机结果返回。
 * </p>
 *
 * <p>读写路由语义：</p>
 * <ul>
 *     <li>PUT、DELETE 必须通过 Leader 路由，因为写请求必须进入 Raft。</li>
 *     <li>GET、LIST_KEYS 默认 linearizableRead=true，也通过 Leader 路由并进入 Raft。</li>
 *     <li>GET、LIST_KEYS 如果 linearizableRead=false，只调用当前 endpoint，由服务端本地状态机返回非线性一致读结果。</li>
 * </ul>
 *
 * <p>职责边界：</p>
 * <ul>
 *     <li>只维护 nodeId 到 NodeEndpoint 的简单映射。</li>
 *     <li>只保留以 XxxRequest 为参数的调用方法，避免客户端再额外提供一层重载封装。</li>
 *     <li>不承担完整 SDK 的连接池、服务发现、负载均衡、鉴权、租约、watch 或复杂重试策略。</li>
 * </ul>
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

    public EtcdClient(RpcClient rpcClient, NodeEndpoint endpoint) {
        this(rpcClient, singletonEndpointList(endpoint));
    }

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
     * LIST_KEYS 操作。
     *
     * <p>request.linearizableRead=true 时走 Leader 路由；false 时只请求 currentEndpoint，由服务端本地读。</p>
     */
    public ListKeysResponse listKeys(ListKeysRequest request) {
        if (request != null && !request.isLinearizableRead()) {
            return callCurrentEtcdRequest(EtcdNode.HANDLE_ETCD_RPC_LIST_KEYS_REQUEST_METHOD_NAME, request, ListKeysResponse.class);
        }
        return callLeaderRoutedEtcdRequest(EtcdNode.HANDLE_ETCD_RPC_LIST_KEYS_REQUEST_METHOD_NAME, request, ListKeysResponse.class);
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

    private void registerClientEndpoint(NodeEndpoint endpoint) {
        if (endpoint == null) {
            throw new IllegalArgumentException("endpoint must not be null");
        }
        endpointMap.put(endpoint.getNodeId(), endpoint);
    }

    private static List<NodeEndpoint> singletonEndpointList(NodeEndpoint endpoint) {
        List<NodeEndpoint> endpoints = new ArrayList<>();
        endpoints.add(endpoint);
        return endpoints;
    }
}
