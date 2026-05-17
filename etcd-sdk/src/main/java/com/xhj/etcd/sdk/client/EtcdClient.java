package com.xhj.etcd.sdk.client;

import com.xhj.etcd.sdk.client.watch.WatchHandle;
import com.xhj.etcd.sdk.client.watch.WatchListener;
import com.xhj.etcd.sdk.client.watch.WatchSubscription;
import com.xhj.etcd.sdk.client.watch.WatchSubscriptionRegistry;
import com.xhj.etcd.kernel.etcd.etcdrpc.DeleteRangeRequest;
import com.xhj.etcd.kernel.etcd.etcdrpc.DeleteRangeResponse;
import com.xhj.etcd.kernel.etcd.etcdrpc.DeleteRequest;
import com.xhj.etcd.kernel.etcd.etcdrpc.DeleteResponse;
import com.xhj.etcd.kernel.etcd.etcdrpc.CompactRequest;
import com.xhj.etcd.kernel.etcd.etcdrpc.CompactResponse;
import com.xhj.etcd.kernel.etcd.etcdrpc.EtcdRpcResponse;
import com.xhj.etcd.kernel.etcd.etcdrpc.KvStateHashRequest;
import com.xhj.etcd.kernel.etcd.etcdrpc.KvStateHashResponse;
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
import com.xhj.etcd.kernel.etcd.etcdrpc.NodeStatusRequest;
import com.xhj.etcd.kernel.etcd.etcdrpc.NodeStatusResponse;
import com.xhj.etcd.kernel.etcd.etcdrpc.TxnRequest;
import com.xhj.etcd.kernel.etcd.etcdrpc.TxnResponse;
import com.xhj.etcd.kernel.etcd.etcdrpc.WatchSubscribeRequest;
import com.xhj.etcd.kernel.etcd.etcdrpc.WatchSubscribeResponse;
import com.xhj.etcd.kernel.etcd.node.EtcdNode;
import com.xhj.etcd.rpc.NodeEndpoint;
import com.xhj.etcd.rpc.RpcClient;
import com.xhj.etcd.rpc.netty.NettyRpcClient;
import com.xhj.etcd.serializer.SerializerRegistry;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

/**
 * EtcdClient
 *
 * @author XJks
 * @description mini-etcd 客户端，提供基础 KV/Txn/Compact/Lease/Watch 请求能力。
 */
public class EtcdClient implements AutoCloseable {

    /**
     * Watch 创建握手超时时间，单位：毫秒。
     */
    private static final long WATCH_SUBSCRIBE_ACK_TIMEOUT_MILLIS = 5000L;

    /**
     * 客户端本地 watchId 序列。
     */
    private static final AtomicLong WATCH_ID_SEQUENCE = new AtomicLong(System.currentTimeMillis() * 1000L);

    /**
     * 客户端本地 watch 路由消息序列。
     */
    private static final AtomicLong WATCH_RPC_MESSAGE_SEQUENCE = new AtomicLong(0L);

    // ==================== 基础依赖 ====================

    /**
     * RPC 客户端。
     */
    private final RpcClient rpcClient;

    /**
     * 是否由当前客户端持有并负责关闭 rpcClient。
     */
    private final boolean ownRpcClient;

    /**
     * 客户端已知的节点地址表。
     */
    private final Map<String, NodeEndpoint> endpointMap = new LinkedHashMap<>();

    /**
     * Leader 路由最大重试次数。
     */
    private final int maxLeaderRetryTimes;

    /**
     * Watch 订阅注册表。
     */
    private final WatchSubscriptionRegistry watchSubscriptionRegistry = new WatchSubscriptionRegistry();

    /**
     * 当前优先请求的节点地址。
     */
    private volatile NodeEndpoint currentEndpoint;

    /**
     * 客户端实例 ID。
     */
    private final String clientId = UUID.randomUUID().toString();

    /**
     * 使用单节点地址构造客户端。
     */
    public EtcdClient(RpcClient rpcClient, NodeEndpoint endpoint) {
        this(rpcClient, singletonEndpointList(endpoint), false);
    }

    /**
     * 使用多节点地址构造客户端。
     *
     * <p>客户端会先登记 endpointMap，再默认使用第一个地址作为初始 currentEndpoint。</p>
     */
    public EtcdClient(RpcClient rpcClient, List<NodeEndpoint> endpoints) {
        this(rpcClient, endpoints, false);
    }

    /**
     * 使用默认 NettyRpcClient 构造客户端。
     */
    public EtcdClient(List<NodeEndpoint> endpoints) {
        this(new NettyRpcClient(SerializerRegistry.getDefaultSerializer(), 5000L), endpoints, true);
    }

    EtcdClient(RpcClient rpcClient, List<NodeEndpoint> endpoints, boolean ownRpcClient) {
        if (rpcClient == null) {
            throw new IllegalArgumentException("rpcClient must not be null");
        }
        if (endpoints == null || endpoints.isEmpty()) {
            throw new IllegalArgumentException("endpoints must not be empty");
        }

        this.rpcClient = rpcClient;
        this.ownRpcClient = ownRpcClient;
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

    @Override
    public void close() {
        if (ownRpcClient) {
            rpcClient.shutdown();
        }
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
     * KvStateHash 诊断。
     *
     * <p>KvStateHash 只读取当前节点本地状态机，不走 leader 路由。</p>
     */
    public KvStateHashResponse computeKvStateHash(KvStateHashRequest request) {
        return callCurrentEtcdRequest(EtcdNode.HANDLE_ETCD_RPC_KV_STATE_HASH_REQUEST_METHOD_NAME, request, KvStateHashResponse.class);
    }

    /**
     * NodeStatus 诊断。
     *
     * <p>NodeStatus 只读取当前节点本地运行态，不走 leader 路由。</p>
     */
    public NodeStatusResponse getNodeStatus(NodeStatusRequest request) {
        return callCurrentEtcdRequest(EtcdNode.HANDLE_ETCD_RPC_NODE_STATUS_REQUEST_METHOD_NAME, request, NodeStatusResponse.class);
    }

    /**
     * Lease 发放。
     */
    public LeaseGrantResponse leaseGrant(LeaseGrantRequest request) {
        return callLeaderRoutedEtcdRequest(EtcdNode.HANDLE_ETCD_RPC_LEASE_GRANT_REQUEST_METHOD_NAME, request, LeaseGrantResponse.class);
    }

    /**
     * Lease 续租。
     */
    public LeaseKeepAliveResponse leaseKeepAlive(LeaseKeepAliveRequest request) {
        return callLeaderRoutedEtcdRequest(EtcdNode.HANDLE_ETCD_RPC_LEASE_KEEP_ALIVE_REQUEST_METHOD_NAME, request, LeaseKeepAliveResponse.class);
    }

    /**
     * Lease 撤销。
     */
    public LeaseRevokeResponse leaseRevoke(LeaseRevokeRequest request) {
        return callLeaderRoutedEtcdRequest(EtcdNode.HANDLE_ETCD_RPC_LEASE_REVOKE_REQUEST_METHOD_NAME, request, LeaseRevokeResponse.class);
    }

    /**
     * Lease TTL 查询。
     */
    public LeaseTtlResponse leaseTtl(LeaseTtlRequest request) {
        return callLeaderRoutedEtcdRequest(EtcdNode.HANDLE_ETCD_RPC_LEASE_TTL_REQUEST_METHOD_NAME, request, LeaseTtlResponse.class);
    }

    /**
     * Lease 列表查询。
     */
    public LeaseListResponse leaseList(LeaseListRequest request) {
        return callLeaderRoutedEtcdRequest(EtcdNode.HANDLE_ETCD_RPC_LEASE_LIST_REQUEST_METHOD_NAME, request, LeaseListResponse.class);
    }

    /**
     * Watch 长连接订阅。
     *
     * <p>TODO: watch subscribe/cancel 走一元 REQUEST/RESPONSE，服务端事件推送走 STREAM。
     * 默认模式下采用“leader 优先”策略：先尝试 currentEndpoint，再按节点列表依次降级。</p>
     *
     * <p>TODO: 流程分为 4 步：</p>
     * <ol>
     *     <li>先准备本地订阅对象：watchId + rpcMessageId + endpoint + listener。</li>
     *     <li>再发送 subscribe 一元请求，首帧 RESPONSE 作为订阅握手结果。</li>
     *     <li>握手成功后，后续 STREAM 推送继续复用同一个 rpcMessageId 路由到该 watch 订阅对象。</li>
     *     <li>cancel/失败/连接关闭时统一清理本地订阅对象与 rpcMessageId handler 注册，避免泄漏。</li>
     * </ol>
     *
     * <p>TODO: watchId 是业务会话标识；rpcMessageId 是 RPC 路由标识。一个 TCP 连接上可以同时存在多个 watch，靠不同 rpcMessageId 分流。</p>
     */
    public WatchHandle watch(WatchSubscribeRequest request, WatchListener listener) {
        if (request != null) {
            request.setLeaderOnly(true);
        }
        return watch(request, buildLeaderPreferredWatchEndpoints(), listener);
    }

    /**
     * Watch 长连接订阅（显式指定目标节点）。
     *
     * <p>当调用方明确希望把 watch 挂在某个节点（例如指定 follower）时，使用该方法。</p>
     *
     * @param request  订阅请求
     * @param endpoint 指定目标节点
     * @param listener watch 监听器
     * @return watch 句柄
     */
    public WatchHandle watch(WatchSubscribeRequest request, NodeEndpoint endpoint, WatchListener listener) {
        if (endpoint == null) {
            throw new IllegalArgumentException("watch endpoint must not be null");
        }
        if (request != null && request.isLeaderOnly()) {
            throw new IllegalArgumentException("watch(request, endpoint, listener) does not allow leaderOnly=true");
        }
        List<NodeEndpoint> endpoints = new ArrayList<>();
        endpoints.add(endpoint);
        return watch(request, endpoints, listener);
    }

    /**
     * Watch 长连接订阅（按候选节点列表轮询）。
     *
     * <p>调用约束：</p>
     * <ol>
     *     <li>列表第一个节点视为“首选节点”。</li>
     *     <li>若首选节点不可用，则按列表顺序降级重试。</li>
     * </ol>
     */
    private WatchHandle watch(WatchSubscribeRequest request, List<NodeEndpoint> candidateEndpoints, WatchListener listener) {
        if (listener == null) {
            throw new IllegalArgumentException("watch listener must not be null");
        }
        if (request == null) {
            throw new IllegalArgumentException("watch subscribe request must not be null");
        }
        if (candidateEndpoints == null || candidateEndpoints.isEmpty()) {
            throw new IllegalArgumentException("watch candidateEndpoints must not be empty");
        }
        boolean leaderOnly = request.isLeaderOnly();

        // 1) 先确定本次 watch 会话 ID：外部已指定则沿用，未指定则由客户端分配。
        long requestedWatchId = request.getWatchId() > 0L ? request.getWatchId() : nextWatchId();
        request.setWatchId(requestedWatchId);

        /**
         * TODO:
         *  leaderOnly=true 时，订阅握手要求最终落到 Leader；收到 notLeader 后按 leaderId 跳转重试。
         *  leaderOnly=false 时，不做 leader 跳转，按候选 endpoint 顺序重试并允许在 follower 建立订阅。
         */
        Exception lastException = null;
        for (int retryIndex = 0; retryIndex < candidateEndpoints.size(); retryIndex++) {
            NodeEndpoint endpoint = candidateEndpoints.get(retryIndex);
            WatchSubscription subscription = null;
            try {
                // 2) 为当前 watch 会话分配独立 rpcMessageId，用于同一 TCP 连接下的多路分发。
                String rpcMessageId = nextWatchRpcMessageId(endpoint);
                subscription = new WatchSubscription(
                        rpcClient,
                        watchSubscriptionRegistry,
                        requestedWatchId,
                        rpcMessageId,
                        endpoint,
                        listener,
                        WATCH_SUBSCRIBE_ACK_TIMEOUT_MILLIS);
                // 3) 先注册路由，再发请求，避免响应先到时找不到本地订阅上下文。
                watchSubscriptionRegistry.register(subscription);

                // 4) 发送 subscribe 一元请求，并阻塞等待首帧 ACK（由订阅对象内部 future 协调）。
                EtcdRpcResponse<WatchSubscribeResponse> subscribeResponse = subscription.subscribe(request);
                if (subscribeResponse != null && subscribeResponse.getHeader() != null && subscribeResponse.getHeader().isSuccess()) {
                    if (leaderOnly) {
                        // 默认 watch 成功后，把当前 endpoint 视为 leader 路由优先节点。
                        currentEndpoint = endpoint;
                    }
                    return subscription;
                }

                // 6) leaderOnly 订阅允许根据 notLeader + leaderId 做 leader 跳转重试。
                if (leaderOnly && subscribeResponse != null && subscribeResponse.shouldRetryLeader()) {
                    NodeEndpoint leaderEndpoint = endpointMap.get(subscribeResponse.getLeaderId());
                    if (subscription != null) {
                        subscription.close();
                    }
                    if (leaderEndpoint != null && !containsEndpoint(candidateEndpoints, leaderEndpoint)) {
                        candidateEndpoints.add(0, leaderEndpoint);
                        retryIndex = -1;
                        continue;
                    }
                }

                // 7) 非 leader 跳转场景的失败：直接终止，避免掩盖业务错误。
                subscription.close();
                if (subscribeResponse != null && subscribeResponse.getHeader() != null) {
                    lastException = new IllegalStateException(subscribeResponse.getHeader().getMessage());
                    break;
                }
                lastException = new IllegalStateException("watch subscribe failed");
                break;
            } catch (Exception exception) {
                // 8) 节点异常或超时场景：关闭当前订阅并切下一个 endpoint 重试。
                lastException = exception;
                if (subscription != null) {
                    subscription.close();
                }
                try {
                    Thread.sleep(80L);
                } catch (InterruptedException interruptedException) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
        throw new IllegalStateException("watch subscribe failed after retries", lastException);
    }

    /**
     * 构造 leader 优先的 watch 候选节点列表。
     *
     * <p>顺序策略：</p>
     * <ol>
     *     <li>优先 currentEndpoint（通常是最近一次 leader 路由成功节点）。</li>
     *     <li>其后追加其余已知节点。</li>
     * </ol>
     */
    private List<NodeEndpoint> buildLeaderPreferredWatchEndpoints() {
        List<NodeEndpoint> endpoints = new ArrayList<>();
        if (currentEndpoint != null) {
            endpoints.add(currentEndpoint);
        }
        for (NodeEndpoint endpoint : endpointMap.values()) {
            if (endpoint == null) {
                continue;
            }
            if (currentEndpoint != null && currentEndpoint.endpointKey().equals(endpoint.endpointKey())) {
                continue;
            }
            endpoints.add(endpoint);
        }
        return endpoints;
    }

    /**
     * 判断 endpoint 是否已存在于候选列表。
     *
     * @param endpoints      候选列表
     * @param targetEndpoint 目标节点
     * @return true 表示已存在
     */
    private boolean containsEndpoint(List<NodeEndpoint> endpoints, NodeEndpoint targetEndpoint) {
        if (endpoints == null || targetEndpoint == null) {
            return false;
        }
        for (NodeEndpoint endpoint : endpoints) {
            if (endpoint != null && targetEndpoint.endpointKey().equals(endpoint.endpointKey())) {
                return true;
            }
        }
        return false;
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

    /**
     * 分配下一个 watchId。
     */
    private long nextWatchId() {
        long nextWatchId = WATCH_ID_SEQUENCE.incrementAndGet();
        return nextWatchId <= 0L ? Math.abs(nextWatchId) + 1L : nextWatchId;
    }

    /**
     * 分配下一个 watch 路由消息 ID。
     */
    private String nextWatchRpcMessageId(NodeEndpoint endpoint) {
        long sequence = WATCH_RPC_MESSAGE_SEQUENCE.incrementAndGet();
        String endpointKey = endpoint == null ? "unknown-endpoint" : endpoint.endpointKey();
        // TODO: rpcMessageId 结构包含 clientId + endpointKey + 递增序号，便于排障时快速定位来源与目标节点。
        return "watch-" + clientId + "-" + endpointKey + "-" + sequence;
    }

}
