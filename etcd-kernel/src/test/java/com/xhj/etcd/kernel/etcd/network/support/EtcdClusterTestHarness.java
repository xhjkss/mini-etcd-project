package com.xhj.etcd.kernel.etcd.network.support;

import com.xhj.etcd.kernel.raft.core.RaftConfig;
import com.xhj.etcd.kernel.raft.storage.RaftPersistentState;
import com.xhj.etcd.kernel.etcd.etcdrpc.EtcdRpcResponse;
import com.xhj.etcd.kernel.etcd.etcdrpc.GetResponse;
import com.xhj.etcd.kernel.etcd.node.EtcdNode;
import com.xhj.etcd.kernel.testsupport.network.DistributedClusterHarness;
import com.xhj.etcd.rpc.NodeEndpoint;
import com.xhj.etcd.rpc.RpcClient;
import com.xhj.etcd.rpc.netty.NettyRpcClient;
import com.xhj.etcd.rpc.netty.NettyRpcServer;
import com.xhj.etcd.serializer.Serializer;
import com.xhj.etcd.serializer.SerializerRegistry;
import com.xhj.etcd.storage.Storage;
import com.xhj.etcd.storage.memory.MemoryStorage;

import java.net.BindException;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * EtcdClusterTestHarness
 *
 * @author XJks
 * @description Etcd + Raft + Netty RPC 多节点联调测试运行时。
 */
public class EtcdClusterTestHarness implements DistributedClusterHarness {

    /**
     * RPC 调用超时时间，单位：毫秒。
     */
    private static final long RPC_TIMEOUT_MILLIS = 5000L;

    /**
     * 用于通过 RPC 探测 leader 的只读 key。
     */
    private static final String LEADER_PROBE_KEY = "__leader_probe__";

    /**
     * 节点重启时 Netty 端口绑定重试次数。
     *
     * <p>用于规避 stop -> start 紧邻窗口中的端口释放延迟。</p>
     */
    private static final int NODE_SERVER_BIND_RETRY_TIMES = 5;

    private final Serializer serializer = SerializerRegistry.getDefaultSerializer();

    private final RpcClient testClient = new NettyRpcClient(serializer, RPC_TIMEOUT_MILLIS);

    private final Map<String, NodeRuntime> runtimeByNodeId = new HashMap<String, NodeRuntime>();

    /**
     * 生成快照的提交日志阈值。
     *
     * <p>测试默认值保持偏大，只有专门的快照恢复测试才会显式调小。</p>
     */
    private int snapshotTriggerLogCount = 50;

    public void startThreeNodeCluster() throws Exception {
        startCluster(3);
    }

    @Override
    public void startCluster(int nodeCount) throws Exception {
        if (nodeCount < 3) {
            throw new IllegalArgumentException("nodeCount must be >= 3");
        }
        List<String> nodeIds = new ArrayList<String>();
        for (int i = 1; i <= nodeCount; i++) {
            nodeIds.add("n" + i);
        }
        startCluster(nodeIds);
    }

    public void startCluster(List<String> nodeIds) throws Exception {
        if (nodeIds == null || nodeIds.size() < 3) {
            throw new IllegalArgumentException("nodeIds size must be >= 3");
        }
        if (!runtimeByNodeId.isEmpty()) {
            throw new IllegalStateException("cluster already started");
        }

        List<String> sortedNodeIds = new ArrayList<String>(nodeIds);
        Collections.sort(sortedNodeIds);

        for (String nodeId : sortedNodeIds) {
            List<String> peers = new ArrayList<String>(sortedNodeIds);
            peers.remove(nodeId);
            createNode(nodeId, peers);
        }

        for (NodeRuntime runtime : runtimeByNodeId.values()) {
            runtime.start();
        }
        registerAllEndpoints();
    }

    public int getClusterSize() {
        return runtimeByNodeId.size();
    }

    public int quorumSize() {
        return getClusterSize() / 2 + 1;
    }

    @Override
    public List<String> getNodeIds() {
        List<String> nodeIds = new ArrayList<String>(runtimeByNodeId.keySet());
        Collections.sort(nodeIds);
        return nodeIds;
    }

    @Override
    public void stopAll() {
        for (NodeRuntime runtime : runtimeByNodeId.values()) {
            runtime.stop();
        }
        testClient.shutdown();
    }

    @Override
    public void stopNode(String nodeId) {
        NodeRuntime runtime = runtimeByNodeId.get(nodeId);
        if (runtime != null) {
            runtime.stop();
        }
    }

    @Override
    public void restartNode(String nodeId) throws Exception {
        NodeRuntime runtime = runtimeByNodeId.get(nodeId);
        if (runtime == null) {
            throw new IllegalArgumentException("node not found: " + nodeId);
        }
        runtime.start();
        registerAllEndpoints();
    }

    public boolean isNodeRunning(String nodeId) {
        NodeRuntime runtime = runtimeByNodeId.get(nodeId);
        return runtime != null && runtime.running;
    }

    public NodeEndpoint getEndpoint(String nodeId) {
        NodeRuntime runtime = runtimeByNodeId.get(nodeId);
        return runtime != null ? runtime.endpoint : null;
    }

    /**
     * 获取指定节点 endpoint，不存在时直接抛错。
     */
    public NodeEndpoint requireEndpoint(String nodeId) {
        NodeEndpoint endpoint = getEndpoint(nodeId);
        if (endpoint == null) {
            throw new IllegalArgumentException("endpoint not found, nodeId=" + nodeId);
        }
        return endpoint;
    }

    public RpcClient getTestClient() {
        return testClient;
    }

    /**
     * 从当前集群中选择一个 follower 节点。
     *
     * <p>该方法不要求 follower 必须处于 running 状态，只按 nodeId 选择一个非 leader 节点。</p>
     */
    @Override
    public String chooseFollowerId(String leaderId) {
        List<String> followerNodeIds = chooseFollowerIds(leaderId, 1);
        if (followerNodeIds.isEmpty()) {
            throw new IllegalStateException("follower node not found, leaderId=" + leaderId);
        }
        return followerNodeIds.get(0);
    }

    /**
     * 选择多个 follower 节点。
     *
     * <p>返回顺序按 nodeId 字典序稳定，便于测试结果可复现。</p>
     */
    public List<String> chooseFollowerIds(String leaderId, int maxCount) {
        if (maxCount <= 0) {
            return new ArrayList<>();
        }
        List<String> followerNodeIds = new ArrayList<>();
        for (String nodeId : getNodeIds()) {
            if (leaderId != null && leaderId.equals(nodeId)) {
                continue;
            }
            followerNodeIds.add(nodeId);
            if (followerNodeIds.size() >= maxCount) {
                break;
            }
        }
        return followerNodeIds;
    }

    /**
     * 停止指定数量的 follower，并返回实际停止的 follower 节点列表。
     */
    @Override
    public List<String> stopFollowers(String leaderId, int maxCount) {
        List<String> followerNodeIds = chooseFollowerIds(leaderId, maxCount);
        for (String followerNodeId : followerNodeIds) {
            stopNode(followerNodeId);
        }
        return followerNodeIds;
    }

    /**
     * 设置每个节点的快照触发阈值。
     *
     * @param snapshotTriggerLogCount 已提交日志数量阈值
     */
    @Override
    public void setSnapshotTriggerLogCount(int snapshotTriggerLogCount) {
        this.snapshotTriggerLogCount = snapshotTriggerLogCount;
    }

    public String getValueViaRpc(String nodeId, String key) throws Exception {
        NodeEndpoint endpoint = getEndpoint(nodeId);
        if (endpoint == null) {
            return null;
        }
        // 这里使用本地读，确保可以直接观察 follower 本地状态机是否已经追平。
        EtcdRpcResponse<GetResponse> response = EtcdTestSupport.callGetByRpc(testClient, endpoint, key, false);
        if (response == null || response.getHeader() == null || !response.getHeader().isSuccess() || response.getBody() == null) {
            return null;
        }
        return response.getBody().getValue();
    }

    public boolean hasValueViaRpc(String nodeId, String key, String expected) throws Exception {
        String value = getValueViaRpc(nodeId, key);
        return expected == null ? value == null : expected.equals(value);
    }

    public void awaitValueVisibleOnNode(final String nodeId, final String key, final String expected, long timeoutMillis) throws Exception {
        EtcdTestSupport.awaitTrue(new java.util.concurrent.Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                return hasValueViaRpc(nodeId, key, expected);
            }
        }, timeoutMillis, "value is not visible on node, nodeId=" + nodeId + ", key=" + key + ", expected=" + expected);
    }

    @Override
    public String awaitLeaderElected(long timeoutMillis) throws Exception {
        NodeEndpoint leaderEndpoint = awaitLeaderEndpoint(timeoutMillis);
        return leaderEndpoint == null ? null : findNodeIdByEndpoint(leaderEndpoint);
    }

    @Override
    public String awaitLeaderElectedExcluding(String excludedNodeId, long timeoutMillis) throws Exception {
        NodeEndpoint leaderEndpoint = awaitLeaderEndpointExcluding(excludedNodeId, timeoutMillis);
        return leaderEndpoint == null ? null : findNodeIdByEndpoint(leaderEndpoint);
    }

    public NodeEndpoint awaitLeaderEndpoint(long timeoutMillis) throws Exception {
        return awaitLeaderEndpointExcluding(null, timeoutMillis);
    }

    public NodeEndpoint awaitLeaderEndpointExcluding(final String excludedNodeId, long timeoutMillis) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (System.currentTimeMillis() < deadline) {
            for (Map.Entry<String, NodeRuntime> entry : runtimeByNodeId.entrySet()) {
                String nodeId = entry.getKey();
                if (excludedNodeId != null && excludedNodeId.equals(nodeId)) {
                    continue;
                }

                NodeRuntime runtime = entry.getValue();
                if (!runtime.running) {
                    continue;
                }

                if (isLeaderByRpc(runtime.endpoint)) {
                    return runtime.endpoint;
                }
            }
            Thread.sleep(50L);
        }
        throw new AssertionError("leader is not elected");
    }

    public void awaitValueReplicated(String key, String expected, long timeoutMillis) throws Exception {
        awaitValueReplicated(key, expected, quorumSize(), timeoutMillis);
    }

    public void awaitValueReplicated(String key, String expected, int minMatchCount, long timeoutMillis) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (System.currentTimeMillis() < deadline) {
            int matchedCount = countRunningNodesWithExpectedValue(key, expected);
            if (matchedCount >= minMatchCount) {
                return;
            }
            Thread.sleep(50L);
        }
        throw new AssertionError("value is not replicated to enough nodes, key="
                + key + ", expected=" + expected + ", minMatchCount=" + minMatchCount);
    }

    public void awaitKeyDeleted(String key, long timeoutMillis) throws Exception {
        awaitKeyDeleted(key, quorumSize(), timeoutMillis);
    }

    public void awaitKeyDeleted(String key, int minDeletedCount, long timeoutMillis) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (System.currentTimeMillis() < deadline) {
            int deletedCount = countRunningNodesWithExpectedValue(key, null);
            if (deletedCount >= minDeletedCount) {
                return;
            }
            Thread.sleep(50L);
        }
        throw new AssertionError("key is not deleted on enough nodes, key="
                + key + ", minDeletedCount=" + minDeletedCount);
    }

    public boolean isValueVisibleOnLeader(String key, String expected) throws Exception {
        NodeEndpoint leaderEndpoint = tryResolveLeaderEndpoint();
        if (leaderEndpoint == null) {
            return false;
        }
        EtcdRpcResponse<GetResponse> response = EtcdTestSupport.callGetByRpc(testClient, leaderEndpoint, key);
        if (response == null || response.getHeader() == null || !response.getHeader().isSuccess() || response.getBody() == null) {
            return false;
        }
        return expected == null ? response.getBody().getValue() == null : expected.equals(response.getBody().getValue());
    }

    /**
     * 读取指定节点当前持久化的 RaftPersistentState。
     *
     * @param nodeId 节点 ID
     * @return 持久化状态；不存在时返回 null
     */
    public RaftPersistentState getPersistentState(String nodeId) {
        NodeRuntime runtime = runtimeByNodeId.get(nodeId);
        if (runtime == null) {
            return null;
        }
        byte[] data = runtime.storage.get("raft", "persistent-state");
        if (data == null) {
            return null;
        }
        return serializer.deserialize(data, RaftPersistentState.class);
    }

    /**
     * 判断指定节点是否已经持久化出 snapshot。
     *
     * @param nodeId 节点 ID
     * @return true 表示已存在 snapshot
     */
    public boolean hasPersistedSnapshot(String nodeId) {
        RaftPersistentState state = getPersistentState(nodeId);
        return state != null && state.getSnapshot() != null;
    }

    /**
     * 等待指定节点的 snapshot 持久化完成。
     *
     * @param nodeId        节点 ID
     * @param timeoutMillis 超时时间
     */
    public void awaitPersistedSnapshotOnNode(final String nodeId, long timeoutMillis) throws Exception {
        EtcdTestSupport.awaitTrue(new java.util.concurrent.Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                return hasPersistedSnapshot(nodeId);
            }
        }, timeoutMillis, "snapshot is not persisted on node, nodeId=" + nodeId);
    }

    public void awaitValueVisibleOnLeader(String key, String expected, long timeoutMillis) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (System.currentTimeMillis() < deadline) {
            if (isValueVisibleOnLeader(key, expected)) {
                return;
            }
            Thread.sleep(50L);
        }
        throw new AssertionError("value is not visible on leader, key=" + key + ", expected=" + expected);
    }

    /**
     * 双向隔离两个节点集合之间的网络通信。
     *
     * <p>实现方式是把 source 节点视角下 target 的 endpoint 替换为黑洞地址，
     * 这样 Raft RPC send 会快速失败，达到“逻辑网络分区”效果。</p>
     *
     * @param leftNodeIds  分区左侧节点集合
     * @param rightNodeIds 分区右侧节点集合
     */
    @Override
    public void isolateBidirectional(List<String> leftNodeIds, List<String> rightNodeIds) throws Exception {
        Set<String> leftSet = sanitizeNodeSet(leftNodeIds);
        Set<String> rightSet = sanitizeNodeSet(rightNodeIds);
        if (leftSet.isEmpty() || rightSet.isEmpty()) {
            return;
        }
        for (String leftNodeId : leftSet) {
            for (String rightNodeId : rightSet) {
                if (leftNodeId.equals(rightNodeId)) {
                    continue;
                }
                overrideEndpointToBlackhole(leftNodeId, rightNodeId);
                overrideEndpointToBlackhole(rightNodeId, leftNodeId);
            }
        }
    }

    /**
     * 将单个节点与其他所有节点双向隔离。
     */
    public void isolateNodeBidirectional(String isolatedNodeId) throws Exception {
        if (isolatedNodeId == null || isolatedNodeId.trim().isEmpty()) {
            return;
        }
        List<String> otherNodeIds = new ArrayList<>();
        for (String nodeId : getNodeIds()) {
            if (!isolatedNodeId.equals(nodeId)) {
                otherNodeIds.add(nodeId);
            }
        }
        isolateBidirectional(Collections.singletonList(isolatedNodeId), otherNodeIds);
    }

    /**
     * 恢复所有节点 endpoint 注册，清理测试阶段注入的网络隔离覆盖。
     */
    @Override
    public void healAllNetworkIsolation() {
        registerAllEndpoints();
    }

    private boolean isLeaderByRpc(NodeEndpoint endpoint) {
        try {
            EtcdRpcResponse<GetResponse> response = EtcdTestSupport.callGetByRpc(testClient, endpoint, LEADER_PROBE_KEY);
            return response != null && response.getHeader() != null && response.getHeader().isSuccess();
        } catch (Exception ignore) {
            return false;
        }
    }

    private NodeEndpoint tryResolveLeaderEndpoint() throws Exception {
        for (Map.Entry<String, NodeRuntime> entry : runtimeByNodeId.entrySet()) {
            NodeRuntime runtime = entry.getValue();
            if (runtime.running && isLeaderByRpc(runtime.endpoint)) {
                return runtime.endpoint;
            }
        }
        return null;
    }

    /**
     * 统计运行中节点里，本地状态机值与 expected 一致的节点数量。
     *
     * <p>该方法使用非线性一致本地读（linearizableRead=false）观察 follower 本地状态，
     * 用于验证日志复制/快照追平是否真正落到各节点状态机，而不是只验证 leader 可读。</p>
     */
    private int countRunningNodesWithExpectedValue(String key, String expected) throws Exception {
        int matchedCount = 0;
        for (Map.Entry<String, NodeRuntime> entry : runtimeByNodeId.entrySet()) {
            NodeRuntime runtime = entry.getValue();
            if (!runtime.running) {
                continue;
            }
            String value = getValueViaRpc(entry.getKey(), key);
            boolean matched = expected == null ? value == null : expected.equals(value);
            if (matched) {
                matchedCount++;
            }
        }
        return matchedCount;
    }

    private String findNodeIdByEndpoint(NodeEndpoint endpoint) {
        if (endpoint == null) {
            return null;
        }
        for (Map.Entry<String, NodeRuntime> entry : runtimeByNodeId.entrySet()) {
            if (entry.getValue().endpoint == endpoint) {
                return entry.getKey();
            }
        }
        return null;
    }

    private void createNode(String nodeId, List<String> peers) throws Exception {
        int port = findFreePort();
        NodeEndpoint endpoint = new NodeEndpoint(nodeId, "127.0.0.1", port);

        Storage storage = new MemoryStorage();
        RaftConfig config = new RaftConfig();
        config.setElectionTimeoutTicks(10);
        config.setHeartbeatTimeoutTicks(3);
        config.setSnapshotTriggerLogCount(snapshotTriggerLogCount);
        config.setPeerNodeIds(new ArrayList<String>(peers));

        runtimeByNodeId.put(nodeId, new NodeRuntime(nodeId, endpoint, storage, config));
    }

    private void registerAllEndpoints() {
        for (NodeRuntime source : runtimeByNodeId.values()) {
            if (!source.running || source.node == null) {
                continue;
            }
            for (NodeRuntime target : runtimeByNodeId.values()) {
                source.node.registerNodeEndpoint(target.endpoint);
            }
        }
    }

    /**
     * 校验并规整节点列表。
     */
    private Set<String> sanitizeNodeSet(List<String> sourceNodeIds) {
        Set<String> nodeSet = new HashSet<>();
        if (sourceNodeIds == null) {
            return nodeSet;
        }
        for (String nodeId : sourceNodeIds) {
            if (nodeId == null || nodeId.trim().isEmpty()) {
                continue;
            }
            if (runtimeByNodeId.containsKey(nodeId)) {
                nodeSet.add(nodeId);
            }
        }
        return nodeSet;
    }

    /**
     * 在 source 节点视角把 target 节点 endpoint 改写为黑洞地址。
     */
    private void overrideEndpointToBlackhole(String sourceNodeId, String targetNodeId) throws Exception {
        NodeRuntime sourceRuntime = runtimeByNodeId.get(sourceNodeId);
        if (sourceRuntime == null || !sourceRuntime.running || sourceRuntime.node == null) {
            return;
        }
        int blackholePort = findFreePort();
        NodeEndpoint blackholeEndpoint = new NodeEndpoint(targetNodeId, "127.0.0.1", blackholePort);
        sourceRuntime.node.registerNodeEndpoint(blackholeEndpoint);
    }

    private static int findFreePort() throws Exception {
        ServerSocket socket = new ServerSocket(0);
        try {
            return socket.getLocalPort();
        } finally {
            socket.close();
        }
    }

    private class NodeRuntime {
        private final String nodeId;
        private final NodeEndpoint endpoint;
        private final Storage storage;
        private final RaftConfig config;

        private EtcdNode node;
        private NettyRpcServer server;
        private RpcClient raftRpcClient;
        private volatile boolean running;

        private NodeRuntime(String nodeId, NodeEndpoint endpoint, Storage storage, RaftConfig config) {
            this.nodeId = nodeId;
            this.endpoint = endpoint;
            this.storage = storage;
            this.config = config;
            this.running = false;
        }

        private void start() throws Exception {
            if (running) {
                return;
            }
            Exception lastException = null;
            for (int retryIndex = 0; retryIndex < NODE_SERVER_BIND_RETRY_TIMES; retryIndex++) {
                try {
                    startOnce();
                    running = true;
                    return;
                } catch (Exception exception) {
                    stopQuietlyAfterStartFailure();
                    if (!isBindException(exception) || retryIndex == NODE_SERVER_BIND_RETRY_TIMES - 1) {
                        throw exception;
                    }
                    lastException = exception;
                    Thread.sleep(80L * (retryIndex + 1));
                }
            }
            if (lastException != null) {
                throw lastException;
            }
        }

        private void startOnce() throws Exception {
            raftRpcClient = new NettyRpcClient(serializer, RPC_TIMEOUT_MILLIS);
            node = new EtcdNode(nodeId, config, storage, serializer, raftRpcClient);
            server = new NettyRpcServer(endpoint, serializer);
            server.registerService(EtcdNode.RPC_SERVICE_NAME,
                    node,
                    EtcdNode.HANDLE_ETCD_RPC_PUT_REQUEST_METHOD_NAME,
                    EtcdNode.HANDLE_ETCD_RPC_GET_REQUEST_METHOD_NAME,
                    EtcdNode.HANDLE_ETCD_RPC_DELETE_REQUEST_METHOD_NAME,
                    EtcdNode.HANDLE_ETCD_RPC_RANGE_REQUEST_METHOD_NAME,
                    EtcdNode.HANDLE_ETCD_RPC_DELETE_RANGE_REQUEST_METHOD_NAME,
                    EtcdNode.HANDLE_ETCD_RPC_TXN_REQUEST_METHOD_NAME,
                    EtcdNode.HANDLE_RAFT_RPC_REQUEST_VOTE_REQUEST_METHOD_NAME,
                    EtcdNode.HANDLE_RAFT_RPC_REQUEST_VOTE_RESPONSE_METHOD_NAME,
                    EtcdNode.HANDLE_RAFT_RPC_APPEND_ENTRIES_REQUEST_METHOD_NAME,
                    EtcdNode.HANDLE_RAFT_RPC_APPEND_ENTRIES_RESPONSE_METHOD_NAME,
                    EtcdNode.HANDLE_RAFT_RPC_INSTALL_SNAPSHOT_REQUEST_METHOD_NAME,
                    EtcdNode.HANDLE_RAFT_RPC_INSTALL_SNAPSHOT_RESPONSE_METHOD_NAME);
            server.start();
            node.start();
        }

        private void stopQuietlyAfterStartFailure() {
            if (node != null) {
                try {
                    node.stop();
                } catch (Exception ignore) {
                }
            }
            if (server != null) {
                try {
                    server.stop();
                } catch (Exception ignore) {
                }
            }
            if (raftRpcClient != null) {
                try {
                    raftRpcClient.shutdown();
                } catch (Exception ignore) {
                }
            }
            node = null;
            server = null;
            raftRpcClient = null;
        }

        private boolean isBindException(Throwable throwable) {
            Throwable current = throwable;
            while (current != null) {
                if (current instanceof BindException) {
                    return true;
                }
                current = current.getCause();
            }
            return false;
        }

        private void stop() {
            if (!running) {
                return;
            }
            running = false;
            node.stop();
            server.stop();
            raftRpcClient.shutdown();
        }
    }
}
