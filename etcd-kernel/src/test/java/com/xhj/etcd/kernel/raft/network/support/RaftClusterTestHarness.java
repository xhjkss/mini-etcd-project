package com.xhj.etcd.kernel.raft.network.support;

import com.xhj.etcd.kernel.etcd.command.EtcdCommand;
import com.xhj.etcd.kernel.etcd.command.EtcdCommandCodec;
import com.xhj.etcd.kernel.etcd.command.EtcdCommandType;
import com.xhj.etcd.kernel.etcd.etcdrpc.PutRequest;
import com.xhj.etcd.kernel.etcd.node.EtcdNode;
import com.xhj.etcd.kernel.raft.core.RaftConfig;
import com.xhj.etcd.kernel.raft.core.RaftProposeResult;
import com.xhj.etcd.kernel.raft.core.RaftRoleType;
import com.xhj.etcd.kernel.raft.storage.RaftPersistentState;
import com.xhj.etcd.kernel.testsupport.network.DistributedClusterHarness;
import com.xhj.etcd.rpc.NodeEndpoint;
import com.xhj.etcd.rpc.RpcClient;
import com.xhj.etcd.rpc.netty.NettyRpcClient;
import com.xhj.etcd.rpc.netty.NettyRpcServer;
import com.xhj.etcd.serializer.Serializer;
import com.xhj.etcd.serializer.SerializerRegistry;
import com.xhj.etcd.storage.Storage;
import com.xhj.etcd.storage.memory.MemoryStorage;

import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Base64;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * RaftClusterTestHarness
 *
 * @author XJks
 * @description Raft 真实网络测试运行时：复用 EtcdNode 的 RaftReady 处理与持久化流程，不在测试层重复实现 ready 逻辑。
 */
public class RaftClusterTestHarness implements DistributedClusterHarness {

    /**
     * RPC 调用超时时间，单位：毫秒。
     */
    private static final long RPC_TIMEOUT_MILLIS = 5000L;

    private final Serializer serializer = SerializerRegistry.getDefaultSerializer();

    private final EtcdCommandCodec commandCodec = new EtcdCommandCodec(serializer);

    private final Map<String, NodeRuntime> runtimeByNodeId = new HashMap<>();

    private final AtomicLong proposeSequence = new AtomicLong(0L);

    /**
     * 生成快照的提交日志阈值。
     */
    private int snapshotTriggerLogCount = 50;

    @Override
    public void startCluster(int nodeCount) throws Exception {
        if (nodeCount < 3) {
            throw new IllegalArgumentException("nodeCount must be >= 3");
        }
        if (!runtimeByNodeId.isEmpty()) {
            throw new IllegalStateException("cluster already started");
        }

        List<String> nodeIds = new ArrayList<>();
        for (int index = 1; index <= nodeCount; index++) {
            nodeIds.add("r" + index);
        }
        Collections.sort(nodeIds);

        for (String nodeId : nodeIds) {
            List<String> peerNodeIds = new ArrayList<>(nodeIds);
            peerNodeIds.remove(nodeId);
            createRuntime(nodeId, peerNodeIds);
        }

        for (NodeRuntime runtime : runtimeByNodeId.values()) {
            runtime.start();
        }
        registerAllEndpoints();
    }

    @Override
    public void stopAll() {
        for (NodeRuntime runtime : runtimeByNodeId.values()) {
            runtime.stop();
        }
        runtimeByNodeId.clear();
    }

    public int getClusterSize() {
        return runtimeByNodeId.size();
    }

    public int quorumSize() {
        return getClusterSize() / 2 + 1;
    }

    @Override
    public List<String> getNodeIds() {
        List<String> nodeIds = new ArrayList<>(runtimeByNodeId.keySet());
        Collections.sort(nodeIds);
        return nodeIds;
    }

    @Override
    public void setSnapshotTriggerLogCount(int snapshotTriggerLogCount) {
        this.snapshotTriggerLogCount = snapshotTriggerLogCount;
    }

    public boolean isNodeRunning(String nodeId) {
        NodeRuntime runtime = runtimeByNodeId.get(nodeId);
        return runtime != null && runtime.running;
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

    @Override
    public String chooseFollowerId(String leaderId) {
        for (String nodeId : getNodeIds()) {
            if (!nodeId.equals(leaderId)) {
                return nodeId;
            }
        }
        throw new IllegalStateException("follower not found, leaderId=" + leaderId);
    }

    public List<String> chooseFollowerIds(String leaderId, int maxCount) {
        List<String> followerNodeIds = new ArrayList<>();
        for (String nodeId : getNodeIds()) {
            if (!nodeId.equals(leaderId)) {
                followerNodeIds.add(nodeId);
            }
            if (followerNodeIds.size() >= maxCount) {
                break;
            }
        }
        return followerNodeIds;
    }

    @Override
    public List<String> stopFollowers(String leaderId, int maxCount) {
        List<String> followerNodeIds = chooseFollowerIds(leaderId, maxCount);
        for (String followerNodeId : followerNodeIds) {
            stopNode(followerNodeId);
        }
        return followerNodeIds;
    }

    @Override
    public String awaitLeaderElected(long timeoutMillis) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (System.currentTimeMillis() < deadline) {
            String leaderId = tryFindLeaderId();
            if (leaderId != null) {
                return leaderId;
            }
            Thread.sleep(50L);
        }
        throw new AssertionError("leader is not elected");
    }

    @Override
    public String awaitLeaderElectedExcluding(String excludedNodeId, long timeoutMillis) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (System.currentTimeMillis() < deadline) {
            for (String nodeId : getNodeIds()) {
                if (excludedNodeId != null && excludedNodeId.equals(nodeId)) {
                    continue;
                }
                NodeRuntime runtime = runtimeByNodeId.get(nodeId);
                if (runtime == null || !runtime.running || runtime.node == null) {
                    continue;
                }
                if (runtime.node.getRole() == RaftRoleType.LEADER) {
                    return nodeId;
                }
            }
            Thread.sleep(50L);
        }
        throw new AssertionError("leader is not elected excluding node=" + excludedNodeId);
    }

    public RaftProposeResult proposeOnLeader(byte[] commandData, long timeoutMillis) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        Exception lastException = null;
        while (System.currentTimeMillis() < deadline) {
            try {
                String leaderId = awaitLeaderElected(3000L);
                NodeRuntime leaderRuntime = runtimeByNodeId.get(leaderId);
                if (leaderRuntime == null || !leaderRuntime.running || leaderRuntime.node == null) {
                    Thread.sleep(50L);
                    continue;
                }
                byte[] proposeData = toEtcdCommandBytes(leaderId, commandData);
                RaftProposeResult result = leaderRuntime.node.propose(proposeData).get(3, TimeUnit.SECONDS);
                if (result != null && result.isAccepted()) {
                    return result;
                }
            } catch (Exception e) {
                lastException = e;
            }
            Thread.sleep(80L);
        }
        throw new AssertionError("raft propose timeout", lastException);
    }

    /**
     * 将 Raft 网络测试输入转换为 EtcdCommand 字节，确保 EtcdNode 能推进 apply 边界并触发快照流程。
     */
    private byte[] toEtcdCommandBytes(String leaderId, byte[] commandData) {
        long sequence = proposeSequence.incrementAndGet();
        PutRequest request = new PutRequest();
        request.setKey("raft/network/propose/" + leaderId + "/" + sequence);
        if (commandData == null || commandData.length == 0) {
            request.setValue("empty");
        } else {
            request.setValue(Base64.getEncoder().encodeToString(commandData));
        }
        EtcdCommand command = new EtcdCommand();
        command.setType(EtcdCommandType.PUT);
        command.setCommandId("raft-network-command-" + sequence);
        command.setData(request);
        return commandCodec.encodeEtcdCommand(command);
    }

    public void awaitCommitIndexAtLeastOnQuorum(long expectedCommitIndex, long timeoutMillis) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (System.currentTimeMillis() < deadline) {
            int matchedCount = 0;
            for (NodeRuntime runtime : runtimeByNodeId.values()) {
                if (!runtime.running || runtime.node == null) {
                    continue;
                }
                if (runtime.node.getRaftNode().getCommitIndex() >= expectedCommitIndex) {
                    matchedCount++;
                }
            }
            if (matchedCount >= quorumSize()) {
                return;
            }
            Thread.sleep(50L);
        }
        throw new AssertionError("commit index is not replicated to quorum, expected=" + expectedCommitIndex);
    }

    public void awaitCommitIndexAtLeastOnAllRunningNodes(long expectedCommitIndex, long timeoutMillis) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (System.currentTimeMillis() < deadline) {
            boolean allMatched = true;
            for (NodeRuntime runtime : runtimeByNodeId.values()) {
                if (!runtime.running || runtime.node == null) {
                    continue;
                }
                if (runtime.node.getRaftNode().getCommitIndex() < expectedCommitIndex) {
                    allMatched = false;
                    break;
                }
            }
            if (allMatched) {
                return;
            }
            Thread.sleep(50L);
        }
        throw new AssertionError("commit index is not replicated to all running nodes, expected=" + expectedCommitIndex);
    }

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

    @Override
    public void healAllNetworkIsolation() {
        registerAllEndpoints();
    }

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

    public boolean hasPersistedSnapshot(String nodeId) {
        RaftPersistentState state = getPersistentState(nodeId);
        return state != null && state.getSnapshot() != null;
    }

    public void awaitPersistedSnapshotOnNode(String nodeId, long timeoutMillis) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (System.currentTimeMillis() < deadline) {
            if (hasPersistedSnapshot(nodeId)) {
                return;
            }
            Thread.sleep(50L);
        }
        throw new AssertionError("snapshot is not persisted on node, nodeId=" + nodeId);
    }

    public long getCommitIndex(String nodeId) {
        NodeRuntime runtime = runtimeByNodeId.get(nodeId);
        if (runtime == null || runtime.node == null) {
            return 0L;
        }
        return runtime.node.getRaftNode().getCommitIndex();
    }

    private String tryFindLeaderId() {
        for (String nodeId : getNodeIds()) {
            NodeRuntime runtime = runtimeByNodeId.get(nodeId);
            if (runtime == null || !runtime.running || runtime.node == null) {
                continue;
            }
            if (runtime.node.getRole() == RaftRoleType.LEADER) {
                return nodeId;
            }
        }
        return null;
    }

    private void createRuntime(String nodeId, List<String> peerNodeIds) throws Exception {
        int port = findFreePort();
        NodeEndpoint endpoint = new NodeEndpoint(nodeId, "127.0.0.1", port);

        Storage storage = new MemoryStorage();
        RaftConfig raftConfig = new RaftConfig();
        raftConfig.setElectionTimeoutTicks(10);
        raftConfig.setHeartbeatTimeoutTicks(3);
        raftConfig.setSnapshotTriggerLogCount(snapshotTriggerLogCount);
        raftConfig.setPeerNodeIds(new ArrayList<>(peerNodeIds));

        runtimeByNodeId.put(nodeId, new NodeRuntime(nodeId, endpoint, storage, raftConfig));
    }

    private void registerAllEndpoints() {
        for (NodeRuntime sourceRuntime : runtimeByNodeId.values()) {
            if (!sourceRuntime.running || sourceRuntime.node == null) {
                continue;
            }
            for (NodeRuntime targetRuntime : runtimeByNodeId.values()) {
                sourceRuntime.node.registerNodeEndpoint(targetRuntime.endpoint);
            }
        }
    }

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
        private final RaftConfig raftConfig;

        private EtcdNode node;
        private NettyRpcServer rpcServer;
        private RpcClient raftRpcClient;
        private volatile boolean running;

        private NodeRuntime(String nodeId, NodeEndpoint endpoint, Storage storage, RaftConfig raftConfig) {
            this.nodeId = nodeId;
            this.endpoint = endpoint;
            this.storage = storage;
            this.raftConfig = raftConfig;
            this.running = false;
        }

        private void start() throws Exception {
            if (running) {
                return;
            }
            raftRpcClient = new NettyRpcClient(serializer, RPC_TIMEOUT_MILLIS);
            node = new EtcdNode(nodeId, raftConfig, storage, serializer, raftRpcClient);

            rpcServer = new NettyRpcServer(endpoint, serializer);
            rpcServer.registerService(
                    EtcdNode.RPC_SERVICE_NAME,
                    node,
                    EtcdNode.HANDLE_RAFT_RPC_REQUEST_VOTE_REQUEST_METHOD_NAME,
                    EtcdNode.HANDLE_RAFT_RPC_REQUEST_VOTE_RESPONSE_METHOD_NAME,
                    EtcdNode.HANDLE_RAFT_RPC_APPEND_ENTRIES_REQUEST_METHOD_NAME,
                    EtcdNode.HANDLE_RAFT_RPC_APPEND_ENTRIES_RESPONSE_METHOD_NAME,
                    EtcdNode.HANDLE_RAFT_RPC_INSTALL_SNAPSHOT_REQUEST_METHOD_NAME,
                    EtcdNode.HANDLE_RAFT_RPC_INSTALL_SNAPSHOT_RESPONSE_METHOD_NAME);
            rpcServer.start();

            node.start();
            running = true;
        }

        private void stop() {
            if (!running) {
                return;
            }
            running = false;
            if (node != null) {
                node.stop();
            }
            if (rpcServer != null) {
                rpcServer.stop();
            }
            if (raftRpcClient != null) {
                raftRpcClient.shutdown();
            }
        }
    }
}
