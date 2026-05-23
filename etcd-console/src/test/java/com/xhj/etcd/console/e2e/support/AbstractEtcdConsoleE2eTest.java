package com.xhj.etcd.console.e2e.support;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.xhj.etcd.kernel.etcd.node.EtcdNode;
import com.xhj.etcd.kernel.raft.core.RaftConfig;
import com.xhj.etcd.rpc.NodeEndpoint;
import com.xhj.etcd.rpc.RpcClient;
import com.xhj.etcd.rpc.netty.NettyRpcClient;
import com.xhj.etcd.rpc.netty.NettyRpcServer;
import com.xhj.etcd.serializer.Serializer;
import com.xhj.etcd.serializer.SerializerRegistry;
import com.xhj.etcd.storage.Storage;
import com.xhj.etcd.storage.memory.MemoryStorage;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;

import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * AbstractEtcdConsoleE2eTest
 *
 * @author XJks
 * @description Console E2E 测试公共骨架，统一管理 3 节点集群、HTTP 调用和基础断言。
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class AbstractEtcdConsoleE2eTest {

    // ==================== 注入组件 ====================
    /**
     * 控制台 HTTP 端口。
     */
    @LocalServerPort
    protected int serverPort;

    /**
     * 控制台 HTTP 客户端。
     */
    @Autowired
    protected TestRestTemplate restTemplate;

    /**
     * JSON 编解码器。
     */
    @Autowired
    protected ObjectMapper objectMapper;

    // ==================== 集群运行时 ====================
    /**
     * 测试使用的 mini-etcd 三节点集群。
     */
    protected MiniEtcdCluster cluster;

    /**
     * 启动三节点集群。
     */
    @BeforeAll
    public void startCluster() throws Exception {
        cluster = new MiniEtcdCluster();
        cluster.startThreeNodeCluster();
    }

    /**
     * 停止三节点集群。
     */
    @AfterAll
    public void stopCluster() {
        if (cluster != null) {
            cluster.close();
        }
    }

    /**
     * 连接全部三节点到 console。
     */
    protected void connectAllNodes() throws Exception {
        connectNodeByPort(endpointByNodeId("n1").getPort());
        connectNodeByPort(endpointByNodeId("n2").getPort());
        connectNodeByPort(endpointByNodeId("n3").getPort());
    }

    /**
     * 读取指定 nodeId 的 endpoint。
     */
    protected NodeEndpoint endpointByNodeId(String nodeId) {
        return cluster.endpointByNodeId(nodeId);
    }

    /**
     * 按 nodeId 构造 endpoint 查询参数（host + port）。
     */
    protected String endpointQueryByNodeId(String nodeId) {
        NodeEndpoint endpoint = endpointByNodeId(nodeId);
        return "host=" + endpoint.getHost() + "&port=" + endpoint.getPort();
    }

    /**
     * 连接单节点（幂等）。
     *
     * <p>当同一测试 JVM 复用 Spring 上下文时，连接管理状态可能跨测试类残留；
     * 若返回“already connected”，这里视为成功，避免无意义失败。</p>
     */
    protected void connectNodeByPort(int port) throws Exception {
        JsonNode response = postJson("/api/connections", connectBody(port));
        if (response != null && response.get("code") != null && response.get("code").asInt() == 0) {
            return;
        }
        String message = response == null || response.get("message") == null ? "" : response.get("message").asText();
        if (message.contains("already connected")) {
            return;
        }
        assertSuccess(response);
    }

    /**
     * 等待 leader 节点就绪并返回 nodeId。
     */
    protected String awaitLeaderNodeId() throws Exception {
        long deadline = System.currentTimeMillis() + 10000L;
        Exception lastException = null;
        while (System.currentTimeMillis() < deadline) {
            try {
                JsonNode statusResponse = getJson("/api/cluster/node-status/on-all-nodes");
                JsonNode nodeStatusResponseList = statusResponse.get("data");
                if (nodeStatusResponseList != null) {
                    for (JsonNode nodeStatusResponse : nodeStatusResponseList) {
                        if (nodeStatusResponse != null && "LEADER".equals(nodeStatusResponse.get("role").asText())) {
                            return nodeStatusResponse.get("nodeId").asText();
                        }
                    }
                }
            } catch (Exception exception) {
                lastException = exception;
            }
            Thread.sleep(200L);
        }
        throw new AssertionError("leader node is not ready", lastException);
    }

    /**
     * POST JSON 请求。
     */
    protected JsonNode postJson(String path, String body) throws Exception {
        ResponseEntity<String> response = restTemplate.postForEntity(baseUrl(path), jsonEntity(body), String.class);
        assertEquals(200, response.getStatusCodeValue());
        return objectMapper.readTree(response.getBody());
    }

    /**
     * GET JSON 请求。
     */
    protected JsonNode getJson(String path) throws Exception {
        ResponseEntity<String> response = restTemplate.getForEntity(baseUrl(path), String.class);
        assertEquals(200, response.getStatusCodeValue());
        return objectMapper.readTree(response.getBody());
    }

    /**
     * DELETE JSON 请求。
     */
    protected JsonNode deleteJson(String path) throws Exception {
        ResponseEntity<String> response = restTemplate.exchange(baseUrl(path), HttpMethod.DELETE, jsonEntity(null), String.class);
        assertEquals(200, response.getStatusCodeValue());
        return objectMapper.readTree(response.getBody());
    }

    /**
     * DELETE JSON 请求（带 body）。
     */
    protected JsonNode deleteJson(String path, String body) throws Exception {
        ResponseEntity<String> response = restTemplate.exchange(baseUrl(path), HttpMethod.DELETE, jsonEntity(body), String.class);
        assertEquals(200, response.getStatusCodeValue());
        return objectMapper.readTree(response.getBody());
    }

    /**
     * 统一成功断言。
     */
    protected void assertSuccess(JsonNode response) {
        assertNotNull(response);
        assertEquals(0, response.get("code").asInt(), "response=" + response.toString());
    }

    /**
     * 构造连接请求 JSON。
     */
    protected String connectBody(int port) throws Exception {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("host", "127.0.0.1");
        body.put("port", port);
        return objectMapper.writeValueAsString(body);
    }

    /**
     * 构造断连请求 JSON（host + port）。
     */
    protected String disconnectBodyByNodeId(String nodeId) throws Exception {
        NodeEndpoint endpoint = endpointByNodeId(nodeId);
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("host", endpoint.getHost());
        body.put("port", endpoint.getPort());
        return objectMapper.writeValueAsString(body);
    }

    /**
     * 构造 JSON 请求实体。
     */
    private HttpEntity<String> jsonEntity(String body) {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        return new HttpEntity<>(body, headers);
    }

    /**
     * 基础地址。
     */
    private String baseUrl(String path) {
        return "http://127.0.0.1:" + serverPort + path;
    }

    /**
     * MiniEtcdCluster
     *
     * @author XJks
     * @description Console E2E 测试使用的最小三节点 Etcd+Raft+RPC 运行时。
     */
    protected static class MiniEtcdCluster {

        /**
         * Raft RPC 超时时间。
         */
        private static final long RPC_TIMEOUT_MILLIS = 5000L;

        /**
         * 默认序列化器。
         */
        private final Serializer serializer = SerializerRegistry.getDefaultSerializer();

        /**
         * nodeId -> runtime。
         */
        private final Map<String, NodeRuntime> runtimeByNodeId = new LinkedHashMap<>();

        /**
         * 启动 3 节点集群。
         */
        private void startThreeNodeCluster() throws Exception {
            if (!runtimeByNodeId.isEmpty()) {
                throw new IllegalStateException("cluster already started");
            }

            List<String> nodeIds = new ArrayList<>();
            nodeIds.add("n1");
            nodeIds.add("n2");
            nodeIds.add("n3");

            for (String nodeId : nodeIds) {
                NodeEndpoint endpoint = new NodeEndpoint(nodeId, "127.0.0.1", findFreePort());
                List<String> peerNodeIds = new ArrayList<>(nodeIds);
                peerNodeIds.remove(nodeId);

                RaftConfig raftConfig = new RaftConfig();
                raftConfig.setElectionTimeoutTicks(10);
                raftConfig.setHeartbeatTimeoutTicks(3);
                raftConfig.setSnapshotTriggerLogCount(50);
                raftConfig.setPeerNodeIds(peerNodeIds);

                runtimeByNodeId.put(nodeId, new NodeRuntime(endpoint, raftConfig));
            }

            for (NodeRuntime runtime : runtimeByNodeId.values()) {
                runtime.start(serializer);
            }
            registerAllEndpoints();
        }

        /**
         * 获取指定 nodeId 的 endpoint。
         */
        private NodeEndpoint endpointByNodeId(String nodeId) {
            NodeRuntime runtime = runtimeByNodeId.get(nodeId);
            if (runtime == null) {
                throw new IllegalArgumentException("node runtime not found: " + nodeId);
            }
            return runtime.endpoint;
        }

        /**
         * 关闭集群。
         */
        private void close() {
            for (NodeRuntime runtime : runtimeByNodeId.values()) {
                runtime.stop();
            }
        }

        /**
         * 注册全部节点 endpoint。
         */
        private void registerAllEndpoints() {
            for (NodeRuntime source : runtimeByNodeId.values()) {
                if (source.node == null) {
                    continue;
                }
                for (NodeRuntime target : runtimeByNodeId.values()) {
                    source.node.registerNodeEndpoint(target.endpoint);
                }
            }
        }

        /**
         * 获取空闲端口。
         */
        private static int findFreePort() throws Exception {
            ServerSocket socket = new ServerSocket(0);
            try {
                return socket.getLocalPort();
            } finally {
                socket.close();
            }
        }

        /**
         * NodeRuntime
         *
         * @author XJks
         * @description 单节点运行时。
         */
        private static class NodeRuntime {

            /**
             * 节点 endpoint。
             */
            private final NodeEndpoint endpoint;

            /**
             * 节点存储。
             */
            private final Storage storage;

            /**
             * Raft 配置。
             */
            private final RaftConfig raftConfig;

            /**
             * Raft RPC 客户端。
             */
            private RpcClient raftRpcClient;

            /**
             * 节点本体。
             */
            private EtcdNode node;

            /**
             * RPC 服务端。
             */
            private NettyRpcServer rpcServer;

            private NodeRuntime(NodeEndpoint endpoint, RaftConfig raftConfig) {
                this.endpoint = endpoint;
                this.raftConfig = raftConfig;
                this.storage = new MemoryStorage();
            }

            /**
             * 启动节点。
             */
            private void start(Serializer serializer) {
                raftRpcClient = new NettyRpcClient(serializer, RPC_TIMEOUT_MILLIS);
                node = new EtcdNode(endpoint.getNodeId(), raftConfig, storage, serializer, raftRpcClient);
                rpcServer = new NettyRpcServer(endpoint, serializer);
                rpcServer.registerService(EtcdNode.RPC_SERVICE_NAME,
                        node,
                        EtcdNode.HANDLE_ETCD_RPC_PUT_REQUEST_METHOD_NAME,
                        EtcdNode.HANDLE_ETCD_RPC_GET_REQUEST_METHOD_NAME,
                        EtcdNode.HANDLE_ETCD_RPC_DELETE_REQUEST_METHOD_NAME,
                        EtcdNode.HANDLE_ETCD_RPC_RANGE_REQUEST_METHOD_NAME,
                        EtcdNode.HANDLE_ETCD_RPC_DELETE_RANGE_REQUEST_METHOD_NAME,
                        EtcdNode.HANDLE_ETCD_RPC_TXN_REQUEST_METHOD_NAME,
                        EtcdNode.HANDLE_ETCD_RPC_COMPACT_REQUEST_METHOD_NAME,
                        EtcdNode.HANDLE_ETCD_RPC_KV_STATE_HASH_REQUEST_METHOD_NAME,
                        EtcdNode.HANDLE_ETCD_RPC_NODE_STATUS_REQUEST_METHOD_NAME,
                        EtcdNode.HANDLE_ETCD_RPC_LEASE_GRANT_REQUEST_METHOD_NAME,
                        EtcdNode.HANDLE_ETCD_RPC_LEASE_KEEP_ALIVE_REQUEST_METHOD_NAME,
                        EtcdNode.HANDLE_ETCD_RPC_LEASE_REVOKE_REQUEST_METHOD_NAME,
                        EtcdNode.HANDLE_ETCD_RPC_LEASE_TTL_REQUEST_METHOD_NAME,
                        EtcdNode.HANDLE_ETCD_RPC_LEASE_LIST_REQUEST_METHOD_NAME,
                        EtcdNode.HANDLE_ETCD_RPC_WATCH_SUBSCRIBE_REQUEST_METHOD_NAME,
                        EtcdNode.HANDLE_ETCD_RPC_WATCH_CANCEL_REQUEST_METHOD_NAME,
                        EtcdNode.HANDLE_RAFT_RPC_REQUEST_VOTE_REQUEST_METHOD_NAME,
                        EtcdNode.HANDLE_RAFT_RPC_REQUEST_VOTE_RESPONSE_METHOD_NAME,
                        EtcdNode.HANDLE_RAFT_RPC_APPEND_ENTRIES_REQUEST_METHOD_NAME,
                        EtcdNode.HANDLE_RAFT_RPC_APPEND_ENTRIES_RESPONSE_METHOD_NAME,
                        EtcdNode.HANDLE_RAFT_RPC_INSTALL_SNAPSHOT_REQUEST_METHOD_NAME,
                        EtcdNode.HANDLE_RAFT_RPC_INSTALL_SNAPSHOT_RESPONSE_METHOD_NAME);
                rpcServer.start();
                node.start();
            }

            /**
             * 停止节点。
             */
            private void stop() {
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
}
