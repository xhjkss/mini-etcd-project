package com.xhj.etcd.kernel.etcd.bootstrap;

import com.xhj.etcd.kernel.etcd.node.EtcdNode;
import com.xhj.etcd.kernel.raft.core.RaftConfig;
import com.xhj.etcd.rpc.NodeEndpoint;
import com.xhj.etcd.rpc.RpcClient;
import com.xhj.etcd.rpc.netty.NettyRpcClient;
import com.xhj.etcd.rpc.netty.NettyRpcServer;
import com.xhj.etcd.serializer.Serializer;
import com.xhj.etcd.serializer.SerializerRegistry;
import com.xhj.etcd.storage.Storage;
import com.xhj.etcd.storage.file.FileStorage;

import java.io.File;
import java.lang.management.ManagementFactory;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * MiniEtcdNodeLauncher
 *
 * @author XJks
 * @description mini-etcd 节点命令行启动入口，负责解析参数并启动单个 EtcdNode + NettyRpcServer 运行时。
 *
 * <p>参数约定：</p>
 * <ul>
 *     <li>必填：--nodeId= --host= --port= --peerEndpoints= --dataDir=</li>
 *     <li>可选：--electionTimeoutTicks= --heartbeatTimeoutTicks= --snapshotTriggerLogCount=</li>
 * </ul>
 *
 * <p>peerEndpoints 格式：</p>
 * <p>nodeId@host:port,nodeId@host:port,...</p>
 */
public class MiniEtcdNodeLauncher {

    // ==================== 默认参数 ====================

    /**
     * Raft RPC 调用默认超时，单位：毫秒。
     */
    private static final long DEFAULT_RPC_TIMEOUT_MILLIS = 5000L;

    // ==================== 运行时组件 ====================

    /**
     * 用于发送节点间 Raft RPC 的客户端。
     */
    private RpcClient raftRpcClient;

    /**
     * 当前节点对外暴露 RPC 的服务端。
     */
    private NettyRpcServer rpcServer;

    /**
     * mini-etcd 节点核心对象，承载 raft/event-loop/mvcc/watch 等运行逻辑。
     */
    private EtcdNode etcdNode;

    /**
     * 是否已经进入 stop 流程。
     *
     * <p>用于保证 stop 是幂等的，避免 shutdown hook 与外部 stop 重入时重复释放资源。</p>
     */
    private final AtomicBoolean stopStarted = new AtomicBoolean(false);

    /**
     * 节点主线程阻塞门闩。
     *
     * <p>main 线程在启动完成后阻塞等待，直到 stop 流程触发后再退出。</p>
     */
    private final CountDownLatch processStopLatch = new CountDownLatch(1);

    /**
     * 当前节点运行时 PID 文件路径。
     *
     * <p>仅当用户传入 --pidFile 参数时才会写入该字段。</p>
     */
    private Path runtimePidFilePath;

    public static void main(String[] args) {
        MiniEtcdNodeLauncher launcher = new MiniEtcdNodeLauncher();
        try {
            launcher.startAndBlock(args);
        } catch (Exception exception) {
            System.err.println("[mini-etcd] node start failed: " + exception.getMessage());
            exception.printStackTrace(System.err);
            System.exit(1);
        }
    }

    // ==================== 启动流程 ====================

    /**
     * 启动节点并阻塞当前线程，直到进程退出。
     *
     * <p>启动顺序：</p>
     * <ol>
     *     <li>先解析参数并构造 Storage/RaftConfig。</li>
     *     <li>再创建 EtcdNode 与 NettyRpcServer 并注册 RPC 方法。</li>
     *     <li>先启动 RPC server，再启动 EtcdNode。</li>
     *     <li>最后注册 shutdown hook，并阻塞当前线程等待 stop。</li>
     * </ol>
     */
    private void startAndBlock(String[] args) throws Exception {
        NodeLaunchArguments launchArguments = parseAndValidateArgs(args);
        Serializer serializer = SerializerRegistry.getDefaultSerializer();

        // 1) dataDir 对应 FileStorage，节点重启后可复用同一目录恢复状态。
        Storage storage = new FileStorage(new File(launchArguments.dataDir));
        RaftConfig raftConfig = buildRaftConfig(launchArguments);

        // 2) raftRpcClient 用于节点间 Raft 消息发送，rpcServer 用于对外提供 Etcd/Raft RPC 入口。
        raftRpcClient = new NettyRpcClient(serializer, DEFAULT_RPC_TIMEOUT_MILLIS);
        etcdNode = new EtcdNode(launchArguments.nodeId, raftConfig, storage, serializer, raftRpcClient);
        rpcServer = new NettyRpcServer(new NodeEndpoint(launchArguments.nodeId, launchArguments.host, launchArguments.port), serializer);

        // 3) 注册 peer endpoint 与 rpc 方法，确保节点启动后能够立即参与 raft 网络交互。
        registerPeerEndpoints(etcdNode, launchArguments.peerEndpointList);
        registerRpcMethods(rpcServer, etcdNode);

        // 4) 先启动 RPC server 再启动 EtcdNode，避免节点已启动但服务端尚未监听导致初始网络请求丢失。
        rpcServer.start();
        etcdNode.start();

        // 5) 由节点进程主动写 PID 文件，供 cmd/sh 脚本做进程管理。
        registerPidFileIfPresent(launchArguments.pidFile);
        printBootMessage(launchArguments);
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                stopQuietly();
            }
        }, "mini-etcd-node-shutdown-" + launchArguments.nodeId));

        processStopLatch.await();
    }

    /**
     * 构造 Raft 配置。
     */
    private RaftConfig buildRaftConfig(NodeLaunchArguments launchArguments) {
        RaftConfig raftConfig = new RaftConfig();
        raftConfig.setElectionTimeoutTicks(launchArguments.electionTimeoutTicks);
        raftConfig.setHeartbeatTimeoutTicks(launchArguments.heartbeatTimeoutTicks);
        raftConfig.setSnapshotTriggerLogCount(launchArguments.snapshotTriggerLogCount);

        List<String> peerNodeIds = new ArrayList<>();
        for (NodeEndpoint endpoint : launchArguments.peerEndpointList) {
            if (!launchArguments.nodeId.equals(endpoint.getNodeId())) {
                peerNodeIds.add(endpoint.getNodeId());
            }
        }
        raftConfig.setPeerNodeIds(peerNodeIds);
        return raftConfig;
    }

    /**
     * 注册节点 endpoint 映射，供 EtcdNode 发送 Raft RPC 使用。
     */
    private void registerPeerEndpoints(EtcdNode node, List<NodeEndpoint> endpointList) {
        for (NodeEndpoint endpoint : endpointList) {
            node.registerNodeEndpoint(endpoint);
        }
    }

    /**
     * 注册 Etcd 与 Raft RPC 对外方法。
     */
    private void registerRpcMethods(NettyRpcServer server, EtcdNode node) {
        server.registerService(EtcdNode.RPC_SERVICE_NAME,
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
    }

    /**
     * 打印启动信息。
     */
    private void printBootMessage(NodeLaunchArguments launchArguments) {
        System.out.println("[mini-etcd] node started.");
        System.out.println("[mini-etcd] nodeId=" + launchArguments.nodeId);
        System.out.println("[mini-etcd] endpoint=" + launchArguments.host + ":" + launchArguments.port);
        System.out.println("[mini-etcd] dataDir=" + launchArguments.dataDir);
        System.out.println("[mini-etcd] peerEndpoints=" + launchArguments.rawPeerEndpoints);
        System.out.println("[mini-etcd] electionTimeoutTicks=" + launchArguments.electionTimeoutTicks
                + ", heartbeatTimeoutTicks=" + launchArguments.heartbeatTimeoutTicks
                + ", snapshotTriggerLogCount=" + launchArguments.snapshotTriggerLogCount);
    }

    /**
     * 停机流程（幂等）。
     *
     * <p>停止顺序：</p>
     * <ol>
     *     <li>先 stop EtcdNode，停止内部 event-loop。</li>
     *     <li>再 stop NettyRpcServer，关闭对外监听。</li>
     *     <li>最后 shutdown raftRpcClient。</li>
     * </ol>
     */
    private void stopQuietly() {
        if (!stopStarted.compareAndSet(false, true)) {
            return;
        }

        if (etcdNode != null) {
            try {
                etcdNode.stop();
            } catch (Exception ignore) {
            }
        }
        if (rpcServer != null) {
            try {
                rpcServer.stop();
            } catch (Exception ignore) {
            }
        }
        if (raftRpcClient != null) {
            try {
                raftRpcClient.shutdown();
            } catch (Exception ignore) {
            }
        }
        clearPidFileQuietly();
        processStopLatch.countDown();
    }

    // ==================== 参数解析 ====================

    /**
     * 解析并校验启动参数。
     */
    private NodeLaunchArguments parseAndValidateArgs(String[] args) {
        Map<String, String> namedArgumentMap = parseNamedArgumentMap(args);

        NodeLaunchArguments launchArguments = new NodeLaunchArguments();
        launchArguments.nodeId = requireArgument(namedArgumentMap, "nodeId");
        launchArguments.host = requireArgument(namedArgumentMap, "host");
        launchArguments.port = parsePort(requireArgument(namedArgumentMap, "port"));
        launchArguments.dataDir = requireArgument(namedArgumentMap, "dataDir");
        launchArguments.rawPeerEndpoints = requireArgument(namedArgumentMap, "peerEndpoints");
        launchArguments.peerEndpointList = parsePeerEndpointList(launchArguments.rawPeerEndpoints);
        launchArguments.pidFile = normalizeOptionalArgument(namedArgumentMap.get("pidFile"));

        launchArguments.electionTimeoutTicks = parsePositiveInt(namedArgumentMap.get("electionTimeoutTicks"), 10, "electionTimeoutTicks");
        launchArguments.heartbeatTimeoutTicks = parsePositiveInt(namedArgumentMap.get("heartbeatTimeoutTicks"), 3, "heartbeatTimeoutTicks");
        launchArguments.snapshotTriggerLogCount = parsePositiveInt(namedArgumentMap.get("snapshotTriggerLogCount"), 50, "snapshotTriggerLogCount");

        validatePeerEndpointList(launchArguments);
        return launchArguments;
    }

    /**
     * 解析命令行参数，支持 --key=value 形式。
     *
     * @param args 命令行参数数组
     * @return 参数名 -> 参数值映射
     */
    private Map<String, String> parseNamedArgumentMap(String[] args) {
        Map<String, String> namedArgumentMap = new LinkedHashMap<>();
        if (args == null) {
            return namedArgumentMap;
        }
        for (String arg : args) {
            if (arg == null || arg.trim().length() == 0) {
                continue;
            }
            String trimmedArg = arg.trim();
            if (!trimmedArg.startsWith("--")) {
                throw new IllegalArgumentException("invalid argument format: " + trimmedArg + ", expected --key=value");
            }
            int index = trimmedArg.indexOf('=');
            if (index <= 2 || index == trimmedArg.length() - 1) {
                throw new IllegalArgumentException("invalid argument format: " + trimmedArg + ", expected --key=value");
            }
            String key = trimmedArg.substring(2, index).trim();
            String value = trimmedArg.substring(index + 1).trim();
            if (key.length() == 0 || value.length() == 0) {
                throw new IllegalArgumentException("invalid argument format: " + trimmedArg + ", expected --key=value");
            }
            namedArgumentMap.put(key, value);
        }
        return namedArgumentMap;
    }

    /**
     * 获取必填参数。
     */
    private String requireArgument(Map<String, String> namedArgumentMap, String key) {
        String value = namedArgumentMap.get(key);
        if (value == null || value.trim().length() == 0) {
            throw new IllegalArgumentException("missing required argument: --" + key + "=...");
        }
        return value.trim();
    }

    /**
     * 解析端口。
     */
    private int parsePort(String value) {
        int port;
        try {
            port = Integer.parseInt(value);
        } catch (Exception exception) {
            throw new IllegalArgumentException("invalid port: " + value);
        }
        if (port <= 0 || port > 65535) {
            throw new IllegalArgumentException("invalid port: " + value);
        }
        return port;
    }

    /**
     * 解析正整数参数；参数为空时返回默认值。
     */
    private int parsePositiveInt(String value, int defaultValue, String argumentName) {
        if (value == null || value.trim().length() == 0) {
            return defaultValue;
        }
        int parsed;
        try {
            parsed = Integer.parseInt(value.trim());
        } catch (Exception exception) {
            throw new IllegalArgumentException("invalid argument --" + argumentName + ": " + value);
        }
        if (parsed <= 0) {
            throw new IllegalArgumentException("invalid argument --" + argumentName + ": " + value + ", must be > 0");
        }
        return parsed;
    }

    /**
     * 规范化可选参数值。
     */
    private String normalizeOptionalArgument(String value) {
        if (value == null) {
            return null;
        }
        String trimmedValue = value.trim();
        return trimmedValue.length() == 0 ? null : trimmedValue;
    }

    /**
     * 解析 nodeId@host:port 逗号列表。
     */
    private List<NodeEndpoint> parsePeerEndpointList(String peerEndpoints) {
        List<NodeEndpoint> endpointList = new ArrayList<>();
        String[] items = peerEndpoints.split(",");
        for (String item : items) {
            if (item == null || item.trim().length() == 0) {
                continue;
            }
            String normalizedItem = item.trim();
            int atIndex = normalizedItem.indexOf('@');
            int colonIndex = normalizedItem.lastIndexOf(':');
            if (atIndex <= 0 || colonIndex <= atIndex + 1 || colonIndex == normalizedItem.length() - 1) {
                throw new IllegalArgumentException("invalid peer endpoint: " + normalizedItem + ", expected nodeId@host:port");
            }

            String nodeId = normalizedItem.substring(0, atIndex).trim();
            String host = normalizedItem.substring(atIndex + 1, colonIndex).trim();
            String portText = normalizedItem.substring(colonIndex + 1).trim();
            if (nodeId.length() == 0 || host.length() == 0 || portText.length() == 0) {
                throw new IllegalArgumentException("invalid peer endpoint: " + normalizedItem + ", expected nodeId@host:port");
            }

            int port = parsePort(portText);
            endpointList.add(new NodeEndpoint(nodeId, host, port));
        }
        if (endpointList.isEmpty()) {
            throw new IllegalArgumentException("peerEndpoints must not be empty");
        }
        return endpointList;
    }

    /**
     * 校验 peer endpoint 集合与当前节点参数一致性。
     */
    private void validatePeerEndpointList(NodeLaunchArguments launchArguments) {
        Map<String, NodeEndpoint> endpointMap = new LinkedHashMap<>();
        for (NodeEndpoint endpoint : launchArguments.peerEndpointList) {
            if (endpointMap.containsKey(endpoint.getNodeId())) {
                throw new IllegalArgumentException("duplicate peer nodeId: " + endpoint.getNodeId());
            }
            endpointMap.put(endpoint.getNodeId(), endpoint);
        }

        NodeEndpoint currentEndpoint = endpointMap.get(launchArguments.nodeId);
        if (currentEndpoint != null) {
            if (!launchArguments.host.equals(currentEndpoint.getHost()) || launchArguments.port != currentEndpoint.getPort()) {
                throw new IllegalArgumentException("current node endpoint mismatch, nodeId=" + launchArguments.nodeId
                        + ", expected " + launchArguments.host + ":" + launchArguments.port
                        + ", but peerEndpoints contains " + currentEndpoint.endpointKey());
            }
        } else {
            launchArguments.peerEndpointList.add(new NodeEndpoint(launchArguments.nodeId, launchArguments.host, launchArguments.port));
        }
    }

    /**
     * 将当前进程 PID 写入外部指定文件。
     */
    private void registerPidFileIfPresent(String pidFile) throws Exception {
        if (pidFile == null || pidFile.trim().length() == 0) {
            return;
        }
        runtimePidFilePath = new File(pidFile).toPath();
        Path parent = runtimePidFilePath.getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }
        String processId = currentProcessId();
        Files.write(runtimePidFilePath, processId.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * 安静删除 PID 文件。
     */
    private void clearPidFileQuietly() {
        if (runtimePidFilePath == null) {
            return;
        }
        try {
            Files.deleteIfExists(runtimePidFilePath);
        } catch (Exception ignore) {
        }
    }

    /**
     * 获取当前 Java 进程 PID。
     */
    private String currentProcessId() {
        String runtimeName = ManagementFactory.getRuntimeMXBean().getName();
        int splitIndex = runtimeName.indexOf('@');
        if (splitIndex <= 0) {
            return runtimeName;
        }
        return runtimeName.substring(0, splitIndex);
    }

    /**
     * NodeLaunchArguments
     *
     * @author XJks
     * @description 启动参数模型。
     */
    private static class NodeLaunchArguments {
        /**
         * 当前节点 ID。
         */
        private String nodeId;

        /**
         * 当前节点 host。
         */
        private String host;

        /**
         * 当前节点端口。
         */
        private int port;

        /**
         * 当前节点数据目录（FileStorage 根目录）。
         */
        private String dataDir;

        /**
         * 原始 peerEndpoints 参数文本，用于启动日志打印。
         */
        private String rawPeerEndpoints;

        /**
         * 可选 PID 文件路径。
         */
        private String pidFile;

        /**
         * 解析后的 peer endpoint 列表。
         */
        private List<NodeEndpoint> peerEndpointList = new ArrayList<>();

        /**
         * 选举超时 tick。
         */
        private int electionTimeoutTicks;

        /**
         * 心跳间隔 tick。
         */
        private int heartbeatTimeoutTicks;

        /**
         * 快照触发阈值。
         */
        private int snapshotTriggerLogCount;
    }
}
