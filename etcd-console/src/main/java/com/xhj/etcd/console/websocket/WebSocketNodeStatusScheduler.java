package com.xhj.etcd.console.websocket;

import com.xhj.etcd.console.model.response.websocket.WebSocketMessageType;
import com.xhj.etcd.console.service.ConnectionService;
import com.xhj.etcd.kernel.etcd.etcdrpc.NodeStatusRequest;
import com.xhj.etcd.kernel.etcd.etcdrpc.NodeStatusResponse;
import com.xhj.etcd.rpc.NodeEndpoint;
import com.xhj.etcd.sdk.client.EtcdClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * WebSocketNodeStatusScheduler
 *
 * @author XJks
 * @description 集群状态推送调度器，负责把“连接列表 + 节点状态”按 WebSocket 会话增量推送到前端。
 *
 * <p>核心设计：</p>
 * <ul>
 *     <li>按 session 维度维护快照，不使用全局共享快照，避免 A 会话影响 B 会话。</li>
 *     <li>每轮先统一探测节点状态，再按 session 做差异比较并定向发送。</li>
 *     <li>只推送变化项，减少无效 WS 流量和前端重复渲染。</li>
 * </ul>
 */
@Component
public class WebSocketNodeStatusScheduler {

    // ==================== 依赖组件 ====================
    /**
     * 连接服务，提供连接列表和节点访问入口。
     *
     * <p>该组件职责是“状态广播调度”，不承载业务写入和连接生命周期管理。</p>
     */
    @Autowired
    private ConnectionService connectionService;

    /**
     * WebSocket 推送服务。
     */
    @Autowired
    private ConsoleWebSocketGateway consoleWebSocketGateway;

    // ==================== 会话级快照缓存 ====================
    /**
     * 每个浏览器会话独立保存一份“已发送快照”，用于会话级增量推送。
     *
     * <p>key: sessionId</p>
     * <p>value: 该 session 最近一次收到的 connections/status/error 快照</p>
     */
    private final Map<String, SessionNodeStatusSnapshot> sessionSnapshotBySessionId = new ConcurrentHashMap<>();

    /**
     * 周期推送集群状态到前端。
     *
     * <p>单轮处理流程：</p>
     * <ol>
     *     <li>检查是否存在活跃 session；若无则清空快照并结束本轮。</li>
     *     <li>从 ConnectionService 拿连接列表和 EtcdClient。</li>
     *     <li>按连接列表探测一次当前节点状态（本轮共享读取结果）。</li>
     *     <li>逐 session 对比“当前结果 vs 该 session 的历史快照”，只推送变化项。</li>
     *     <li>清理掉断开 session 和断开节点在快照中的残留键。</li>
     * </ol>
     *
     * <p>消息类型说明：</p>
     * <ul>
     *     <li>{@link WebSocketMessageType#CONNECTIONS}：连接列表变化时发送。</li>
     *     <li>{@link WebSocketMessageType#NODE_STATUS}：某节点状态变化时发送。</li>
     *     <li>{@link WebSocketMessageType#CONSOLE_ERROR}：某节点探测失败且错误消息变化时发送。</li>
     * </ul>
     */
    @Scheduled(fixedDelayString = "${mini-etcd.console.monitor-interval-millis:1000}")
    public void broadcastAllNodeStatus() {
        // 0) 无活跃前端会话时，跳过本轮轮询与广播，避免无意义资源开销。
        if (!consoleWebSocketGateway.hasActiveSession()) {
            sessionSnapshotBySessionId.clear();
            return;
        }
        List<String> activeSessionIdList = consoleWebSocketGateway.listActiveSessionIdList();
        if (activeSessionIdList.isEmpty()) {
            sessionSnapshotBySessionId.clear();
            return;
        }

        // 1) 获取连接列表与客户端入口。
        // connections 是“控制台当前要观察的节点集合”。
        List<NodeEndpoint> connections = connectionService.listConnections();
        EtcdClient etcdClient = null;
        try {
            etcdClient = connectionService.getEtcdClient();
        } catch (Exception ignored) {
            // TODO:控制台尚未建立任何连接时，跳过本轮状态探测，避免定时任务周期刷异常日志。
        }

        // 2) 预计算本轮节点状态快照，避免按 session 重复探测节点。
        // 这里的 currentNodeStatusByNodeId/currentErrorByNodeId 只在本轮有效，不会跨轮次复用。
        Map<String, NodeStatusResponse> currentNodeStatusByNodeId = new HashMap<>();
        Map<String, String> currentErrorByNodeId = new HashMap<>();
        if (etcdClient != null) {
            for (NodeEndpoint endpoint : connections) {
                String nodeId = endpoint.getNodeId();
                try {
                    NodeStatusResponse status = etcdClient.getNodeStatusOnEndpoint(endpoint, new NodeStatusRequest());
                    currentNodeStatusByNodeId.put(nodeId, status);
                } catch (Exception exception) {
                    currentErrorByNodeId.put(nodeId, exception.getMessage());
                }
            }
        }

        // 3) 清理已断开的 session 快照。
        // 防止 session 断开后 snapshot 常驻内存。
        Set<String> activeSessionIdSet = new HashSet<>(activeSessionIdList);
        sessionSnapshotBySessionId.keySet().removeIf(sessionId -> !activeSessionIdSet.contains(sessionId));

        // 4) 按 session 维度判断并推送变更（每个 session 独立快照，不共享状态）。
        for (String sessionId : activeSessionIdList) {
            SessionNodeStatusSnapshot snapshot = sessionSnapshotBySessionId.computeIfAbsent(
                    sessionId,
                    ignoredSessionId -> new SessionNodeStatusSnapshot());

            if (!connections.equals(snapshot.lastConnections)) {
                // 当前 session 的连接列表发生变化才发送 CONNECTIONS。
                consoleWebSocketGateway.sendEventToSession(sessionId, WebSocketMessageType.CONNECTIONS, null, connections);
                snapshot.lastConnections = new ArrayList<>(connections);
            }

            for (NodeEndpoint endpoint : connections) {
                String nodeId = endpoint.getNodeId();
                NodeStatusResponse currentStatus = currentNodeStatusByNodeId.get(nodeId);
                String currentError = currentErrorByNodeId.get(nodeId);

                if (currentStatus != null) {
                    // 节点探测成功：比较 status 变更，变化才发送 NODE_STATUS。
                    NodeStatusResponse previousStatus = snapshot.lastStatusByNodeId.get(nodeId);
                    if (!currentStatus.equals(previousStatus)) {
                        consoleWebSocketGateway.sendEventToSession(
                                sessionId,
                                WebSocketMessageType.NODE_STATUS,
                                nodeId,
                                currentStatus);
                        snapshot.lastStatusByNodeId.put(nodeId, currentStatus);
                    }
                    snapshot.lastErrorByNodeId.remove(nodeId);
                } else if (currentError != null) {
                    // 节点探测失败：仅当错误消息变化时发送 CONSOLE_ERROR，避免每秒重复刷同一错误。
                    String previousError = snapshot.lastErrorByNodeId.get(nodeId);
                    if (!currentError.equals(previousError)) {
                        consoleWebSocketGateway.sendEventToSession(
                                sessionId,
                                WebSocketMessageType.CONSOLE_ERROR,
                                nodeId,
                                currentError);
                        snapshot.lastErrorByNodeId.put(nodeId, currentError);
                    }
                    snapshot.lastStatusByNodeId.remove(nodeId);
                }
            }

            // 清理断开节点残留，防止后续误判“未变化”。
            Set<String> currentNodeIdSet = new HashSet<>();
            for (NodeEndpoint endpoint : connections) {
                currentNodeIdSet.add(endpoint.getNodeId());
            }
            snapshot.lastStatusByNodeId.keySet().removeIf(nodeId -> !currentNodeIdSet.contains(nodeId));
            snapshot.lastErrorByNodeId.keySet().removeIf(nodeId -> !currentNodeIdSet.contains(nodeId));
        }
    }

    /**
     * SessionNodeStatusSnapshot
     *
     * @author XJks
     * @description 单个 WebSocket 会话的状态广播快照。
     */
    private static class SessionNodeStatusSnapshot {

        /**
         * 该会话最后一次收到的连接列表。
         *
         * <p>用于判断是否需要再次发送 CONNECTIONS。</p>
         */
        private List<NodeEndpoint> lastConnections = new ArrayList<>();

        /**
         * 该会话最后一次收到的节点状态。
         *
         * <p>key: nodeId</p>
         * <p>value: 上次已发送给该 session 的 NodeStatusResponse</p>
         */
        private final Map<String, NodeStatusResponse> lastStatusByNodeId = new HashMap<>();

        /**
         * 该会话最后一次收到的节点错误消息。
         *
         * <p>用于错误去重：同一错误文本不重复推送。</p>
         */
        private final Map<String, String> lastErrorByNodeId = new HashMap<>();
    }
}
