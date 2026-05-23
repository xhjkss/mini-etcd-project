package com.xhj.etcd.console.websocket;

import com.xhj.etcd.console.model.response.websocket.KeyValueChangedPayload;
import com.xhj.etcd.console.model.response.websocket.KeyValueChangeOperationType;
import com.xhj.etcd.console.model.response.websocket.WebSocketMessageType;
import com.xhj.etcd.console.service.ConnectionService;
import com.xhj.etcd.kernel.etcd.etcdrpc.WatchCancelResponse;
import com.xhj.etcd.kernel.etcd.etcdrpc.WatchNotification;
import com.xhj.etcd.kernel.etcd.etcdrpc.WatchSubscribeRequest;
import com.xhj.etcd.kernel.etcd.etcdrpc.WatchSubscribeResponse;
import com.xhj.etcd.rpc.NodeEndpoint;
import com.xhj.etcd.sdk.client.EtcdClient;
import com.xhj.etcd.sdk.client.watch.WatchHandle;
import com.xhj.etcd.sdk.client.watch.WatchListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import javax.annotation.PreDestroy;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * WebSocketNodeKvWatchScheduler
 *
 * @author XJks
 * @description 节点 KV 自动 watch 会话管理器（仅用于浏览器实时刷新）。
 *
 * <p>
 * TODO:
 *  <ul>
 *     <li>本类只维护“系统内置自动 watch”会话：每个 nodeId 最多 1 条 watchHandle。</li>
 *     <li>用途是把节点 MVCC 变更实时推送到浏览器（KV_CHANGED），不承载用户手动 watch。</li>
 *     <li>用户手动 watch 会话由 {@link com.xhj.etcd.console.service.WatchService} 管理。</li>
 *  </ul>
 * </p>
 */
@Component
public class WebSocketNodeKvWatchScheduler {

    // ==================== 常量配置 ====================
    /**
     * 浏览器全量 watch 起始 key。
     */
    private static final String WATCH_KEYSPACE_START = "!";

    /**
     * 浏览器全量 watch 结束 key（开区间）。
     */
    private static final String WATCH_KEYSPACE_END = "\uffff";

    // ==================== 依赖组件 ====================
    /**
     * 连接服务。
     */
    @Autowired
    private ConnectionService connectionService;

    /**
     * WebSocket 推送服务。
     */
    @Autowired
    private ConsoleWebSocketGateway consoleWebSocketGateway;

    // ==================== 运行时状态 ====================
    /**
     * 节点自动 watch 映射：nodeId -> watchHandle。
     *
     * <p>每个节点只保留 1 条自动 watch，用于浏览器实时刷新。</p>
     */
    private final ConcurrentMap<String, WatchHandle> nodeAutoWatchHandleMap = new ConcurrentHashMap<>();

    /**
     * 是否启用浏览器侧自动 watch。
     */
    @Value("${mini-etcd.console.browser-watch-enabled:true}")
    private boolean browserWatchEnabled;

    /**
     * 确保指定连接存在可用 watch 会话。
     *
     * @param nodeId 节点 ID
     */
    public void ensureNodeWatchSessionStarted(String nodeId) {
        // 已关闭自动 watch、nodeId 非法或已存在会话时直接返回，避免重复创建。
        if (!browserWatchEnabled || isBlank(nodeId) || nodeAutoWatchHandleMap.containsKey(nodeId)) {
            return;
        }
        try {
            startNodeWatchSession(nodeId);
        } catch (RuntimeException ignored) {
        }
    }

    /**
     * 停止指定连接的浏览器 watch 会话。
     *
     * @param nodeId 节点 ID
     */
    public void stopNodeWatchSession(String nodeId) {
        WatchHandle watchHandle = nodeAutoWatchHandleMap.remove(nodeId);
        closeWatchHandle(watchHandle);
    }

    /**
     * 周期协调浏览器 watch 会话。
     *
     * <p>处理流程：</p>
     * <ol>
     *     <li>若配置关闭，直接停止全部会话。</li>
     *     <li>若配置开启，确保每个活动连接都有会话。</li>
     *     <li>清理已经失效的连接会话。</li>
     * </ol>
     */
    @Scheduled(
            fixedDelayString = "${mini-etcd.console.browser-watch-reconcile-interval-millis:3000}",
            initialDelayString = "${mini-etcd.console.browser-watch-initial-delay-millis:500}")
    public void reconcileNodeWatchSessions() {
        if (!browserWatchEnabled) {
            stopAllNodeWatchSessions();
            return;
        }

        List<NodeEndpoint> connections = connectionService.listConnections();
        Set<String> activeNodeIdSet = new HashSet<>();
        for (NodeEndpoint endpoint : connections) {
            if (endpoint == null || isBlank(endpoint.getNodeId())) {
                continue;
            }
            activeNodeIdSet.add(endpoint.getNodeId());
            ensureNodeWatchSessionStarted(endpoint.getNodeId());
        }

        for (String watchedNodeId : new ArrayList<>(nodeAutoWatchHandleMap.keySet())) {
            if (!activeNodeIdSet.contains(watchedNodeId)) {
                stopNodeWatchSession(watchedNodeId);
            }
        }
    }

    /**
     * 组件关闭时停止全部浏览器 watch 会话。
     */
    @PreDestroy
    public void stopAllNodeWatchSessions() {
        for (String nodeId : new ArrayList<>(nodeAutoWatchHandleMap.keySet())) {
            stopNodeWatchSession(nodeId);
        }
    }

    /**
     * 启动指定连接的浏览器 watch 会话。
     *
     * @param nodeId 节点 ID
     */
    private void startNodeWatchSession(final String nodeId) {
        EtcdClient etcdClient = connectionService.getEtcdClient();

        // TODO:自动 watch 固定订阅全 key 空间，供前端“数据浏览”页面实时刷新目录列表。这是系统内置观察流，不承载用户手动 watch 语义。
        WatchSubscribeRequest request = new WatchSubscribeRequest();
        request.setStartKey(WATCH_KEYSPACE_START);
        request.setEndKeyExclusive(WATCH_KEYSPACE_END);
        request.setPrefixMatch(false);
        request.setStartRevision(0L);
        request.setMaxEvents(256);
        request.setLeaderOnly(false);

        etcdClient.watch(request, new WatchListener() {
            @Override
            public void onSubscribed(WatchSubscribeResponse response) {
                /**
                 * TODO:
                 *  自动 watch 在 onSubscribed 回调中注册 nodeId -> handle，确保后续通知判断“当前有效句柄”时可见。
                 *  若并发下同一 nodeId 已有自动 watch，则关闭当前重复句柄，保证“一节点一条自动 watch”。
                 */
                WatchHandle currentWatchHandle = getWatchHandle();
                if (currentWatchHandle == null) {
                    return;
                }
                WatchHandle previousWatchHandle = nodeAutoWatchHandleMap.putIfAbsent(nodeId, currentWatchHandle);
                if (previousWatchHandle != null && previousWatchHandle != currentWatchHandle) {
                    closeWatchHandle(currentWatchHandle);
                }
            }

            @Override
            public void onNotification(WatchNotification response) {
                WatchHandle currentWatchHandle = getWatchHandle();
                if (response == null || currentWatchHandle == null || !isCurrentNodeWatchHandle(nodeId, currentWatchHandle)) {
                    return;
                }
                consoleWebSocketGateway.broadcastEvent(
                        WebSocketMessageType.KV_CHANGED,
                        nodeId,
                        buildKeyValueChangedPayload(response));
                if (response.isCanceled()) {
                    removeNodeWatchHandleIfSame(nodeId, currentWatchHandle);
                }
            }

            @Override
            public void onCanceled(WatchCancelResponse response) {
                WatchHandle currentWatchHandle = getWatchHandle();
                removeNodeWatchHandleIfSame(nodeId, currentWatchHandle);
            }

            @Override
            public void onError(Throwable cause) {
                WatchHandle currentWatchHandle = getWatchHandle();
                removeNodeWatchHandleIfSame(nodeId, currentWatchHandle);
            }
        });
    }

    /**
     * 构建浏览器侧 KV_CHANGED 推送载荷。
     */
    private KeyValueChangedPayload buildKeyValueChangedPayload(WatchNotification watchNotification) {
        KeyValueChangedPayload keyValueChangedPayload = new KeyValueChangedPayload();
        keyValueChangedPayload.setOperationType(KeyValueChangeOperationType.WATCH);
        keyValueChangedPayload.setRevision(watchNotification.getCurrentRevision());
        if (watchNotification != null && watchNotification.getEvents() != null) {
            keyValueChangedPayload.getWatchEventViewList().addAll(watchNotification.getEvents());
        }
        return keyValueChangedPayload;
    }

    /**
     * 判断当前回调句柄是否仍是该节点的“当前自动 watch 句柄”。
     *
     * <p>用于过滤延迟回调：若该节点自动 watch 已被替换，旧句柄回调不能继续推送。</p>
     */
    private boolean isCurrentNodeWatchHandle(String nodeId, WatchHandle watchHandle) {
        if (isBlank(nodeId) || watchHandle == null) {
            return false;
        }
        WatchHandle currentWatchHandle = nodeAutoWatchHandleMap.get(nodeId);
        return currentWatchHandle == watchHandle;
    }

    /**
     * 仅当映射中的 handle 与传入 handle 一致时移除，避免误删并发新建的自动 watch。
     */
    private void removeNodeWatchHandleIfSame(String nodeId, WatchHandle watchHandle) {
        if (isBlank(nodeId) || watchHandle == null) {
            return;
        }
        if (nodeAutoWatchHandleMap.remove(nodeId, watchHandle)) {
            closeWatchHandle(watchHandle);
        }
    }

    /**
     * 关闭自动 watch 句柄。
     */
    private void closeWatchHandle(WatchHandle watchHandle) {
        if (watchHandle == null) {
            return;
        }
        // close 统一执行“先尝试 cancel，再本地收敛”。
        watchHandle.close();
    }

    /**
     * 判空工具方法。
     */
    private boolean isBlank(String value) {
        return value == null || value.trim().length() == 0;
    }

}
