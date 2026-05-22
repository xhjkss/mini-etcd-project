package com.xhj.etcd.console.service;

import com.xhj.etcd.console.model.response.websocket.WatchNotificationPayload;
import com.xhj.etcd.console.model.response.websocket.WebSocketMessageType;
import com.xhj.etcd.console.model.response.watch.WatchSessionResponse;
import com.xhj.etcd.console.websocket.ConsoleWebSocketGateway;
import com.xhj.etcd.kernel.etcd.etcdrpc.WatchCancelResponse;
import com.xhj.etcd.kernel.etcd.etcdrpc.WatchNotification;
import com.xhj.etcd.kernel.etcd.etcdrpc.WatchSubscribeRequest;
import com.xhj.etcd.kernel.etcd.etcdrpc.WatchSubscribeResponse;
import com.xhj.etcd.rpc.NodeEndpoint;
import com.xhj.etcd.sdk.client.EtcdClient;
import com.xhj.etcd.sdk.client.watch.WatchHandle;
import com.xhj.etcd.sdk.client.watch.WatchListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.annotation.PreDestroy;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * WatchService
 *
 * @author XJks
 * @description Watch 控制台服务，负责 watch 会话生命周期与事件推送。
 */
@Service
public class WatchService {

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
     * 节点 watch 句柄映射：
     * nodeId -> (watchId -> watchHandle)。
     */
    private final ConcurrentMap<String, ConcurrentMap<Long, WatchHandle>> watchHandleMapByNodeId = new ConcurrentHashMap<>();

    /**
     * 控制台本地 watchId 序列（仅在请求未指定 watchId 时分配）。
     */
    private final AtomicLong watchIdSequence = new AtomicLong(System.currentTimeMillis() * 1000L);

    /**
     * 创建并启动 watch 会话。
     *
     * <p>流程：</p>
     * <ol>
     *     <li>校验入参与 host/port 对应 endpoint。</li>
     *     <li>补齐 watchId/maxEvents 默认值。</li>
     *     <li>创建 WatchListener 并发起 etcdClient.watch(...)。</li>
     *     <li>订阅成功后由 onSubscribed 回调完成 map 注册与 WATCH_CREATED 推送。</li>
     * </ol>
     *
     * @param host             节点主机地址
     * @param port             节点端口
     * @param subscribeRequest 订阅请求
     * @return watch 会话响应
     */
    public WatchSessionResponse startWatchOnEndpoint(String host, int port, WatchSubscribeRequest subscribeRequest) {
        // ==================== 1) 参数与目标节点准备 ====================
        // 这里仅做控制台输入校验与 endpoint 解析，真正的 watch 语义校验仍由 Etcd 节点侧处理。
        validateStartRequest(host, port, subscribeRequest);
        NodeEndpoint nodeEndpoint = connectionService.requireConnectedEndpoint(host, port);
        String nodeId = nodeEndpoint.getNodeId();

        // ==================== 2) 会话标识与默认参数准备 ====================
        if (subscribeRequest.getWatchId() <= 0L) {
            subscribeRequest.setWatchId(nextWatchId());
        }
        if (subscribeRequest.getMaxEvents() <= 0) {
            subscribeRequest.setMaxEvents(128);
        }

        // ==================== 3) 建立 watch 并绑定回调 ====================
        EtcdClient etcdClient = connectionService.getEtcdClient();
        WatchListener watchListener = new WatchListener() {
            @Override
            public void onSubscribed(WatchSubscribeResponse response) {
                /**
                 * TODO:
                 *  监听器与 watchHandle 是一对一绑定关系（见 sdk WatchListener 约束），
                 *  所以 onSubscribed 回调时可直接拿到当前订阅句柄并注册到 nodeId 分桶。
                 *  这里统一作为“会话创建完成”的时刻，推送 WATCH_CREATED。
                 */
                WatchHandle watchHandle = getWatchHandle();
                if (watchHandle == null) {
                    return;
                }
                registerWatchHandle(nodeId, watchHandle);
                consoleWebSocketGateway.broadcastEvent(
                        WebSocketMessageType.WATCH_CREATED,
                        nodeId,
                        buildWatchSessionResponse(watchHandle));
            }

            @Override
            public void onNotification(WatchNotification response) {
                if (response == null) {
                    return;
                }
                // 事件推送统一走监听器绑定句柄，避免通过外层临时变量拼装会话状态。
                WatchHandle watchHandle = getWatchHandle();
                if (watchHandle == null) {
                    return;
                }
                publishWatchNotification(nodeId, buildWatchSessionResponse(watchHandle), response);

                // 服务端已标记 canceled：本地句柄同步关闭并移除。
                if (response.isCanceled()) {
                    watchHandle.close();
                    removeWatchHandleIfSame(nodeId, watchHandle);
                }
            }

            @Override
            public void onCanceled(WatchCancelResponse response) {
                WatchHandle watchHandle = getWatchHandle();
                if (watchHandle != null) {
                    watchHandle.close();
                }
                // canceled 回调语义是“服务端确认取消”，这里向前端广播最终态并清理映射。
                consoleWebSocketGateway.broadcastEvent(
                        WebSocketMessageType.WATCH_CANCELED,
                        nodeId,
                        buildWatchSessionResponse(watchHandle));
                if (watchHandle != null) {
                    removeWatchHandleIfSame(nodeId, watchHandle);
                }
            }

            @Override
            public void onError(Throwable cause) {
                // watch 内部异常统一转换为 WATCH_ERROR，前端可据此提示用户重建订阅。
                consoleWebSocketGateway.broadcastEvent(
                        WebSocketMessageType.WATCH_ERROR,
                        nodeId,
                        cause == null ? "unknown" : cause.getMessage());
                WatchHandle watchHandle = getWatchHandle();
                if (watchHandle != null) {
                    removeWatchHandleIfSame(nodeId, watchHandle);
                }
            }
        };
        /**
         * TODO:
         *  etcdClient.watch(...) 在 SDK 内部会把当前 watchHandle 绑定到 watchListener，
         *  因此本方法返回后即可得到可用句柄；实际 map 注册由 onSubscribed 回调完成。
         */
        WatchHandle watchHandle = etcdClient.watch(subscribeRequest, nodeEndpoint, watchListener);
        return buildWatchSessionResponse(watchHandle);
    }

    /**
     * 查询当前 watch 会话列表。
     *
     * @return watch 会话列表
     */
    public List<WatchSessionResponse> listWatchSessions() {
        // list 直接基于 nodeId -> watchId -> handle 快照构建，不维护独立“响应状态表”，避免双写不一致。
        List<WatchSessionResponse> watchSessionResponseList = new ArrayList<>();
        for (ConcurrentMap<Long, WatchHandle> nodeWatchHandleMap : watchHandleMapByNodeId.values()) {
            if (nodeWatchHandleMap == null || nodeWatchHandleMap.isEmpty()) {
                continue;
            }
            for (WatchHandle watchHandle : nodeWatchHandleMap.values()) {
                if (watchHandle != null) {
                    watchSessionResponseList.add(buildWatchSessionResponse(watchHandle));
                }
            }
        }
        return watchSessionResponseList;
    }

    /**
     * 取消指定 watch 会话。
     *
     * @param watchId watchId
     */
    public void cancelWatchByWatchId(long watchId) {
        /**
         * TODO:
         *  先全局移除再执行 cancel/close，避免并发重复取消造成重复广播。
         *  removeWatchHandleByWatchId 会遍历 node 分桶定位 watchId 并移除，保证后续回调收敛。
         */
        WatchHandle watchHandle = removeWatchHandleByWatchId(watchId);
        if (watchHandle == null) {
            return;
        }

        // cancel + close：cancel 负责协议层取消，close 负责本地句柄收敛。
        watchHandle.cancel();
        watchHandle.close();
        String nodeId = watchHandle.getEndpoint() == null ? null : watchHandle.getEndpoint().getNodeId();
        consoleWebSocketGateway.broadcastEvent(
                WebSocketMessageType.WATCH_CANCELED,
                nodeId,
                buildWatchSessionResponse(watchHandle));
    }

    /**
     * 组件销毁时清理所有 watch 会话。
     */
    @PreDestroy
    public void cancelAllWatchSessions() {
        // 基于 key 快照遍历，避免遍历期间 map 并发修改导致漏取消或 ConcurrentModification。
        for (ConcurrentMap<Long, WatchHandle> nodeWatchHandleMap : new ArrayList<>(watchHandleMapByNodeId.values())) {
            if (nodeWatchHandleMap == null || nodeWatchHandleMap.isEmpty()) {
                continue;
            }
            for (Long watchId : new ArrayList<>(nodeWatchHandleMap.keySet())) {
                cancelWatchByWatchId(watchId);
            }
        }
    }

    /**
     * 校验订阅请求参数。
     */
    private void validateStartRequest(String host, int port, WatchSubscribeRequest subscribeRequest) {
        if (host == null || host.trim().length() == 0) {
            throw new IllegalArgumentException("host must not be empty");
        }
        if (port <= 0 || port > 65535) {
            throw new IllegalArgumentException("port must be between 1 and 65535");
        }
        if (subscribeRequest == null) {
            throw new IllegalArgumentException("watch subscribe request must not be null");
        }
        if (subscribeRequest.getStartKey() == null || subscribeRequest.getStartKey().trim().length() == 0) {
            throw new IllegalArgumentException("watch startKey must not be empty");
        }
    }

    /**
     * 构造 watch 事件推送载荷。
     *
     * <p>WebSocket 统一消息体：会话状态 + 通知事件，前端可直接按 watchId 分流展示。</p>
     */
    private WatchNotificationPayload buildWatchNotificationPayload(WatchSessionResponse watchSessionResponse,
                                                                   WatchNotification watchNotification) {
        WatchNotificationPayload watchNotificationPayload = new WatchNotificationPayload();
        watchNotificationPayload.setWatchSessionResponse(watchSessionResponse);
        watchNotificationPayload.setWatchNotification(watchNotification);
        return watchNotificationPayload;
    }

    /**
     * 构造 watch 会话响应。
     *
     * <p>这里统一从 WatchHandle 读取状态，保证 list/start/cancel 推送口径一致。</p>
     */
    private WatchSessionResponse buildWatchSessionResponse(WatchHandle watchHandle) {
        if (watchHandle == null) {
            return null;
        }
        WatchSessionResponse watchSessionResponse = new WatchSessionResponse();
        watchSessionResponse.setNodeId(watchHandle.getEndpoint() == null ? null : watchHandle.getEndpoint().getNodeId());
        watchSessionResponse.setKey(watchHandle.getStartKey());
        watchSessionResponse.setPrefix(watchHandle.isPrefixMatch());
        watchSessionResponse.setWatchId(watchHandle.getWatchId());
        watchSessionResponse.setActive(!watchHandle.isClosed());
        return watchSessionResponse;
    }

    /**
     * 推送 watch 事件。
     *
     * <p>nodeId 作为路由标签；payload 中携带 watchId，前端可在同一节点下区分多条 watch 流。</p>
     */
    private void publishWatchNotification(String nodeId,
                                          WatchSessionResponse watchSessionResponse,
                                          WatchNotification watchNotification) {
        if (watchSessionResponse == null) {
            return;
        }
        consoleWebSocketGateway.broadcastEvent(
                WebSocketMessageType.WATCH_EVENT,
                nodeId,
                buildWatchNotificationPayload(watchSessionResponse, watchNotification));
    }

    /**
     * 分配下一个 watchId。
     */
    private long nextWatchId() {
        long nextWatchId = watchIdSequence.incrementAndGet();
        return nextWatchId <= 0L ? Math.abs(nextWatchId) + 1L : nextWatchId;
    }

    /**
     * 注册指定节点下的 watch 句柄。
     *
     * <p>映射结构：nodeId -> (watchId -> watchHandle)。同一 nodeId 下允许并发存在多条 watch。</p>
     */
    private void registerWatchHandle(String nodeId, WatchHandle watchHandle) {
        if (watchHandle == null || nodeId == null || nodeId.trim().length() == 0) {
            return;
        }
        ConcurrentMap<Long, WatchHandle> nodeWatchHandleMap = watchHandleMapByNodeId.computeIfAbsent(
                nodeId,
                ignoredNodeId -> new ConcurrentHashMap<>());
        nodeWatchHandleMap.put(watchHandle.getWatchId(), watchHandle);
    }

    /**
     * 仅当映射中的句柄与传入句柄一致时才移除，避免误删并发替换后的新句柄。
     *
     * <p>适用场景：回调线程延迟到达时，旧句柄清理不能影响同 watchId 的新句柄。</p>
     */
    private void removeWatchHandleIfSame(String nodeId, WatchHandle watchHandle) {
        if (nodeId == null || nodeId.trim().length() == 0 || watchHandle == null) {
            return;
        }
        ConcurrentMap<Long, WatchHandle> nodeWatchHandleMap = watchHandleMapByNodeId.get(nodeId);
        if (nodeWatchHandleMap == null) {
            return;
        }
        nodeWatchHandleMap.remove(watchHandle.getWatchId(), watchHandle);
        if (nodeWatchHandleMap.isEmpty()) {
            watchHandleMapByNodeId.remove(nodeId, nodeWatchHandleMap);
        }
    }

    /**
     * 按 watchId 全局查找并移除句柄。
     *
     * <p>用于 cancelWatchByWatchId(watchId) 入口：调用方只传 watchId，不传 nodeId，因此需要跨节点分桶定位。</p>
     */
    private WatchHandle removeWatchHandleByWatchId(long watchId) {
        for (String nodeId : new ArrayList<>(watchHandleMapByNodeId.keySet())) {
            ConcurrentMap<Long, WatchHandle> nodeWatchHandleMap = watchHandleMapByNodeId.get(nodeId);
            if (nodeWatchHandleMap == null || nodeWatchHandleMap.isEmpty()) {
                continue;
            }
            WatchHandle watchHandle = nodeWatchHandleMap.remove(watchId);
            if (watchHandle != null) {
                if (nodeWatchHandleMap.isEmpty()) {
                    watchHandleMapByNodeId.remove(nodeId, nodeWatchHandleMap);
                }
                return watchHandle;
            }
            if (nodeWatchHandleMap.isEmpty()) {
                watchHandleMapByNodeId.remove(nodeId, nodeWatchHandleMap);
            }
        }
        return null;
    }

}
