package com.xhj.etcd.console.controller;

import com.xhj.etcd.console.model.response.common.BaseResponse;
import com.xhj.etcd.console.model.request.connection.ConnectRequest;
import com.xhj.etcd.console.model.request.connection.DisconnectRequest;
import com.xhj.etcd.console.service.ConnectionService;
import com.xhj.etcd.console.service.LeaseService;
import com.xhj.etcd.console.service.WatchService;
import com.xhj.etcd.console.websocket.WebSocketNodeKvWatchScheduler;
import com.xhj.etcd.rpc.NodeEndpoint;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import org.springframework.beans.factory.annotation.Autowired;

import java.util.List;

/**
 * ConnectionController
 *
 * @author XJks
 * @description 连接管理接口，负责 connect/list/disconnect。
 */
@RestController
@RequestMapping("/api/connections")
public class ConnectionController {

    // ==================== 依赖组件 ====================
    /**
     * 连接服务。
     */
    @Autowired
    private ConnectionService connectionService;

    /**
     * 浏览器 KV 自动 watch 支持组件。
     */
    @Autowired
    private WebSocketNodeKvWatchScheduler webSocketNodeKvWatchScheduler;

    /**
     * Lease 服务。
     */
    @Autowired
    private LeaseService leaseService;

    /**
     * Watch 会话服务。
     */
    @Autowired
    private WatchService watchService;

    /**
     * 查询当前连接列表。
     */
    @GetMapping
    public BaseResponse<List<NodeEndpoint>> listConnections() {
        return BaseResponse.success(connectionService.listConnections());
    }

    /**
     * 建立连接并启动浏览器侧自动 watch。
     */
    @PostMapping
    public BaseResponse<NodeEndpoint> connect(@RequestBody ConnectRequest connectRequest) {
        // 1) 先建立连接并返回真实 nodeId 的 endpoint（由后端探测校准）。
        NodeEndpoint nodeEndpoint = connectionService.connect(connectRequest);
        watchService.cancelAllWatchSessions();
        // 连接集合变更会重建 EtcdClient，旧 LeaseHandle 会被关闭，这里同步清空 console 会话视图。
        leaseService.closeAllLeaseSessions();
        // 2) 再为该节点启动浏览器自动 watch（用于 KV_CHANGED 实时刷新）。
        webSocketNodeKvWatchScheduler.ensureNodeWatchSessionStarted(nodeEndpoint.getNodeId());
        return BaseResponse.success(nodeEndpoint);
    }

    /**
     * 断开连接并停止浏览器侧自动 watch。
     */
    @DeleteMapping
    public BaseResponse<Void> disconnect(@RequestBody DisconnectRequest disconnectRequest) {
        if (disconnectRequest == null) {
            throw new IllegalArgumentException("disconnectRequest must not be null");
        }
        NodeEndpoint nodeEndpoint = connectionService.requireConnectedEndpoint(disconnectRequest.getHost(), disconnectRequest.getPort());
        // 先停自动 watch，再断连接，避免后续调度还使用已移除连接。
        webSocketNodeKvWatchScheduler.stopNodeWatchSession(nodeEndpoint.getNodeId());
        connectionService.disconnect(disconnectRequest);
        watchService.cancelAllWatchSessions();
        // 连接集合变更会重建 EtcdClient，旧 LeaseHandle 会被关闭，这里同步清空 console 会话视图。
        leaseService.closeAllLeaseSessions();
        return BaseResponse.success();
    }
}
