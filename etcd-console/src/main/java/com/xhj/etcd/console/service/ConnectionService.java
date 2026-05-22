package com.xhj.etcd.console.service;

import com.xhj.etcd.console.model.request.connection.ConnectRequest;
import com.xhj.etcd.console.model.request.connection.DisconnectRequest;
import com.xhj.etcd.kernel.etcd.etcdrpc.NodeStatusRequest;
import com.xhj.etcd.kernel.etcd.etcdrpc.NodeStatusResponse;
import com.xhj.etcd.rpc.NodeEndpoint;
import com.xhj.etcd.sdk.client.EtcdClient;
import org.springframework.stereotype.Service;

import javax.annotation.PreDestroy;
import java.util.ArrayList;
import java.util.List;

/**
 * ConnectionService
 *
 * @author XJks
 * @description 控制台连接服务，仅负责连接生命周期与节点解析，不承载具体 etcd 业务调用。
 */
@Service
public class ConnectionService {

    // ==================== 运行时状态 ====================
    /**
     * 单用户控制台唯一 EtcdClient。
     */
    private volatile EtcdClient etcdClient;

    /**
     * 客户端替换锁。
     */
    private final Object clientLock = new Object();

    /**
     * 建立一个控制台连接（新增 endpoint）。
     *
     * @param connectRequest 连接请求
     * @return 新增后的节点 endpoint
     */
    public NodeEndpoint connect(ConnectRequest connectRequest) {
        validateConnectRequest(connectRequest);
        String host = connectRequest.getHost().trim();
        int port = connectRequest.getPort();

        // 1) 先探测目标节点，拿到真实 nodeId，避免前端传入错误 nodeId。
        NodeEndpoint targetNodeEndpoint = probeNodeEndpoint(host, port);
        synchronized (clientLock) {
            // 2) 再基于当前客户端 endpoint 快照做唯一性校验和列表合并。
            List<NodeEndpoint> endpointList = snapshotEndpointList();
            for (NodeEndpoint endpoint : endpointList) {
                if (isSameEndpoint(endpoint, host, port)) {
                    throw new IllegalArgumentException("endpoint already connected: " + host + ":" + port);
                }
                if (targetNodeEndpoint.getNodeId().equals(endpoint.getNodeId())) {
                    throw new IllegalArgumentException("nodeId already connected: " + targetNodeEndpoint.getNodeId());
                }
            }
            endpointList.add(targetNodeEndpoint);
            // 3) 最后替换为新客户端，旧客户端统一关闭。
            replaceClient(endpointList);
        }
        return copyNodeEndpoint(targetNodeEndpoint);
    }

    /**
     * 查询当前连接列表。
     *
     * @return 已连接节点 endpoint 列表
     */
    public List<NodeEndpoint> listConnections() {
        List<NodeEndpoint> endpointList = snapshotEndpointList();
        endpointList.sort((leftNodeEndpoint, rightNodeEndpoint) -> {
            int hostCompare = normalizeHost(leftNodeEndpoint.getHost()).compareTo(normalizeHost(rightNodeEndpoint.getHost()));
            if (hostCompare != 0) {
                return hostCompare;
            }
            return Integer.compare(leftNodeEndpoint.getPort(), rightNodeEndpoint.getPort());
        });
        List<NodeEndpoint> responseEndpointList = new ArrayList<>();
        for (NodeEndpoint endpoint : endpointList) {
            responseEndpointList.add(copyNodeEndpoint(endpoint));
        }
        return responseEndpointList;
    }

    /**
     * 获取当前集群客户端。
     *
     * @return 当前集群客户端
     */
    public EtcdClient getEtcdClient() {
        EtcdClient client = this.etcdClient;
        if (client == null) {
            throw new IllegalStateException("etcd client is not initialized");
        }
        return client;
    }

    /**
     * 断开指定 endpoint 连接。
     *
     * @param disconnectRequest 断连请求（host + port）
     */
    public void disconnect(DisconnectRequest disconnectRequest) {
        validateDisconnectRequest(disconnectRequest);
        disconnectByEndpoint(disconnectRequest.getHost(), disconnectRequest.getPort());
    }

    /**
     * 按 host + port 断开指定节点连接。
     *
     * @param host 主机地址
     * @param port 端口
     */
    public void disconnectByEndpoint(String host, int port) {
        if (isBlank(host) || port <= 0 || port > 65535) {
            return;
        }
        synchronized (clientLock) {
            // 1) 从当前 endpoint 快照中过滤掉目标 endpoint。
            List<NodeEndpoint> endpointList = snapshotEndpointList();
            List<NodeEndpoint> newEndpointList = new ArrayList<>();
            for (NodeEndpoint endpoint : endpointList) {
                if (!isSameEndpoint(endpoint, host, port)) {
                    newEndpointList.add(endpoint);
                }
            }
            if (newEndpointList.size() == endpointList.size()) {
                return;
            }
            // 2) 替换客户端并关闭旧实例。
            replaceClient(newEndpointList);
        }
    }

    /**
     * 按 host + port 获取已连接 endpoint（不存在则抛错）。
     *
     * @param host 主机地址
     * @param port 端口
     * @return 已连接 endpoint
     */
    public NodeEndpoint requireConnectedEndpoint(String host, int port) {
        if (isBlank(host)) {
            throw new IllegalArgumentException("host must not be empty");
        }
        if (port <= 0 || port > 65535) {
            throw new IllegalArgumentException("port must be between 1 and 65535");
        }
        String normalizedHost = normalizeHost(host);
        for (NodeEndpoint nodeEndpoint : snapshotEndpointList()) {
            if (nodeEndpoint != null
                    && nodeEndpoint.getPort() == port
                    && normalizeHost(nodeEndpoint.getHost()).equals(normalizedHost)) {
                return nodeEndpoint;
            }
        }
        throw new IllegalArgumentException("endpoint not found: " + host + ":" + port);
    }

    /**
     * 组件销毁时关闭当前客户端。
     */
    @PreDestroy
    public void disconnectAll() {
        synchronized (clientLock) {
            EtcdClient oldClient = this.etcdClient;
            this.etcdClient = null;
            if (oldClient != null) {
                oldClient.close();
            }
        }
    }

    /**
     * 校验连接请求参数。
     */
    private void validateConnectRequest(ConnectRequest connectRequest) {
        if (connectRequest == null) {
            throw new IllegalArgumentException("connect request must not be null");
        }
        if (isBlank(connectRequest.getHost())) {
            throw new IllegalArgumentException("host must not be empty");
        }
        if (connectRequest.getPort() <= 0 || connectRequest.getPort() > 65535) {
            throw new IllegalArgumentException("port must be between 1 and 65535");
        }
    }

    /**
     * 校验断连请求参数。
     */
    private void validateDisconnectRequest(DisconnectRequest disconnectRequest) {
        if (disconnectRequest == null) {
            throw new IllegalArgumentException("disconnect request must not be null");
        }
        if (isBlank(disconnectRequest.getHost())) {
            throw new IllegalArgumentException("host must not be empty");
        }
        if (disconnectRequest.getPort() <= 0 || disconnectRequest.getPort() > 65535) {
            throw new IllegalArgumentException("port must be between 1 and 65535");
        }
    }

    /**
     * 探测目标 endpoint 并获取真实 nodeId。
     */
    private NodeEndpoint probeNodeEndpoint(String host, int port) {
        // 探测阶段仅需要一个可序列化的 nodeId 占位值，这里使用 host:port 占位。
        NodeEndpoint probeEndpoint = new NodeEndpoint(host + ":" + port, host, port);
        List<NodeEndpoint> endpointList = new ArrayList<>();
        endpointList.add(probeEndpoint);
        try (EtcdClient probeClient = new EtcdClient(endpointList)) {
            // 使用 NodeStatus 拿真实 nodeId，避免把“探测 nodeId”写入正式 endpoint 列表。
            NodeStatusResponse nodeStatusResponse = probeClient.getNodeStatusOnEndpoint(probeEndpoint, new NodeStatusRequest());
            if (nodeStatusResponse == null || isBlank(nodeStatusResponse.getNodeId())) {
                throw new IllegalStateException("node status response is empty");
            }
            return new NodeEndpoint(nodeStatusResponse.getNodeId(), host, port);
        } catch (RuntimeException exception) {
            throw new IllegalArgumentException("connect failed for endpoint " + host + ":" + port + ": " + exception.getMessage(), exception);
        }
    }

    /**
     * 获取 endpoint 列表快照（可变拷贝）。
     */
    private List<NodeEndpoint> snapshotEndpointList() {
        EtcdClient client = this.etcdClient;
        if (client == null) {
            return new ArrayList<>();
        }
        return new ArrayList<>(client.getEndpointsSnapshot());
    }

    /**
     * 替换当前 EtcdClient。
     *
     * <p>
     * TODO:
     *  不支持 EtcdClient 节点列表动态变更，统一采用“重建 EtcdClient”策略。
     *  原因：
     *  1) EtcdClient 内部存在 currentEndpoint/maxLeaderRetryTimes/watch 路由状态；
     *  2) 动态修改列表容易在并发请求与 watch 回调期间引入竞态与半更新状态；
     *  3) 单用户控制台优先保证语义清晰与功能正确，重建逻辑更可控。
     * </p>
     *
     * <p>
     * TODO:
     *  该精简设计的副作用：
     *  1) endpoint 配置变更时，旧 client 上的活动 watch 会中断；
     *  2) 切换窗口内少量并发请求可能失败，需要前端重试。
     *  当前阶段不做 watch 会话迁移，保持实现精简。
     * </p>
     */
    private void replaceClient(List<NodeEndpoint> endpointList) {
        EtcdClient oldClient = this.etcdClient;
        if (endpointList == null || endpointList.isEmpty()) {
            this.etcdClient = null;
        } else {
            this.etcdClient = new EtcdClient(endpointList);
        }
        if (oldClient != null) {
            oldClient.close();
        }
    }

    /**
     * 复制节点 endpoint，避免直接暴露内部可变对象。
     */
    private NodeEndpoint copyNodeEndpoint(NodeEndpoint nodeEndpoint) {
        return new NodeEndpoint(nodeEndpoint.getNodeId(), nodeEndpoint.getHost(), nodeEndpoint.getPort());
    }

    /**
     * host 归一化（判空 + trim + lower）。
     */
    private String normalizeHost(String host) {
        return host == null ? "" : host.trim().toLowerCase();
    }

    /**
     * 判断 endpoint 是否与 host + port 相同。
     */
    private boolean isSameEndpoint(NodeEndpoint nodeEndpoint, String host, int port) {
        if (nodeEndpoint == null) {
            return false;
        }
        return nodeEndpoint.getPort() == port
                && normalizeHost(nodeEndpoint.getHost()).equals(normalizeHost(host));
    }

    /**
     * 判空工具方法。
     */
    private boolean isBlank(String value) {
        return value == null || value.trim().length() == 0;
    }
}
