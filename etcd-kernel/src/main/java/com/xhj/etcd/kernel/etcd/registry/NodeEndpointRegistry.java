package com.xhj.etcd.kernel.etcd.registry;

import com.xhj.etcd.rpc.NodeEndpoint;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * NodeEndpointRegistry
 *
 * @author XJks
 * @description 节点端点注册表，管理集群中所有节点的地址信息。
 */
public class NodeEndpointRegistry {

    /**
     * 节点 ID -> 节点端点 映射。
     */
    private final Map<String, NodeEndpoint> endpointMap = new ConcurrentHashMap<>();

    // ==================== 节点注册 ====================

    /**
     * 注册节点端点。
     *
     * @param endpoint 节点端点
     */
    public void register(NodeEndpoint endpoint) {
        if (endpoint == null) {
            throw new IllegalArgumentException("endpoint must not be null");
        }
        endpointMap.put(endpoint.getNodeId(), endpoint);
    }

    /**
     * 注册节点端点（简化版本，host:port 格式）。
     *
     * @param nodeId  节点 ID
     * @param address 地址，格式为 host:port
     */
    public void register(String nodeId, String address) {
        if (nodeId == null || address == null) {
            throw new IllegalArgumentException("nodeId and address must not be null");
        }
        // 解析 host:port
        String[] parts = address.split(":");
        if (parts.length != 2) {
            throw new IllegalArgumentException("address must be in host:port format");
        }
        NodeEndpoint endpoint = new NodeEndpoint(nodeId, parts[0], Integer.parseInt(parts[1]));
        endpointMap.put(nodeId, endpoint);
    }

    // ==================== 节点查找 ====================

    /**
     * 根据节点 ID 查找端点。
     *
     * @param nodeId 节点 ID
     * @return 节点端点，不存在返回 null
     */
    public NodeEndpoint resolve(String nodeId) {
        if (nodeId == null) {
            return null;
        }
        return endpointMap.get(nodeId);
    }

    /**
     * 根据节点 ID 获取地址字符串（host:port）。
     *
     * @param nodeId 节点 ID
     * @return 地址字符串，不存在返回 null
     */
    public String getAddress(String nodeId) {
        NodeEndpoint endpoint = resolve(nodeId);
        return endpoint != null ? endpoint.endpointKey() : null;
    }
}
