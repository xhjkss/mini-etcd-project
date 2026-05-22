package com.xhj.etcd.console.service;

import com.xhj.etcd.kernel.etcd.etcdrpc.KvStateHashRequest;
import com.xhj.etcd.kernel.etcd.etcdrpc.KvStateHashResponse;
import com.xhj.etcd.kernel.etcd.etcdrpc.NodeStatusRequest;
import com.xhj.etcd.kernel.etcd.etcdrpc.NodeStatusResponse;
import com.xhj.etcd.kernel.etcd.etcdrpc.RangeRequest;
import com.xhj.etcd.kernel.etcd.etcdrpc.RangeResponse;
import com.xhj.etcd.rpc.NodeEndpoint;
import com.xhj.etcd.sdk.client.EtcdClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

/**
 * ClusterDiagnosticService
 *
 * @author XJks
 * @description 集群诊断服务，负责节点状态、KV 哈希和跨节点范围读取。
 */
@Service
public class ClusterDiagnosticService {

    // ==================== 依赖组件 ====================
    /**
     * 连接服务。
     */
    @Autowired
    private ConnectionService connectionService;

    /**
     * 批量查询连接状态。
     *
     * @return 节点状态响应列表
     */
    public List<NodeStatusResponse> listNodeStatusOnAllNodes() {
        // 1) 拿连接快照，避免遍历期间连接列表被并发修改导致结果不稳定。
        List<NodeStatusResponse> responseList = new ArrayList<>();
        List<NodeEndpoint> endpointList = connectionService.listConnections();
        EtcdClient etcdClient = connectionService.getEtcdClient();
        // 2) 逐节点执行 node status 查询，直接复用 etcdrpc 原生响应。
        for (NodeEndpoint endpoint : endpointList) {
            NodeStatusResponse nodeStatusResponse = etcdClient.getNodeStatusOnEndpoint(endpoint, new NodeStatusRequest());
            responseList.add(nodeStatusResponse);
        }
        return responseList;
    }

    /**
     * 查询单连接节点状态。
     *
     * @param host 节点主机地址
     * @param port 节点端口
     * @return 节点状态响应
     */
    public NodeStatusResponse getNodeStatusOnEndpoint(String host, int port) {
        // 单节点查询失败时直接抛出异常，交给统一异常处理器转换为 BaseResponse.fail。
        NodeEndpoint nodeEndpoint = connectionService.requireConnectedEndpoint(host, port);
        EtcdClient etcdClient = connectionService.getEtcdClient();
        return etcdClient.getNodeStatusOnEndpoint(nodeEndpoint, new NodeStatusRequest());
    }

    /**
     * 查询单连接 KV 状态哈希。
     *
     * @param host               节点主机地址
     * @param port               节点端口
     * @param kvStateHashRequest 哈希请求
     * @return 哈希响应
     */
    public KvStateHashResponse computeKvStateHashOnEndpoint(String host, int port, KvStateHashRequest kvStateHashRequest) {
        if (kvStateHashRequest == null) {
            throw new IllegalArgumentException("kvStateHashRequest must not be null");
        }
        // 诊断请求按“调用方指定节点”执行，不做 leader 路由。
        NodeEndpoint nodeEndpoint = connectionService.requireConnectedEndpoint(host, port);
        EtcdClient etcdClient = connectionService.getEtcdClient();
        return etcdClient.computeKvStateHashOnEndpoint(nodeEndpoint, kvStateHashRequest);
    }

    /**
     * 在所有已连接节点上执行范围读取。
     *
     * @param rangeRequest 范围请求
     * @return 各节点范围读取响应列表
     */
    public List<RangeResponse> rangeOnAllNodes(RangeRequest rangeRequest) {
        if (rangeRequest == null) {
            throw new IllegalArgumentException("rangeRequest must not be null");
        }
        // 统一使用同一份 RangeRequest 在所有已连接节点执行，方便做节点间对比诊断。
        List<RangeResponse> responseList = new ArrayList<>();
        EtcdClient etcdClient = connectionService.getEtcdClient();
        for (NodeEndpoint endpoint : connectionService.listConnections()) {
            RangeResponse rangeResponse = etcdClient.rangeOnEndpoint(endpoint, rangeRequest);
            responseList.add(rangeResponse);
        }
        return responseList;
    }

}
