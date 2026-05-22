package com.xhj.etcd.console.controller;

import com.xhj.etcd.console.model.response.common.BaseResponse;
import com.xhj.etcd.console.service.ClusterDiagnosticService;
import com.xhj.etcd.kernel.etcd.etcdrpc.KvStateHashRequest;
import com.xhj.etcd.kernel.etcd.etcdrpc.KvStateHashResponse;
import com.xhj.etcd.kernel.etcd.etcdrpc.NodeStatusResponse;
import com.xhj.etcd.kernel.etcd.etcdrpc.RangeRequest;
import com.xhj.etcd.kernel.etcd.etcdrpc.RangeResponse;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import org.springframework.beans.factory.annotation.Autowired;

import java.util.List;

/**
 * ClusterDiagnosticController
 *
 * @author XJks
 * @description 集群诊断接口。
 */
@RestController
@RequestMapping("/api/cluster")
public class ClusterDiagnosticController {

    // ==================== 依赖组件 ====================
    /**
     * 集群诊断服务。
     */
    @Autowired
    private ClusterDiagnosticService clusterDiagnosticService;

    /**
     * 查询全部已连接节点状态。
     */
    @GetMapping("/node-status/on-all-nodes")
    public BaseResponse<List<NodeStatusResponse>> listNodeStatusOnAllNodes() {
        return BaseResponse.success(clusterDiagnosticService.listNodeStatusOnAllNodes());
    }

    /**
     * 查询单节点状态。
     */
    @GetMapping("/node-status/on-node")
    public BaseResponse<NodeStatusResponse> getNodeStatusOnEndpoint(@RequestParam String host,
                                                                    @RequestParam int port) {
        return BaseResponse.success(clusterDiagnosticService.getNodeStatusOnEndpoint(host, port));
    }

    /**
     * 计算单节点 KV 状态哈希。
     */
    @PostMapping("/kv-state-hash/on-node")
    public BaseResponse<KvStateHashResponse> computeKvStateHashOnEndpoint(@RequestParam String host,
                                                                          @RequestParam int port,
                                                                          @RequestBody KvStateHashRequest kvStateHashRequest) {
        return BaseResponse.success(clusterDiagnosticService.computeKvStateHashOnEndpoint(host, port, kvStateHashRequest));
    }

    /**
     * 在所有已连接节点上执行同一 RANGE 请求。
     */
    @PostMapping("/range/on-all-nodes")
    public BaseResponse<List<RangeResponse>> rangeOnAllNodes(@RequestBody RangeRequest rangeRequest) {
        if (rangeRequest == null) {
            throw new IllegalArgumentException("rangeRequest must not be null");
        }
        return BaseResponse.success(clusterDiagnosticService.rangeOnAllNodes(rangeRequest));
    }
}
