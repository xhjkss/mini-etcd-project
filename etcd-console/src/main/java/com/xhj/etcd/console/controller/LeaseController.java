package com.xhj.etcd.console.controller;

import com.xhj.etcd.console.model.response.common.BaseResponse;
import com.xhj.etcd.console.model.request.lease.LeaseSessionGrantStartRequest;
import com.xhj.etcd.console.model.request.lease.LeaseSessionStartRequest;
import com.xhj.etcd.console.model.response.lease.LeaseSessionResponse;
import com.xhj.etcd.console.service.LeaseService;
import com.xhj.etcd.kernel.etcd.etcdrpc.LeaseGrantRequest;
import com.xhj.etcd.kernel.etcd.etcdrpc.LeaseGrantResponse;
import com.xhj.etcd.kernel.etcd.etcdrpc.LeaseListRequest;
import com.xhj.etcd.kernel.etcd.etcdrpc.LeaseListResponse;
import com.xhj.etcd.kernel.etcd.etcdrpc.LeaseRevokeRequest;
import com.xhj.etcd.kernel.etcd.etcdrpc.LeaseRevokeResponse;
import com.xhj.etcd.kernel.etcd.etcdrpc.LeaseTtlRequest;
import com.xhj.etcd.kernel.etcd.etcdrpc.LeaseTtlResponse;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import org.springframework.beans.factory.annotation.Autowired;

import java.util.List;

/**
 * LeaseController
 *
 * @author XJks
 * @description Lease 管理接口。
 */
@RestController
@RequestMapping("/api/lease")
public class LeaseController {

    // ==================== 依赖组件 ====================
    /**
     * Lease 服务。
     */
    @Autowired
    private LeaseService leaseService;

    /**
     * 执行 lease grant。
     */
    @PostMapping("/grant")
    public BaseResponse<LeaseGrantResponse> leaseGrant(@RequestBody LeaseGrantRequest leaseGrantRequest) {
        return BaseResponse.success(leaseService.leaseGrant(leaseGrantRequest));
    }

    /**
     * 执行 lease revoke。
     */
    @PostMapping("/revoke")
    public BaseResponse<LeaseRevokeResponse> leaseRevoke(@RequestBody LeaseRevokeRequest leaseRevokeRequest) {
        return BaseResponse.success(leaseService.leaseRevoke(leaseRevokeRequest));
    }

    /**
     * 执行 lease ttl 查询。
     */
    @PostMapping("/ttl")
    public BaseResponse<LeaseTtlResponse> leaseTtl(@RequestBody LeaseTtlRequest leaseTtlRequest) {
        return BaseResponse.success(leaseService.leaseTtl(leaseTtlRequest));
    }

    /**
     * 执行 lease list 查询。
     */
    @PostMapping("/list")
    public BaseResponse<LeaseListResponse> leaseList(@RequestBody LeaseListRequest leaseListRequest) {
        return BaseResponse.success(leaseService.leaseList(leaseListRequest));
    }

    /**
     * 启动 KeepAlive 会话（接管已有 lease）。
     */
    @PostMapping("/session/start")
    public BaseResponse<LeaseSessionResponse> startLeaseKeepAlive(@RequestBody LeaseSessionStartRequest leaseSessionStartRequest) {
        return BaseResponse.success(leaseService.startLeaseKeepAlive(leaseSessionStartRequest));
    }

    /**
     * Grant 并启动 KeepAlive 会话。
     */
    @PostMapping("/session/grant-start")
    public BaseResponse<LeaseSessionResponse> grantAndStartLeaseKeepAlive(@RequestBody LeaseSessionGrantStartRequest leaseSessionGrantStartRequest) {
        return BaseResponse.success(leaseService.grantAndStartLeaseKeepAlive(leaseSessionGrantStartRequest));
    }

    /**
     * 停止 KeepAlive 会话。
     */
    @DeleteMapping("/session")
    public BaseResponse<Void> stopLeaseKeepAlive(@RequestParam long leaseId) {
        leaseService.stopLeaseKeepAlive(leaseId);
        return BaseResponse.success();
    }

    /**
     * 查询 KeepAlive 会话列表。
     */
    @GetMapping("/session")
    public BaseResponse<List<LeaseSessionResponse>> listLeaseSessions() {
        return BaseResponse.success(leaseService.listLeaseSessions());
    }
}
