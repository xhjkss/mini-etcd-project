package com.xhj.etcd.console.service;

import com.xhj.etcd.kernel.etcd.etcdrpc.LeaseGrantRequest;
import com.xhj.etcd.kernel.etcd.etcdrpc.LeaseGrantResponse;
import com.xhj.etcd.kernel.etcd.etcdrpc.LeaseListRequest;
import com.xhj.etcd.kernel.etcd.etcdrpc.LeaseListResponse;
import com.xhj.etcd.kernel.etcd.etcdrpc.LeaseRevokeRequest;
import com.xhj.etcd.kernel.etcd.etcdrpc.LeaseRevokeResponse;
import com.xhj.etcd.kernel.etcd.etcdrpc.LeaseTtlRequest;
import com.xhj.etcd.kernel.etcd.etcdrpc.LeaseTtlResponse;
import com.xhj.etcd.sdk.client.EtcdClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * LeaseService
 *
 * @author XJks
 * @description Lease 控制台服务，仅透传 Lease RPC 调用。
 */
@Service
public class LeaseService {

    // ==================== 依赖组件 ====================
    /**
     * 连接服务。
     */
    @Autowired
    private ConnectionService connectionService;

    /**
     * 执行 lease grant 请求。
     */
    public LeaseGrantResponse leaseGrant(LeaseGrantRequest leaseGrantRequest) {
        if (leaseGrantRequest == null) {
            throw new IllegalArgumentException("leaseGrantRequest must not be null");
        }
        EtcdClient etcdClient = connectionService.getEtcdClient();
        return etcdClient.leaseGrant(leaseGrantRequest);
    }

    /**
     * 执行 lease revoke 请求。
     */
    public LeaseRevokeResponse leaseRevoke(LeaseRevokeRequest leaseRevokeRequest) {
        if (leaseRevokeRequest == null) {
            throw new IllegalArgumentException("leaseRevokeRequest must not be null");
        }
        EtcdClient etcdClient = connectionService.getEtcdClient();
        return etcdClient.leaseRevoke(leaseRevokeRequest);
    }

    /**
     * 执行 lease ttl 请求。
     */
    public LeaseTtlResponse leaseTtl(LeaseTtlRequest leaseTtlRequest) {
        if (leaseTtlRequest == null) {
            throw new IllegalArgumentException("leaseTtlRequest must not be null");
        }
        EtcdClient etcdClient = connectionService.getEtcdClient();
        return etcdClient.leaseTtl(leaseTtlRequest);
    }

    /**
     * 执行 lease list 请求。
     */
    public LeaseListResponse leaseList(LeaseListRequest leaseListRequest) {
        if (leaseListRequest == null) {
            throw new IllegalArgumentException("leaseListRequest must not be null");
        }
        EtcdClient etcdClient = connectionService.getEtcdClient();
        return etcdClient.leaseList(leaseListRequest);
    }
}
