package com.xhj.etcd.console.service;

import com.xhj.etcd.console.model.request.lease.LeaseSessionGrantStartRequest;
import com.xhj.etcd.console.model.request.lease.LeaseSessionStartRequest;
import com.xhj.etcd.console.model.response.lease.LeaseSessionResponse;
import com.xhj.etcd.kernel.etcd.etcdrpc.LeaseGrantRequest;
import com.xhj.etcd.kernel.etcd.etcdrpc.LeaseGrantResponse;
import com.xhj.etcd.kernel.etcd.etcdrpc.LeaseKeepAliveRequest;
import com.xhj.etcd.kernel.etcd.etcdrpc.LeaseListRequest;
import com.xhj.etcd.kernel.etcd.etcdrpc.LeaseListResponse;
import com.xhj.etcd.kernel.etcd.etcdrpc.LeaseRevokeRequest;
import com.xhj.etcd.kernel.etcd.etcdrpc.LeaseRevokeResponse;
import com.xhj.etcd.kernel.etcd.etcdrpc.LeaseTtlRequest;
import com.xhj.etcd.kernel.etcd.etcdrpc.LeaseTtlResponse;
import com.xhj.etcd.kernel.etcd.etcdrpc.LeaseView;
import com.xhj.etcd.sdk.client.EtcdClient;
import com.xhj.etcd.sdk.client.lease.LeaseHandle;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.annotation.PreDestroy;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * LeaseService
 *
 * @author XJks
 * @description Lease 控制台服务，统一管理 RPC 调用与 LeaseHandle 会话。
 */
@Service
public class LeaseService {

    /**
     * 连续 TTL 查询失败阈值。
     */
    private static final int LEASE_TTL_QUERY_FAILURE_THRESHOLD = 3;

    // ==================== 依赖组件 ====================
    /**
     * 连接服务。
     */
    @Autowired
    private ConnectionService connectionService;

    // ==================== 运行时状态 ====================
    /**
     * leaseId -> leaseHandle。
     */
    private final ConcurrentMap<Long, LeaseHandle> leaseHandleByLeaseId = new ConcurrentHashMap<>();

    /**
     * leaseId -> TTL 连续失败次数。
     */
    private final ConcurrentMap<Long, Integer> leaseTtlFailureCountByLeaseId = new ConcurrentHashMap<>();

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

    /**
     * 启动 KeepAlive 会话（接管已有 lease）。
     */
    public LeaseSessionResponse startLeaseKeepAlive(LeaseSessionStartRequest leaseSessionStartRequest) {
        if (leaseSessionStartRequest == null) {
            throw new IllegalArgumentException("leaseSessionStartRequest must not be null");
        }
        long leaseId = leaseSessionStartRequest.getLeaseId();
        if (leaseId <= 0L) {
            throw new IllegalArgumentException("leaseId must be positive");
        }
        EtcdClient etcdClient = connectionService.getEtcdClient();
        LeaseHandle leaseHandle = etcdClient.startLeaseKeepAlive(new LeaseKeepAliveRequest(leaseId));
        return registerLeaseHandle(leaseHandle);
    }

    /**
     * Grant 并启动 KeepAlive 会话。
     */
    public LeaseSessionResponse grantAndStartLeaseKeepAlive(LeaseSessionGrantStartRequest leaseSessionGrantStartRequest) {
        if (leaseSessionGrantStartRequest == null) {
            throw new IllegalArgumentException("leaseSessionGrantStartRequest must not be null");
        }
        if (leaseSessionGrantStartRequest.getTtlSeconds() <= 0L) {
            throw new IllegalArgumentException("ttlSeconds must be positive");
        }
        LeaseGrantRequest leaseGrantRequest = new LeaseGrantRequest();
        leaseGrantRequest.setLeaseId(leaseSessionGrantStartRequest.getLeaseId());
        leaseGrantRequest.setTtlSeconds(leaseSessionGrantStartRequest.getTtlSeconds());
        EtcdClient etcdClient = connectionService.getEtcdClient();
        LeaseHandle leaseHandle = etcdClient.grantAndStartLeaseKeepAlive(leaseGrantRequest);
        return registerLeaseHandle(leaseHandle);
    }

    /**
     * 停止指定 lease 会话。
     */
    public void stopLeaseKeepAlive(long leaseId) {
        LeaseHandle leaseHandle = leaseHandleByLeaseId.remove(leaseId);
        leaseTtlFailureCountByLeaseId.remove(leaseId);
        if (leaseHandle != null) {
            leaseHandle.close();
        }
    }

    /**
     * 查询 lease 会话列表。
     */
    public List<LeaseSessionResponse> listLeaseSessions() {
        List<LeaseSessionResponse> leaseSessionResponseList = new ArrayList<>();
        for (LeaseHandle leaseHandle : leaseHandleByLeaseId.values()) {
            if (leaseHandle != null) {
                leaseSessionResponseList.add(buildLeaseSessionResponse(leaseHandle));
            }
        }
        leaseSessionResponseList.sort(Comparator.comparingLong(LeaseSessionResponse::getLeaseId));
        return leaseSessionResponseList;
    }

    /**
     * 获取当前受管 leaseId 列表。
     */
    public List<Long> listManagedLeaseIdList() {
        List<Long> leaseIdList = new ArrayList<>(leaseHandleByLeaseId.keySet());
        Collections.sort(leaseIdList);
        return leaseIdList;
    }

    /**
     * 按 leaseId 获取会话句柄。
     */
    public LeaseHandle getLeaseHandleByLeaseId(long leaseId) {
        return leaseHandleByLeaseId.get(leaseId);
    }

    /**
     * 使用 TTL 结果刷新会话状态。
     */
    public void updateLeaseSessionByTtlResponse(long leaseId, LeaseTtlResponse leaseTtlResponse) {
        LeaseHandle leaseHandle = leaseHandleByLeaseId.get(leaseId);
        if (leaseHandle == null) {
            return;
        }
        LeaseView leaseView = leaseTtlResponse == null ? null : leaseTtlResponse.getLease();
        if (leaseView == null || leaseView.getLeaseId() <= 0L) {
            markLeaseSessionError(leaseId);
            return;
        }
        leaseHandle.refreshLeaseView(leaseView);
        leaseTtlFailureCountByLeaseId.put(leaseId, 0);
    }

    /**
     * 标记会话错误。
     */
    public void markLeaseSessionError(long leaseId) {
        LeaseHandle leaseHandle = leaseHandleByLeaseId.get(leaseId);
        if (leaseHandle == null) {
            return;
        }
        int failureCount = leaseTtlFailureCountByLeaseId.getOrDefault(leaseId, 0) + 1;
        leaseTtlFailureCountByLeaseId.put(leaseId, failureCount);
        if (failureCount >= LEASE_TTL_QUERY_FAILURE_THRESHOLD) {
            stopLeaseKeepAlive(leaseId);
        }
    }

    /**
     * 清理全部 lease 会话。
     */
    @PreDestroy
    public void closeAllLeaseSessions() {
        for (Long leaseId : listManagedLeaseIdList()) {
            stopLeaseKeepAlive(leaseId);
        }
        leaseHandleByLeaseId.clear();
        leaseTtlFailureCountByLeaseId.clear();
    }

    /**
     * 注册或替换 lease 会话。
     */
    private LeaseSessionResponse registerLeaseHandle(LeaseHandle leaseHandle) {
        if (leaseHandle == null) {
            throw new IllegalArgumentException("leaseHandle must not be null");
        }
        long leaseId = leaseHandle.getLeaseId();
        if (leaseId <= 0L) {
            throw new IllegalArgumentException("leaseHandle.leaseId must be positive");
        }
        LeaseHandle oldLeaseHandle = leaseHandleByLeaseId.put(leaseId, leaseHandle);
        if (oldLeaseHandle != null) {
            oldLeaseHandle.close();
        }
        leaseTtlFailureCountByLeaseId.put(leaseId, 0);
        return buildLeaseSessionResponse(leaseHandle);
    }

    /**
     * 基于 LeaseHandle 构造会话响应。
     */
    private LeaseSessionResponse buildLeaseSessionResponse(LeaseHandle leaseHandle) {
        LeaseSessionResponse leaseSessionResponse = new LeaseSessionResponse();
        LeaseView leaseView = leaseHandle.getLeaseView();
        leaseSessionResponse.setLeaseId(leaseHandle.getLeaseId());
        leaseSessionResponse.setActive(!leaseHandle.isClosed());
        leaseSessionResponse.setTtlSeconds(leaseView.getTtlSeconds());
        leaseSessionResponse.setRemainingSeconds(leaseView.getRemainingSeconds());
        leaseSessionResponse.setKeys(new ArrayList<>(leaseView.getKeys()));
        return leaseSessionResponse;
    }
}
