package com.xhj.etcd.console.websocket;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.xhj.etcd.console.model.response.lease.LeaseSessionResponse;
import com.xhj.etcd.console.model.response.websocket.WebSocketMessageType;
import com.xhj.etcd.console.service.ConnectionService;
import com.xhj.etcd.console.service.LeaseService;
import com.xhj.etcd.kernel.etcd.etcdrpc.LeaseTtlRequest;
import com.xhj.etcd.kernel.etcd.etcdrpc.LeaseTtlResponse;
import com.xhj.etcd.sdk.client.EtcdClient;
import com.xhj.etcd.sdk.client.lease.LeaseHandle;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * WebSocketLeaseSessionScheduler
 *
 * @author XJks
 * @description Lease 会话状态推送调度器。
 */
@Component
public class WebSocketLeaseSessionScheduler {

    // ==================== 依赖组件 ====================
    /**
     * Lease 服务。
     */
    @Autowired
    private LeaseService leaseService;

    /**
     * 连接服务。
     */
    @Autowired
    private ConnectionService connectionService;

    /**
     * WebSocket 网关。
     */
    @Autowired
    private ConsoleWebSocketGateway consoleWebSocketGateway;

    /**
     * JSON 编码器。
     */
    @Autowired
    private ObjectMapper objectMapper;

    // ==================== 会话快照 ====================
    /**
     * leaseId -> 最近一次已推送会话快照签名。
     */
    private final Map<Long, String> lastPushedSignatureByLeaseId = new ConcurrentHashMap<>();

    /**
     * leaseId -> TTL 轮询连续失败次数（仅用于推送层容错，不驱动 leaseHandle 关闭）。
     */
    private final Map<Long, Integer> ttlPollFailureCountByLeaseId = new ConcurrentHashMap<>();

    /**
     * 周期轮询 Lease 会话并按增量推送。
     */
    @Scheduled(fixedDelayString = "${mini-etcd.console.lease-session-interval-millis:1000}")
    public void broadcastLeaseSessionChanges() {
        if (!consoleWebSocketGateway.hasActiveSession()) {
            lastPushedSignatureByLeaseId.clear();
            ttlPollFailureCountByLeaseId.clear();
            return;
        }

        List<Long> managedLeaseIdList = leaseService.listManagedLeaseIdList();
        EtcdClient etcdClient = null;
        try {
            etcdClient = connectionService.getEtcdClient();
        } catch (RuntimeException ignored) {
        }

        // ==================== 1) 刷新 TTL 视图 ====================
        refreshLeaseSessionViews(managedLeaseIdList, etcdClient);

        // ==================== 2) 增量推送 CREATED / UPDATED ====================
        Map<Long, String> currentSignatureByLeaseId = pushLeaseSessionChanges();

        // ==================== 3) 推送已移除会话 CLOSED ====================
        pushRemovedLeaseSessionClosedEvents(currentSignatureByLeaseId);
    }

    /**
     * 刷新当前 Lease 会话视图。
     *
     * <p>
     * TODO:
     *  轮询 TTL 失败只记入推送层失败计数，不直接关闭 leaseHandle。
     *  close 决策由 SDK 句柄 keepAlive 流程负责，避免“UI 轮询抖动误关业务会话”。
     * </p>
     */
    private void refreshLeaseSessionViews(List<Long> managedLeaseIdList, EtcdClient etcdClient) {
        for (Long leaseId : managedLeaseIdList) {
            if (leaseId == null || leaseId <= 0L) {
                continue;
            }
            LeaseHandle leaseHandle = leaseService.getLeaseHandleByLeaseId(leaseId);
            if (leaseHandle == null || leaseHandle.isClosed()) {
                leaseService.stopLeaseKeepAlive(leaseId);
                ttlPollFailureCountByLeaseId.remove(leaseId);
                continue;
            }
            if (etcdClient == null) {
                incrementTtlPollFailureCount(leaseId);
                continue;
            }
            try {
                LeaseTtlResponse leaseTtlResponse = etcdClient.leaseTtl(new LeaseTtlRequest(leaseId));
                leaseService.updateLeaseSessionByTtlResponse(leaseId, leaseTtlResponse);
                ttlPollFailureCountByLeaseId.put(leaseId, 0);
            } catch (Exception exception) {
                incrementTtlPollFailureCount(leaseId);
            }
        }
    }

    /**
     * 推送会话新增与变更事件。
     *
     * @return 当前 lease 快照签名映射
     */
    private Map<Long, String> pushLeaseSessionChanges() {
        Map<Long, String> currentSignatureByLeaseId = new HashMap<>();
        List<LeaseSessionResponse> currentLeaseSessionResponseList = leaseService.listLeaseSessions();
        for (LeaseSessionResponse leaseSessionResponse : currentLeaseSessionResponseList) {
            if (leaseSessionResponse == null || leaseSessionResponse.getLeaseId() <= 0L) {
                continue;
            }
            long leaseId = leaseSessionResponse.getLeaseId();
            String currentSignature = buildSessionSignature(leaseSessionResponse);
            currentSignatureByLeaseId.put(leaseId, currentSignature);

            String previousSignature = lastPushedSignatureByLeaseId.get(leaseId);
            if (previousSignature == null) {
                consoleWebSocketGateway.broadcastEvent(WebSocketMessageType.LEASE_SESSION_CREATED, null, leaseSessionResponse);
                lastPushedSignatureByLeaseId.put(leaseId, currentSignature);
                continue;
            }
            if (!currentSignature.equals(previousSignature)) {
                WebSocketMessageType messageType = leaseSessionResponse.isActive()
                        ? WebSocketMessageType.LEASE_SESSION_UPDATED
                        : WebSocketMessageType.LEASE_SESSION_CLOSED;
                consoleWebSocketGateway.broadcastEvent(messageType, null, leaseSessionResponse);
                lastPushedSignatureByLeaseId.put(leaseId, currentSignature);
            }
        }
        return currentSignatureByLeaseId;
    }

    /**
     * 推送已移除会话的 CLOSED 事件并清理本地快照。
     */
    private void pushRemovedLeaseSessionClosedEvents(Map<Long, String> currentSignatureByLeaseId) {
        List<Long> removedLeaseIdList = new ArrayList<>();
        for (Long leaseId : new ArrayList<>(lastPushedSignatureByLeaseId.keySet())) {
            if (!currentSignatureByLeaseId.containsKey(leaseId)) {
                removedLeaseIdList.add(leaseId);
                lastPushedSignatureByLeaseId.remove(leaseId);
                ttlPollFailureCountByLeaseId.remove(leaseId);
            }
        }
        for (Long removedLeaseId : removedLeaseIdList) {
            LeaseSessionResponse leaseSessionResponse = new LeaseSessionResponse();
            leaseSessionResponse.setLeaseId(removedLeaseId);
            leaseSessionResponse.setActive(false);
            consoleWebSocketGateway.broadcastEvent(WebSocketMessageType.LEASE_SESSION_CLOSED, null, leaseSessionResponse);
        }
    }

    /**
     * 增加 TTL 轮询失败计数。
     */
    private void incrementTtlPollFailureCount(Long leaseId) {
        int failureCount = ttlPollFailureCountByLeaseId.getOrDefault(leaseId, 0) + 1;
        ttlPollFailureCountByLeaseId.put(leaseId, failureCount);
    }

    /**
     * 构造会话签名。
     */
    private String buildSessionSignature(LeaseSessionResponse leaseSessionResponse) {
        try {
            return objectMapper.writeValueAsString(leaseSessionResponse);
        } catch (JsonProcessingException exception) {
            return String.valueOf(leaseSessionResponse.getLeaseId()) + "-"
                    + leaseSessionResponse.isActive() + "-"
                    + leaseSessionResponse.getTtlSeconds() + "-"
                    + leaseSessionResponse.getRemainingSeconds() + "-"
                    + String.valueOf(leaseSessionResponse.getKeys());
        }
    }
}
