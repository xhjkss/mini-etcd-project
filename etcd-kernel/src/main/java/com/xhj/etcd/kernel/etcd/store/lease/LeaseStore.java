package com.xhj.etcd.kernel.etcd.store.lease;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

/**
 * LeaseStore
 *
 * @author XJks
 * @description 当前阶段最小 Lease 状态机，负责 lease 的 grant / keepalive / revoke / ttl / list。
 *
 * <p>职责边界：</p>
 * <ul>
 *     <li>维护 lease 元数据和 key 绑定关系。</li>
 *     <li>不直接删除 KV，只向 EtcdNode 提供 revoke/expire 时需要删除的 key 列表。</li>
 *     <li>快照只保存 lease 自身状态，不保存 KV 数据。</li>
 * </ul>
 */
public class LeaseStore {

    /**
     * leaseId -> lease 记录。
     */
    private final NavigableMap<Long, LeaseRecord> leaseById = new TreeMap<>();

    /**
     * 下一次可分配的 leaseId 基线。
     */
    private long nextLeaseId;

    /**
     * 发放租约。
     *
     * @param requestedLeaseId 客户端指定的 leaseId，0 表示由服务端自动分配
     * @param ttlSeconds       TTL 秒数
     * @param nowMillis        当前时间戳，单位：毫秒
     * @return 新租约视图
     */
    public synchronized LeaseRecord grant(long requestedLeaseId, long ttlSeconds, long nowMillis) {
        validateTtl(ttlSeconds);

        long leaseId = requestedLeaseId > 0L ? requestedLeaseId : allocateNextLeaseId();
        if (leaseById.containsKey(leaseId)) {
            throw new IllegalArgumentException("lease already exists, leaseId=" + leaseId);
        }
        nextLeaseId = Math.max(nextLeaseId, leaseId);

        LeaseRecord leaseRecord = new LeaseRecord();
        leaseRecord.setLeaseId(leaseId);
        leaseRecord.setTtlSeconds(ttlSeconds);
        leaseRecord.setDeadlineMillis(nowMillis + ttlSeconds * 1000L);
        leaseById.put(leaseId, leaseRecord);
        return leaseRecord.copy();
    }

    /**
     * 预分配一个新的 leaseId。
     *
     * @return 新 leaseId
     */
    public synchronized long allocateNextLeaseId() {
        nextLeaseId += 1L;
        return nextLeaseId;
    }

    /**
     * 续租。
     *
     * @param leaseId   leaseId
     * @param nowMillis 当前时间戳，单位：毫秒
     * @return 续租后的租约视图
     */
    public synchronized LeaseRecord keepAlive(long leaseId, long nowMillis) {
        LeaseRecord leaseRecord = leaseById.get(leaseId);
        if (leaseRecord == null) {
            throw new IllegalArgumentException("lease not found, leaseId=" + leaseId);
        }
        leaseRecord.setDeadlineMillis(nowMillis + leaseRecord.getTtlSeconds() * 1000L);
        return leaseRecord.copy();
    }

    /**
     * 撤销租约。
     *
     * @param leaseId leaseId
     * @return 被撤销的租约；不存在时返回 null
     */
    public synchronized LeaseRecord revoke(long leaseId) {
        LeaseRecord leaseRecord = leaseById.remove(leaseId);
        return leaseRecord == null ? null : leaseRecord.copy();
    }

    /**
     * 查询租约剩余 TTL。
     *
     * @param leaseId   leaseId
     * @param nowMillis 当前时间戳，单位：毫秒
     * @return 租约视图；不存在时返回 null
     */
    public synchronized LeaseRecord ttl(long leaseId, long nowMillis) {
        LeaseRecord leaseRecord = leaseById.get(leaseId);
        if (leaseRecord == null) {
            return null;
        }
        return leaseRecord.copy();
    }

    /**
     * 列出所有租约。
     *
     * @param nowMillis 当前时间戳，单位：毫秒
     * @return 有序租约列表
     */
    public synchronized List<LeaseRecord> list(long nowMillis) {
        List<LeaseRecord> records = new ArrayList<>();
        for (LeaseRecord leaseRecord : leaseById.values()) {
            records.add(leaseRecord.copy());
        }
        return records;
    }

    /**
     * 绑定 key 到指定 lease。
     *
     * @param leaseId leaseId
     * @param key     key
     */
    public synchronized void attachKey(long leaseId, String key) {
        LeaseRecord leaseRecord = leaseById.get(leaseId);
        if (leaseRecord == null) {
            throw new IllegalArgumentException("lease not found, leaseId=" + leaseId);
        }
        if (key != null && !key.trim().isEmpty()) {
            leaseRecord.getKeys().add(key);
        }
    }

    /**
     * 从指定 lease 解绑 key。
     *
     * @param leaseId leaseId
     * @param key     key
     */
    public synchronized void detachKey(long leaseId, String key) {
        LeaseRecord leaseRecord = leaseById.get(leaseId);
        if (leaseRecord == null || key == null || key.trim().isEmpty()) {
            return;
        }
        leaseRecord.getKeys().remove(key);
    }

    /**
     * 收集已过期租约 ID。
     *
     * <p>调用方负责根据这些 ID 发起 revoke 链路；该方法只做扫描，不直接修改状态。</p>
     *
     * @param nowMillis 当前时间戳，单位：毫秒
     * @return 已过期租约 ID 列表，按 leaseId 升序排列
     */
    public synchronized List<Long> collectExpiredLeaseIds(long nowMillis) {
        List<Long> expiredLeaseIds = new ArrayList<>();
        for (Map.Entry<Long, LeaseRecord> entry : leaseById.entrySet()) {
            if (entry.getValue().isExpired(nowMillis)) {
                expiredLeaseIds.add(entry.getKey());
            }
        }
        return expiredLeaseIds;
    }

    /**
     * 导出 Lease 快照。
     *
     * @return Lease 状态快照
     */
    public synchronized LeaseStoreSnapshot createSnapshot() {
        LeaseStoreSnapshot snapshot = new LeaseStoreSnapshot();
        snapshot.setNextLeaseId(nextLeaseId);
        for (Map.Entry<Long, LeaseRecord> entry : leaseById.entrySet()) {
            snapshot.getLeaseById().put(entry.getKey(), entry.getValue().copy());
        }
        return snapshot;
    }

    /**
     * 从 Lease 快照恢复。
     *
     * @param snapshot Lease 快照
     */
    public synchronized void restoreSnapshot(LeaseStoreSnapshot snapshot) {
        leaseById.clear();
        nextLeaseId = 0L;
        if (snapshot == null) {
            return;
        }

        nextLeaseId = Math.max(0L, snapshot.getNextLeaseId());
        if (snapshot.getLeaseById() != null) {
            for (Map.Entry<Long, LeaseRecord> entry : snapshot.getLeaseById().entrySet()) {
                if (entry.getKey() == null || entry.getValue() == null) {
                    continue;
                }
                leaseById.put(entry.getKey(), entry.getValue().copy());
                nextLeaseId = Math.max(nextLeaseId, entry.getKey());
            }
        }
    }

    /**
     * 获取当前租约数量。
     *
     * @return lease 数量
     */
    public synchronized int size() {
        return leaseById.size();
    }

    /**
     * 获取当前下一次可分配的 leaseId 基线。
     *
     * @return next leaseId
     */
    public synchronized long nextLeaseId() {
        return nextLeaseId;
    }

    /**
     * 获取指定 lease 的 key 数量。
     *
     * @param leaseId leaseId
     * @return key 数量；不存在时返回 0
     */
    public synchronized int attachedKeyCount(long leaseId) {
        LeaseRecord leaseRecord = leaseById.get(leaseId);
        return leaseRecord == null ? 0 : leaseRecord.getKeys().size();
    }

    /**
     * 获取指定 lease 的当前 key 列表。
     *
     * @param leaseId leaseId
     * @return key 列表；不存在时返回空列表
     */
    public synchronized List<String> attachedKeys(long leaseId) {
        LeaseRecord leaseRecord = leaseById.get(leaseId);
        if (leaseRecord == null) {
            return Collections.emptyList();
        }
        return new ArrayList<>(leaseRecord.getKeys());
    }

    private void validateTtl(long ttlSeconds) {
        if (ttlSeconds <= 0L) {
            throw new IllegalArgumentException("lease ttl must be positive");
        }
    }
}
