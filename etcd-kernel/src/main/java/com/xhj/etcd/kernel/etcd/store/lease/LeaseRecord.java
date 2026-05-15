package com.xhj.etcd.kernel.etcd.store.lease;

import lombok.Data;

import java.io.Serializable;
import java.util.NavigableSet;
import java.util.TreeSet;

/**
 * LeaseRecord
 *
 * @author XJks
 * @description Lease 状态机中的单个租约记录。
 *
 * <p>该对象既用于运行态，也用于快照持久化；leaseId、ttlSeconds、deadlineMillis 和 keys
 * 一起描述一个租约及其当前绑定的 key 集合。</p>
 */
@Data
public class LeaseRecord implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * 租约 ID。
     */
    private long leaseId;

    /**
     * 租约 TTL，单位：秒。
     */
    private long ttlSeconds;

    /**
     * 当前租约到期时间戳，单位：毫秒。
     */
    private long deadlineMillis;

    /**
     * 当前租约绑定的 key 集合。
     *
     * <p>使用有序集合保证 revoke / snapshot / test 输出稳定。</p>
     */
    private NavigableSet<String> keys = new TreeSet<>();

    /**
     * 复制当前记录。
     *
     * @return 深拷贝后的 LeaseRecord
     */
    public LeaseRecord copy() {
        LeaseRecord copy = new LeaseRecord();
        copy.setLeaseId(leaseId);
        copy.setTtlSeconds(ttlSeconds);
        copy.setDeadlineMillis(deadlineMillis);
        copy.setKeys(new TreeSet<>(keys));
        return copy;
    }

    /**
     * 计算当前剩余 TTL 秒数。
     *
     * @param nowMillis 当前时间戳，单位：毫秒
     * @return 剩余 TTL 秒数，最小为 0
     */
    public long remainingSeconds(long nowMillis) {
        long remainingMillis = deadlineMillis - nowMillis;
        if (remainingMillis <= 0L) {
            return 0L;
        }
        long remainingSeconds = remainingMillis / 1000L;
        if (remainingMillis % 1000L > 0L) {
            remainingSeconds += 1L;
        }
        return remainingSeconds;
    }

    /**
     * 判断当前租约是否已经过期。
     *
     * @param nowMillis 当前时间戳，单位：毫秒
     * @return true 表示已过期
     */
    public boolean isExpired(long nowMillis) {
        return deadlineMillis <= nowMillis;
    }
}
