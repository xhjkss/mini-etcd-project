package com.xhj.etcd.kernel.etcd.etcdrpc;

import com.xhj.etcd.kernel.etcd.store.lease.LeaseRecord;
import lombok.Data;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * LeaseView
 *
 * @author XJks
 * @description Lease 响应中的单个租约视图。
 */
@Data
public class LeaseView implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * 租约 ID。
     */
    private long leaseId;

    /**
     * TTL 秒数。
     */
    private long ttlSeconds;

    /**
     * 剩余 TTL 秒数。
     */
    private long remainingSeconds;

    /**
     * 当前租约绑定的 key。
     */
    private List<String> keys = new ArrayList<>();

    /**
     * 构造 Lease 视图。
     */
    public static LeaseView of(LeaseRecord leaseRecord, long nowMillis) {
        LeaseView view = new LeaseView();
        if (leaseRecord == null) {
            return view;
        }
        view.leaseId = leaseRecord.getLeaseId();
        view.ttlSeconds = leaseRecord.getTtlSeconds();
        view.remainingSeconds = leaseRecord.remainingSeconds(nowMillis);
        view.keys = new ArrayList<>(leaseRecord.getKeys());
        return view;
    }
}
