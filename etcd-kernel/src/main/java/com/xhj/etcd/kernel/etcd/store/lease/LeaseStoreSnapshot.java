package com.xhj.etcd.kernel.etcd.store.lease;

import lombok.Data;

import java.io.Serializable;
import java.util.NavigableMap;
import java.util.TreeMap;

/**
 * LeaseStoreSnapshot
 *
 * @author XJks
 * @description Lease 状态机快照对象。
 */
@Data
public class LeaseStoreSnapshot implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * 下一次可分配的 leaseId 基线。
     */
    private long nextLeaseId;

    /**
     * leaseId -> 租约记录。
     */
    private NavigableMap<Long, LeaseRecord> leaseById = new TreeMap<>();
}
