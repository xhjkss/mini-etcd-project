package com.xhj.etcd.kernel.etcd.store;

import com.xhj.etcd.kernel.etcd.store.mvcc.KeyValueStoreSnapshot;
import com.xhj.etcd.kernel.etcd.store.lease.LeaseStoreSnapshot;
import lombok.Data;

import java.io.Serializable;

/**
 * EtcdStateMachineSnapshot
 *
 * @author XJks
 * @description 当前阶段 Etcd 状态机快照，统一承载 KV 与 Lease 两部分状态。
 *
 * <p>Raft 层只持久化和传输这份快照的字节数组，不关心其中具体字段。</p>
 */
@Data
public class EtcdStateMachineSnapshot implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * MVCC KV 状态快照。
     */
    private KeyValueStoreSnapshot keyValueStoreSnapshot;

    /**
     * Lease 状态快照。
     */
    private LeaseStoreSnapshot leaseStoreSnapshot;
}
