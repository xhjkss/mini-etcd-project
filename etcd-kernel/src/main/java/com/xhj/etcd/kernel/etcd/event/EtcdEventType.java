package com.xhj.etcd.kernel.etcd.event;

/**
 * EtcdEventType
 *
 * @author XJks
 * @description EtcdNode 内部事件类型枚举，用于标识当前阶段 etcd-event-loop 可以处理的 MVCC KV 事件。
 *
 * <p>EtcdEvent 是 RPC 层进入 EtcdNode 主流程的统一入口。
 * etcd-event-loop 会根据事件类型和请求参数决定：直接本地处理，还是转换为 EtcdCommand 提交 Raft。</p>
 */
public enum EtcdEventType {

    /**
     * Put 事件。
     *
     * <p>写事件，必须转换为 EtcdCommand 并进入 Raft 顺序流。</p>
     */
    PUT,

    /**
     * Delete 事件。
     *
     * <p>写事件，必须转换为 EtcdCommand 并进入 Raft 顺序流。</p>
     */
    DELETE,

    /**
     * Get 事件。
     *
     * <p>读事件。linearizableRead=true 时进入 Raft；linearizableRead=false 时直接读取本地状态机。</p>
     */
    GET,

    /**
     * Range 事件。
     *
     * <p>读事件。linearizableRead=true 时进入 Raft；linearizableRead=false 时直接读取本地状态机。</p>
     */
    RANGE,

    /**
     * DeleteRange 事件。
     *
     * <p>写事件，必须转换为 EtcdCommand 并进入 Raft 顺序流。</p>
     */
    DELETE_RANGE,

    /**
     * Txn 事件。
     *
     * <p>Txn compare 与分支执行必须在 apply 阶段原子执行，因此该事件始终进入 Raft 顺序流。</p>
     */
    TXN,

    /**
     * Compact 事件。
     *
     * <p>历史压缩边界是全局状态机语义，必须通过 Raft apply 串行推进。</p>
     */
    COMPACT,

    /**
     * LeaseGrant 事件。
     *
     * <p>Lease 发放属于写操作，必须通过 Raft apply 串行推进。</p>
     */
    LEASE_GRANT,

    /**
     * LeaseKeepAlive 事件。
     *
     * <p>Lease 续租属于写操作，必须通过 Raft apply 串行推进。</p>
     */
    LEASE_KEEP_ALIVE,

    /**
     * LeaseRevoke 事件。
     *
     * <p>Lease 撤销会触发 key 删除，必须通过 Raft apply 串行推进。</p>
     */
    LEASE_REVOKE,

    /**
     * LeaseTtl 事件。
     *
     * <p>TTL 查询属于只读事件，不进入 Raft；由 etcd-event-loop 统一调度本地读取，保持入口一致。</p>
     */
    LEASE_TTL,

    /**
     * LeaseList 事件。
     *
     * <p>List 查询属于只读事件，不进入 Raft；由 etcd-event-loop 统一调度本地读取，保持入口一致。</p>
     */
    LEASE_LIST
}
