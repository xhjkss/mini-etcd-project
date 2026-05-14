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
    TXN
}
