package com.xhj.etcd.kernel.etcd.command;

/**
 * EtcdCommandType
 *
 * @author XJks
 * @description Etcd 命令类型枚举。
 *
 * <p>当前 Phase 支持 PUT、DELETE、GET、RANGE、DELETE_RANGE、TXN、COMPACT、LEASE_GRANT、LEASE_KEEP_ALIVE、LEASE_REVOKE，
 * 用于内核状态写入与 Raft-RPC 联调闭环。</p>
 */
public enum EtcdCommandType {

    /**
     * 写入 key-value。
     */
    PUT,

    /**
     * 删除指定 key。
     */
    DELETE,

    /**
     * 读取指定 key 的值。
     */
    GET,

    /**
     * 读取范围内的 KV。
     */
    RANGE,

    /**
     * 删除范围内的 KV。
     */
    DELETE_RANGE,

    /**
     * Txn 原子事务。
     */
    TXN,

    /**
     * 历史压缩。
     */
    COMPACT,

    /**
     * Lease 发放。
     */
    LEASE_GRANT,

    /**
     * Lease 续租。
     */
    LEASE_KEEP_ALIVE,

    /**
     * Lease 撤销。
     */
    LEASE_REVOKE;
}
