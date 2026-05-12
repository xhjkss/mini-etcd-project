package com.xhj.etcd.kernel.etcd.command;

/**
 * EtcdCommandType
 *
 * @author XJks
 * @description Etcd 命令类型枚举。
 *
 * <p>当前 Phase 支持 PUT、DELETE、GET、RANGE、DELETE_RANGE，用于 MVCC KV 与 Raft-RPC 联调闭环。</p>
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
    DELETE_RANGE;
}
