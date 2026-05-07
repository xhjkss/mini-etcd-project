package com.xhj.etcd.kernel.server.command;

/**
 * EtcdCommandType
 *
 * @author XJks
 * @description Etcd 命令类型枚举。
 *
 * <p>当前 Phase 支持 PUT、DELETE、GET、LIST_KEYS，用于临时 KV 服务与 Raft-RPC 联调闭环。</p>
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
     * 列出指定分组下的所有 key。
     */
    LIST_KEYS;
}
