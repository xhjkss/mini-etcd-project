package com.xhj.etcd.kernel.etcd.etcdrpc;

/**
 * TxnOperationType
 *
 * @author XJks
 * @description Txn 分支支持的操作类型。
 */
public enum TxnOperationType {

    /**
     * Put 操作。
     */
    PUT,

    /**
     * Delete 单 key 操作。
     */
    DELETE,

    /**
     * Get 单 key 操作。
     */
    GET,

    /**
     * Range 区间读取操作。
     */
    RANGE,

    /**
     * DeleteRange 区间删除操作。
     */
    DELETE_RANGE
}
