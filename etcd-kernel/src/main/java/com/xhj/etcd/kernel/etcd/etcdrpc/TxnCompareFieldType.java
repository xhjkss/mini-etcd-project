package com.xhj.etcd.kernel.etcd.etcdrpc;

/**
 * TxnCompareFieldType
 *
 * @author XJks
 * @description Txn compare 目标字段类型。
 */
public enum TxnCompareFieldType {

    /**
     * 比较 key 的 value。
     */
    VALUE,

    /**
     * 比较 key 的 version。
     */
    VERSION,

    /**
     * 比较 key 的 createRevision。
     */
    CREATE_REVISION,

    /**
     * 比较 key 的 modRevision。
     */
    MOD_REVISION
}
