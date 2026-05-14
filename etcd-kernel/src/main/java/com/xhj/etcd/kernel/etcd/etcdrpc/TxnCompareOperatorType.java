package com.xhj.etcd.kernel.etcd.etcdrpc;

/**
 * TxnCompareOperatorType
 *
 * @author XJks
 * @description Txn compare 运算符类型。
 */
public enum TxnCompareOperatorType {

    /**
     * 等于。
     */
    EQUAL,

    /**
     * 不等于。
     */
    NOT_EQUAL,

    /**
     * 大于。
     */
    GREATER,

    /**
     * 小于。
     */
    LESS
}
