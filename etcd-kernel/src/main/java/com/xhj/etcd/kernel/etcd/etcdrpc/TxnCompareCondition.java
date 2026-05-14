package com.xhj.etcd.kernel.etcd.etcdrpc;

import lombok.Data;

import java.io.Serializable;

/**
 * TxnCompareCondition
 *
 * @author XJks
 * @description Txn compare 条件。
 *
 * <p>一个条件只比较一个 key 的一个目标字段。</p>
 */
@Data
public class TxnCompareCondition implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * 参与比较的 key。
     */
    private String key;

    /**
     * 比较目标字段。
     */
    private TxnCompareFieldType compareFieldType;

    /**
     * 比较运算符。
     */
    private TxnCompareOperatorType compareOperatorType;

    /**
     * 比较数据。
     *
     * <p>统一 type + data 结构，data 的真实类型由 compareFieldType 决定：</p>
     * <ul>
     *     <li>VALUE -> String</li>
     *     <li>VERSION / CREATE_REVISION / MOD_REVISION -> Long</li>
     * </ul>
     */
    private Object data;

    /**
     * 构造 VALUE compare 条件。
     */
    public static TxnCompareCondition value(String key, TxnCompareOperatorType compareOperatorType, String expectedValue) {
        return of(key, TxnCompareFieldType.VALUE, compareOperatorType, expectedValue);
    }

    /**
     * 构造 VERSION compare 条件。
     */
    public static TxnCompareCondition version(String key, TxnCompareOperatorType compareOperatorType, long expectedVersion) {
        return of(key, TxnCompareFieldType.VERSION, compareOperatorType, expectedVersion);
    }

    /**
     * 构造 CREATE_REVISION compare 条件。
     */
    public static TxnCompareCondition createRevision(String key, TxnCompareOperatorType compareOperatorType, long expectedCreateRevision) {
        return of(key, TxnCompareFieldType.CREATE_REVISION, compareOperatorType, expectedCreateRevision);
    }

    /**
     * 构造 MOD_REVISION compare 条件。
     */
    public static TxnCompareCondition modRevision(String key, TxnCompareOperatorType compareOperatorType, long expectedModRevision) {
        return of(key, TxnCompareFieldType.MOD_REVISION, compareOperatorType, expectedModRevision);
    }

    /**
     * 构造通用 compare 条件。
     */
    public static TxnCompareCondition of(String key, TxnCompareFieldType compareFieldType, TxnCompareOperatorType compareOperatorType, Object data) {
        TxnCompareCondition compareCondition = new TxnCompareCondition();
        compareCondition.setKey(key);
        compareCondition.setCompareFieldType(compareFieldType);
        compareCondition.setCompareOperatorType(compareOperatorType);
        compareCondition.setData(data);
        return compareCondition;
    }

    /**
     * 按期望类型读取 compare 数据。
     */
    public <T> T dataAs(Class<T> expectedType) {
        if (data == null) {
            return null;
        }
        if (!expectedType.isInstance(data)) {
            throw new IllegalArgumentException("unexpected txn compare condition data type, expected=" + expectedType.getName() + ", actual=" + data.getClass().getName());
        }
        return expectedType.cast(data);
    }
}
