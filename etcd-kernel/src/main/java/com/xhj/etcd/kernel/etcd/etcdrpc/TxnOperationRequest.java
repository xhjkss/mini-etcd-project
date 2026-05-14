package com.xhj.etcd.kernel.etcd.etcdrpc;

import lombok.Data;

import java.io.Serializable;

/**
 * TxnOperationRequest
 *
 * @author XJks
 * @description Txn 分支中的单个请求操作。
 *
 * <p>当前阶段不支持嵌套 Txn，因此这里只包含 KV 五种操作。</p>
 */
@Data
public class TxnOperationRequest implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * 操作类型。
     */
    private TxnOperationType operationType;

    /**
     * 操作数据。
     *
     * <p>当前阶段采用统一 type + data 结构，data 的真实类型由 operationType 决定。</p>
     */
    private Object data;

    /**
     * 构造 PUT 操作。
     */
    public static TxnOperationRequest put(PutRequest putRequest) {
        return of(TxnOperationType.PUT, putRequest);
    }

    /**
     * 构造 DELETE 操作。
     */
    public static TxnOperationRequest delete(DeleteRequest deleteRequest) {
        return of(TxnOperationType.DELETE, deleteRequest);
    }

    /**
     * 构造 GET 操作。
     */
    public static TxnOperationRequest get(GetRequest getRequest) {
        return of(TxnOperationType.GET, getRequest);
    }

    /**
     * 构造 RANGE 操作。
     */
    public static TxnOperationRequest range(RangeRequest rangeRequest) {
        return of(TxnOperationType.RANGE, rangeRequest);
    }

    /**
     * 构造 DELETE_RANGE 操作。
     */
    public static TxnOperationRequest deleteRange(DeleteRangeRequest deleteRangeRequest) {
        return of(TxnOperationType.DELETE_RANGE, deleteRangeRequest);
    }

    /**
     * 构造通用操作。
     */
    public static TxnOperationRequest of(TxnOperationType operationType, Object data) {
        TxnOperationRequest operation = new TxnOperationRequest();
        operation.setOperationType(operationType);
        operation.setData(data);
        return operation;
    }

    /**
     * 按期望类型读取操作数据。
     */
    public <T> T dataAs(Class<T> expectedType) {
        if (data == null) {
            return null;
        }
        if (!expectedType.isInstance(data)) {
            throw new IllegalArgumentException("unexpected txn request operation data type, expected="
                    + expectedType.getName() + ", actual=" + data.getClass().getName());
        }
        return expectedType.cast(data);
    }
}
