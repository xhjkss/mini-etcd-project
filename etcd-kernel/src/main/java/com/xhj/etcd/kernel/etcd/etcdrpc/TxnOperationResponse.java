package com.xhj.etcd.kernel.etcd.etcdrpc;

import lombok.Data;

import java.io.Serializable;

/**
 * TxnOperationResponse
 *
 * @author XJks
 * @description Txn 分支内单个操作的响应对象。
 */
@Data
public class TxnOperationResponse implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * 操作类型。
     */
    private TxnOperationType operationType;

    /**
     * 响应数据。
     *
     * <p>当前阶段采用统一 type + data 结构，data 的真实类型由 operationType 决定。</p>
     */
    private Object data;

    public static TxnOperationResponse ofPut(PutResponse putResponse) {
        return of(TxnOperationType.PUT, putResponse);
    }

    public static TxnOperationResponse ofDelete(DeleteResponse deleteResponse) {
        return of(TxnOperationType.DELETE, deleteResponse);
    }

    public static TxnOperationResponse ofGet(GetResponse getResponse) {
        return of(TxnOperationType.GET, getResponse);
    }

    public static TxnOperationResponse ofRange(RangeResponse rangeResponse) {
        return of(TxnOperationType.RANGE, rangeResponse);
    }

    public static TxnOperationResponse ofDeleteRange(DeleteRangeResponse deleteRangeResponse) {
        return of(TxnOperationType.DELETE_RANGE, deleteRangeResponse);
    }

    /**
     * 构造通用响应操作。
     */
    public static TxnOperationResponse of(TxnOperationType operationType, Object data) {
        TxnOperationResponse responseOp = new TxnOperationResponse();
        responseOp.setOperationType(operationType);
        responseOp.setData(data);
        return responseOp;
    }

    /**
     * 按期望类型读取响应数据。
     */
    public <T> T dataAs(Class<T> expectedType) {
        if (data == null) {
            return null;
        }
        if (!expectedType.isInstance(data)) {
            throw new IllegalArgumentException("unexpected txn response operation data type, expected="
                    + expectedType.getName() + ", actual=" + data.getClass().getName());
        }
        return expectedType.cast(data);
    }
}
