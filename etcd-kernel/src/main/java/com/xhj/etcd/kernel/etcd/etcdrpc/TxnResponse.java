package com.xhj.etcd.kernel.etcd.etcdrpc;

import lombok.Data;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * TxnResponse
 *
 * @author XJks
 * @description Txn 执行结果。
 */
@Data
public class TxnResponse implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * compare 条件是否成立。
     */
    private boolean succeeded;

    /**
     * Txn 完成后的状态机 revision。
     */
    private long revision;

    /**
     * 分支执行响应列表，按执行顺序返回。
     */
    private List<TxnOperationResponse> responses = new ArrayList<>();

    /**
     * 构造 Txn 成功响应体。
     */
    public static TxnResponse of(boolean succeeded, long revision, List<TxnOperationResponse> responses) {
        TxnResponse txnResponse = new TxnResponse();
        txnResponse.setSucceeded(succeeded);
        txnResponse.setRevision(revision);
        if (responses != null) {
            txnResponse.setResponses(new ArrayList<>(responses));
        }
        return txnResponse;
    }
}
