package com.xhj.etcd.kernel.etcd.etcdrpc;

import lombok.Data;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * TxnRequest
 *
 * @author XJks
 * @description Txn 请求对象，包含 compare 条件和 success/failure 两个分支。
 */
@Data
public class TxnRequest implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * compare 条件列表。
     */
    private List<TxnCompareCondition> compareConditions = new ArrayList<>();

    /**
     * compare=true 时执行的分支操作。
     */
    private List<TxnOperationRequest> successOperations = new ArrayList<>();

    /**
     * compare=false 时执行的分支操作。
     */
    private List<TxnOperationRequest> failureOperations = new ArrayList<>();
}
