package com.xhj.etcd.console.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.xhj.etcd.kernel.etcd.etcdrpc.DeleteRangeRequest;
import com.xhj.etcd.kernel.etcd.etcdrpc.DeleteRequest;
import com.xhj.etcd.kernel.etcd.etcdrpc.GetRequest;
import com.xhj.etcd.kernel.etcd.etcdrpc.PutRequest;
import com.xhj.etcd.kernel.etcd.etcdrpc.RangeRequest;
import com.xhj.etcd.kernel.etcd.etcdrpc.TxnCompareCondition;
import com.xhj.etcd.kernel.etcd.etcdrpc.TxnCompareFieldType;
import com.xhj.etcd.kernel.etcd.etcdrpc.TxnOperationRequest;
import com.xhj.etcd.kernel.etcd.etcdrpc.TxnOperationType;
import com.xhj.etcd.kernel.etcd.etcdrpc.TxnRequest;
import com.xhj.etcd.kernel.etcd.etcdrpc.TxnResponse;
import com.xhj.etcd.sdk.client.EtcdClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

/**
 * TxnService
 *
 * @author XJks
 * @description Console Txn 服务，负责把 UI 请求转换为内核 TxnRequest 并执行。
 */
@Service
public class TxnService {

    /**
     * JSON 对象转换器，用于把 REST 入参中的 Object data 规范化为 etcdrpc 强类型请求。
     */
    @Autowired
    private ObjectMapper objectMapper;

    /**
     * 连接服务。
     */
    @Autowired
    private ConnectionService connectionService;

    /**
     * 执行 Txn 请求。
     */
    public TxnResponse execute(TxnRequest txnRequest) {
        if (txnRequest == null) {
            throw new IllegalArgumentException("txnRequest must not be null");
        }
        normalizeTxnRequest(txnRequest);
        if (txnRequest.getSuccessOperations().isEmpty() && txnRequest.getFailureOperations().isEmpty()) {
            throw new IllegalArgumentException("at least one branch operation is required");
        }
        EtcdClient etcdClient = connectionService.getEtcdClient();
        return etcdClient.txn(txnRequest);
    }

    /**
     * 规范化 TxnRequest。
     *
     * <p>Console REST 入参直接复用 etcdrpc 的 {@link TxnRequest}。其中 type + data 里的 data
     * 经过 JSON 反序列化后可能是 Map、Integer 等通用类型，这里统一收敛为内核执行需要的强类型对象。</p>
     */
    private void normalizeTxnRequest(TxnRequest txnRequest) {
        txnRequest.setCompareConditions(normalizeCompareConditionList(txnRequest.getCompareConditions()));
        txnRequest.setSuccessOperations(normalizeOperationRequestList(txnRequest.getSuccessOperations(), "successOperations"));
        txnRequest.setFailureOperations(normalizeOperationRequestList(txnRequest.getFailureOperations(), "failureOperations"));
    }

    /**
     * 规范化 compare 条件列表。
     */
    private List<TxnCompareCondition> normalizeCompareConditionList(List<TxnCompareCondition> compareConditionList) {
        if (compareConditionList == null || compareConditionList.isEmpty()) {
            throw new IllegalArgumentException("compareConditions must not be empty");
        }
        List<TxnCompareCondition> normalizedCompareConditionList = new ArrayList<>(compareConditionList.size());
        for (int index = 0; index < compareConditionList.size(); index++) {
            normalizedCompareConditionList.add(normalizeCompareCondition(compareConditionList.get(index), "compareConditions[" + index + "]"));
        }
        return normalizedCompareConditionList;
    }

    /**
     * 规范化单个 compare 条件。
     */
    private TxnCompareCondition normalizeCompareCondition(TxnCompareCondition compareCondition, String fieldName) {
        if (compareCondition == null) {
            throw new IllegalArgumentException(fieldName + " must not be null");
        }
        String key = requireNonBlank(compareCondition.getKey(), fieldName + ".key");
        TxnCompareFieldType compareFieldType = requireCompareFieldType(compareCondition.getCompareFieldType(), fieldName + ".compareFieldType");
        if (compareCondition.getCompareOperatorType() == null) {
            throw new IllegalArgumentException(fieldName + ".compareOperatorType must not be null");
        }
        Object data;
        if (compareFieldType == TxnCompareFieldType.VALUE) {
            data = compareCondition.getData() == null ? "" : String.valueOf(compareCondition.getData());
        } else {
            data = requireNonNegativeLong(compareCondition.getData(), fieldName + ".data");
        }
        return TxnCompareCondition.of(key, compareFieldType, compareCondition.getCompareOperatorType(), data);
    }

    /**
     * 规范化分支操作列表。
     */
    private List<TxnOperationRequest> normalizeOperationRequestList(List<TxnOperationRequest> operationRequestList, String fieldName) {
        if (operationRequestList == null || operationRequestList.isEmpty()) {
            return new ArrayList<>();
        }
        List<TxnOperationRequest> normalizedOperationRequestList = new ArrayList<>(operationRequestList.size());
        for (int index = 0; index < operationRequestList.size(); index++) {
            normalizedOperationRequestList.add(normalizeOperationRequest(operationRequestList.get(index), fieldName + "[" + index + "]"));
        }
        return normalizedOperationRequestList;
    }

    /**
     * 规范化单个分支操作。
     */
    private TxnOperationRequest normalizeOperationRequest(TxnOperationRequest operationRequest, String fieldName) {
        if (operationRequest == null) {
            throw new IllegalArgumentException(fieldName + " must not be null");
        }
        TxnOperationType operationType = operationRequest.getOperationType();
        if (operationType == null) {
            throw new IllegalArgumentException(fieldName + ".operationType must not be null");
        }
        switch (operationType) {
            case PUT:
                PutRequest putRequest = convertOperationData(operationRequest.getData(), PutRequest.class, fieldName + ".data");
                putRequest.setKey(requireNonBlank(putRequest.getKey(), fieldName + ".data.key"));
                putRequest.setValue(putRequest.getValue() == null ? "" : putRequest.getValue());
                putRequest.setLeaseId(Math.max(0L, putRequest.getLeaseId()));
                return TxnOperationRequest.put(putRequest);
            case DELETE:
                DeleteRequest deleteRequest = convertOperationData(operationRequest.getData(), DeleteRequest.class, fieldName + ".data");
                deleteRequest.setKey(requireNonBlank(deleteRequest.getKey(), fieldName + ".data.key"));
                return TxnOperationRequest.delete(deleteRequest);
            case GET:
                GetRequest getRequest = convertOperationData(operationRequest.getData(), GetRequest.class, fieldName + ".data");
                getRequest.setKey(requireNonBlank(getRequest.getKey(), fieldName + ".data.key"));
                return TxnOperationRequest.get(getRequest);
            case RANGE:
                RangeRequest rangeRequest = convertOperationData(operationRequest.getData(), RangeRequest.class, fieldName + ".data");
                rangeRequest.setStartKey(requireNonBlank(rangeRequest.getStartKey(), fieldName + ".data.startKey"));
                if (rangeRequest.getEndKeyExclusive() == null) {
                    rangeRequest.setEndKeyExclusive("");
                }
                return TxnOperationRequest.range(rangeRequest);
            case DELETE_RANGE:
                DeleteRangeRequest deleteRangeRequest = convertOperationData(operationRequest.getData(), DeleteRangeRequest.class, fieldName + ".data");
                deleteRangeRequest.setStartKey(requireNonBlank(deleteRangeRequest.getStartKey(), fieldName + ".data.startKey"));
                if (deleteRangeRequest.getEndKeyExclusive() == null) {
                    deleteRangeRequest.setEndKeyExclusive("");
                }
                return TxnOperationRequest.deleteRange(deleteRangeRequest);
            default:
                throw new IllegalArgumentException("unsupported txn operation type: " + operationType);
        }
    }

    /**
     * 转换 TxnOperationRequest.data 为 etcdrpc 强类型请求。
     */
    private <T> T convertOperationData(Object data, Class<T> expectedType, String fieldName) {
        if (data == null) {
            throw new IllegalArgumentException(fieldName + " must not be null");
        }
        if (expectedType.isInstance(data)) {
            return expectedType.cast(data);
        }
        return objectMapper.convertValue(data, expectedType);
    }

    /**
     * 非空字符串校验。
     */
    private String requireNonBlank(String value, String fieldName) {
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException(fieldName + " must not be blank");
        }
        return value.trim();
    }

    /**
     * compare 字段类型校验。
     */
    private TxnCompareFieldType requireCompareFieldType(TxnCompareFieldType compareFieldType, String fieldName) {
        if (compareFieldType == null) {
            throw new IllegalArgumentException(fieldName + " must not be null");
        }
        return compareFieldType;
    }

    /**
     * 非负数校验。
     */
    private long requireNonNegativeLong(Object value, String fieldName) {
        if (value == null) {
            throw new IllegalArgumentException(fieldName + " must not be null");
        }
        long number;
        if (value instanceof Number) {
            number = ((Number) value).longValue();
        } else {
            try {
                number = Long.parseLong(String.valueOf(value));
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException(fieldName + " must be a number");
            }
        }
        if (number < 0L) {
            throw new IllegalArgumentException(fieldName + " must be >= 0");
        }
        return number;
    }
}
