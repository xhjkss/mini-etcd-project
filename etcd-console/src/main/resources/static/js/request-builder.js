/**
 * RequestBuilder
 *
 * @author XJks
 * @description 前端请求体构建器，统一维护与 etcdrpc 对齐的字段结构。
 */
(function (global) {
    'use strict';

    /**
     * 把输入转换为数字；非法值统一回落为 0，保持与后端请求字段类型一致。
     */
    function toNumberOrZero(value) {
        var number = Number(value);
        return isNaN(number) ? 0 : number;
    }

    var requestBuilder = {
        // ==================== MVCC Request Builder ====================
        mvcc: {
            /**
             * 构造前缀查询请求（prefixMatch=true）。
             */
            buildRangeByPrefix: function (prefix) {
                return {
                    startKey: prefix,
                    endKeyExclusive: '',
                    prefixMatch: true,
                    limit: 0,
                    revision: 0,
                    linearizableRead: false
                };
            },
            /**
             * 构造“全量浏览”查询请求。
             */
            buildRangeAll: function () {
                return {
                    startKey: '!',
                    endKeyExclusive: '\uffff\uffff\uffff',
                    prefixMatch: false,
                    limit: 0,
                    revision: 0,
                    linearizableRead: false
                };
            },
            /**
             * 构造 PUT 请求。
             */
            buildPut: function (key, value, leaseId) {
                return {
                    key: key,
                    value: value,
                    leaseId: toNumberOrZero(leaseId)
                };
            },
            /**
             * 构造 GET 请求（默认线性一致读）。
             */
            buildGet: function (key) {
                return {
                    key: key,
                    revision: 0,
                    linearizableRead: true
                };
            },
            /**
             * 构造 DELETE 单 key 请求。
             */
            buildDelete: function (key) {
                return {key: key};
            },
            /**
             * 构造 DELETE_RANGE（前缀删除）请求。
             */
            buildDeleteRangeByPrefix: function (prefix) {
                return {
                    startKey: prefix,
                    endKeyExclusive: '',
                    prefixMatch: true
                };
            }
        },
        // ==================== Txn Request Builder ====================
        txn: {
            /**
             * 构造 Txn compare 条件，字段与 etcdrpc.TxnCompareCondition 对齐。
             */
            buildCompareCondition: function (key, compareFieldType, compareOperatorType, value, longValue) {
                return {
                    key: key,
                    compareFieldType: compareFieldType,
                    compareOperatorType: compareOperatorType,
                    data: compareFieldType === 'VALUE' ? (value || '') : toNumberOrZero(longValue)
                };
            },
            /**
             * 构造 Txn 分支操作，字段与 etcdrpc.TxnOperationRequest 对齐。
             */
            buildOperationRequest: function (operationType, key, value, leaseId, prefixMatch) {
                if (operationType === 'PUT') {
                    return {
                        operationType: operationType,
                        data: requestBuilder.mvcc.buildPut(key, value || '', leaseId)
                    };
                }
                if (operationType === 'DELETE') {
                    return {
                        operationType: operationType,
                        data: requestBuilder.mvcc.buildDelete(key)
                    };
                }
                if (operationType === 'GET') {
                    return {
                        operationType: operationType,
                        data: requestBuilder.mvcc.buildGet(key)
                    };
                }
                if (operationType === 'RANGE') {
                    return {
                        operationType: operationType,
                        data: {
                            startKey: key,
                            endKeyExclusive: '',
                            prefixMatch: !!prefixMatch,
                            limit: 0,
                            keysOnly: false,
                            countOnly: false,
                            revision: 0,
                            linearizableRead: false
                        }
                    };
                }
                if (operationType === 'DELETE_RANGE') {
                    return {
                        operationType: operationType,
                        data: {
                            startKey: key,
                            endKeyExclusive: '',
                            prefixMatch: !!prefixMatch,
                            prevKv: false
                        }
                    };
                }
                return {
                    operationType: operationType,
                    data: {}
                };
            },
            /**
             * 构造 TxnRequest，直接复用 etcdrpc.TxnRequest 的 compare/success/failure 字段结构。
             */
            buildExecute: function (txnForm) {
                txnForm = txnForm || {};
                return {
                    compareConditions: [
                        this.buildCompareCondition(
                            txnForm.compareKey,
                            txnForm.compareFieldType,
                            txnForm.compareOperatorType,
                            txnForm.compareValue,
                            txnForm.compareLongValue)
                    ],
                    successOperations: [
                        this.buildOperationRequest(
                            txnForm.successOperationType,
                            txnForm.successKey,
                            txnForm.successValue,
                            txnForm.successLeaseId,
                            txnForm.successPrefixMatch)
                    ],
                    failureOperations: [
                        this.buildOperationRequest(
                            txnForm.failureOperationType,
                            txnForm.failureKey,
                            txnForm.failureValue,
                            txnForm.failureLeaseId,
                            txnForm.failurePrefixMatch)
                    ]
                };
            }
        },
        // ==================== Watch Request Builder ====================
        watch: {
            buildSubscribe: function (startKey, prefixMatch) {
                return {
                    startKey: startKey,
                    prefixMatch: !!prefixMatch,
                    startRevision: 0,
                    maxEvents: 128,
                    leaderOnly: false
                };
            }
        },
        // ==================== Lease Request Builder ====================
        lease: {
            buildGrant: function (leaseId, ttlSeconds) {
                return {
                    leaseId: toNumberOrZero(leaseId),
                    ttlSeconds: toNumberOrZero(ttlSeconds)
                };
            },
            buildTtl: function (leaseId) {
                return {leaseId: toNumberOrZero(leaseId)};
            },
            buildRevoke: function (leaseId) {
                return {leaseId: toNumberOrZero(leaseId)};
            },
            buildList: function () {
                return {};
            },
            buildSessionStart: function (leaseId) {
                return {
                    leaseId: toNumberOrZero(leaseId)
                };
            },
            buildSessionGrantStart: function (leaseId, ttlSeconds) {
                return {
                    leaseId: toNumberOrZero(leaseId),
                    ttlSeconds: toNumberOrZero(ttlSeconds)
                };
            }
        },
        // ==================== Cluster Diagnostic Request Builder ====================
        cluster: {
            buildKvStateHash: function () {
                return {revision: 0};
            },
            buildRangeByPrefix: function (prefix) {
                return {
                    startKey: prefix,
                    endKeyExclusive: '',
                    prefixMatch: true,
                    limit: 0,
                    revision: 0,
                    linearizableRead: false
                };
            }
        },
        // ==================== Compact Request Builder ====================
        compact: {
            buildCompact: function (revision) {
                return {revision: toNumberOrZero(revision)};
            }
        }
    };

    global.requestBuilder = requestBuilder;
})(window);
