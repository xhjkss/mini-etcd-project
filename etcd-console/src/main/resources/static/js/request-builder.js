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
