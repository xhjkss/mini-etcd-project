/**
 * ApiClient
 *
 * @author XJks
 * @description 控制台前端 API 调用分层，统一维护 URL 与请求方法。
 */
(function (global) {
    'use strict';

    /**
     * 统一 POST 调用入口。
     */
    function post(url, body) {
        return axios.post(url, body || {});
    }

    /**
     * 统一 GET 调用入口。
     */
    function get(url) {
        return axios.get(url);
    }

    /**
     * 统一 DELETE 调用入口（支持携带请求体）。
     */
    function del(url, body) {
        var config = {};
        if (body) {
            config.data = body;
        }
        return axios.delete(url, config);
    }

    var apiClient = {
        // ==================== Connection API ====================
        connections: {
            list: function () {
                return get('/api/connections');
            },
            connect: function (connectRequest) {
                return post('/api/connections', connectRequest);
            },
            disconnect: function (disconnectRequest) {
                return del('/api/connections', disconnectRequest);
            }
        },
        // ==================== MVCC API ====================
        mvcc: {
            getOnEndpoint: function (host, port, getRequest) {
                return post('/api/mvcc/get?host=' + encodeURIComponent(host) + '&port=' + encodeURIComponent(port), getRequest);
            },
            rangeOnEndpoint: function (host, port, rangeRequest) {
                return post('/api/mvcc/range?host=' + encodeURIComponent(host) + '&port=' + encodeURIComponent(port), rangeRequest);
            },
            put: function (putRequest) {
                return post('/api/mvcc/put', putRequest);
            },
            deleteByKey: function (deleteRequest) {
                return post('/api/mvcc/delete', deleteRequest);
            },
            deleteRange: function (deleteRangeRequest) {
                return post('/api/mvcc/delete-range', deleteRangeRequest);
            }
        },
        // ==================== Txn API ====================
        txn: {
            execute: function (txnExecuteRequest) {
                return post('/api/txn/execute', txnExecuteRequest);
            }
        },
        // ==================== Watch API ====================
        watch: {
            list: function () {
                return get('/api/watch');
            },
            startOnEndpoint: function (host, port, watchSubscribeRequest) {
                return post('/api/watch/start?host=' + encodeURIComponent(host) + '&port=' + encodeURIComponent(port), watchSubscribeRequest);
            },
            cancel: function (watchId) {
                return del('/api/watch?watchId=' + encodeURIComponent(watchId));
            }
        },
        // ==================== Lease API ====================
        lease: {
            grant: function (leaseGrantRequest) {
                return post('/api/lease/grant', leaseGrantRequest);
            },
            revoke: function (leaseRevokeRequest) {
                return post('/api/lease/revoke', leaseRevokeRequest);
            },
            ttl: function (leaseTtlRequest) {
                return post('/api/lease/ttl', leaseTtlRequest);
            },
            list: function (leaseListRequest) {
                return post('/api/lease/list', leaseListRequest || {});
            },
            startSession: function (leaseSessionStartRequest) {
                return post('/api/lease/session/start', leaseSessionStartRequest);
            },
            grantAndStartSession: function (leaseSessionGrantStartRequest) {
                return post('/api/lease/session/grant-start', leaseSessionGrantStartRequest);
            },
            stopSession: function (leaseId) {
                return del('/api/lease/session?leaseId=' + encodeURIComponent(leaseId));
            },
            listSessions: function () {
                return get('/api/lease/session');
            }
        },
        // ==================== Compact API ====================
        compact: {
            execute: function (compactRequest) {
                return post('/api/compact', compactRequest);
            }
        },
        // ==================== Cluster Diagnostic API ====================
        cluster: {
            listNodeStatusOnAllNodes: function () {
                return get('/api/cluster/node-status/on-all-nodes');
            },
            getNodeStatusOnEndpoint: function (host, port) {
                return get('/api/cluster/node-status/on-node?host=' + encodeURIComponent(host) + '&port=' + encodeURIComponent(port));
            },
            computeKvStateHashOnEndpoint: function (host, port, kvStateHashRequest) {
                return post('/api/cluster/kv-state-hash/on-node?host=' + encodeURIComponent(host) + '&port=' + encodeURIComponent(port), kvStateHashRequest);
            },
            rangeOnAllNodes: function (rangeRequest) {
                return post('/api/cluster/range/on-all-nodes', rangeRequest);
            }
        }
    };

    global.apiClient = apiClient;
})(window);
