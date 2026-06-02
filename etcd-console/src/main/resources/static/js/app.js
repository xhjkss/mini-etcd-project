Vue.component('result-panel', {
    template: '#result-panel-template',
    props: ['summary', 'raw']
});

var app = new Vue({
    el: '#app',
    data: {
        webSocketConnected: false,
        webSocketClient: null,
        webSocketReconnectTimer: null,
        pageDestroyed: false,
        activeView: 'browser',
        activeOperationTab: 'kv',
        navItems: [
            {key: 'browser', label: '数据浏览', icon: 'el-icon-folder-opened'},
            {key: 'operations', label: '操作中心', icon: 'el-icon-s-operation'},
            {key: 'nodes', label: '节点状态', icon: 'el-icon-monitor'},
            {key: 'connections', label: '连接管理', icon: 'el-icon-connection'},
            {key: 'logs', label: '操作日志', icon: 'el-icon-document'}
        ],
        operationTabs: [
            {key: 'kv', label: 'KV 操作'},
            {key: 'txn', label: 'Txn 事务'},
            {key: 'watch', label: 'Watch 监听'},
            {key: 'lease', label: 'Lease 管理'},
            {key: 'compact', label: 'Compact / 对比'}
        ],
        connections: [],
        currentNodeId: '',
        nodeStatusMap: {},
        kvStateHashMap: {},
        result: null,
        resultSummary: null,
        browserPrefix: '',
        browserLoading: false,
        browserAutoRefreshEnabled: true,
        browserAutoRefreshTimer: null,
        browserRealtimeRefreshTimer: null,
        lastBrowserRefreshAt: '',
        browserLoadError: '',
        keyItems: [],
        selectedKv: null,
        connectForm: {
            host: '127.0.0.1',
            port: null
        },
        kvForm: {
            key: '',
            value: '',
            leaseId: null
        },
        txnForm: {
            compareKey: '',
            compareFieldType: 'VALUE',
            compareOperatorType: 'EQUAL',
            compareValue: '',
            compareLongValue: null,
            successOperationType: 'PUT',
            successKey: '',
            successValue: '',
            successLeaseId: null,
            successPrefixMatch: false,
            failureOperationType: 'PUT',
            failureKey: '',
            failureValue: '',
            failureLeaseId: null,
            failurePrefixMatch: false
        },
        txnOperationResult: null,
        txnOperationSummary: null,
        watchForm: {
            key: '',
            prefix: false
        },
        watchSessions: [],
        watchEventListByWatchId: {},
        selectedWatchId: '',
        showAllWatchEvents: false,
        webSocketStats: {
            messageCount: 0,
            reconnectCount: 0,
            parseErrorCount: 0,
            lastConnectedAt: '',
            lastMessageAt: '',
            lastErrorMessage: ''
        },
        leaseForm: {
            leaseId: null,
            ttlSeconds: null
        },
        leaseInnerTab: 'plain',
        leaseOperationResult: null,
        leaseOperationSummary: null,
        leaseSessions: [],
        leaseSessionStateByLeaseId: {},
        leaseEventListByLeaseId: {},
        selectedLeaseSessionId: 0,
        comparePrefixValue: '',
        comparePrefixOperationResult: null,
        comparePrefixOperationSummary: null,
        compactRevision: 0,
        compactOperationResult: null,
        compactOperationSummary: null,
        operationLogs: [],
        treeProps: {children: 'children', label: 'name'},
        modal: {
            visible: false,
            title: '',
            message: '',
            confirmText: '',
            input: '',
            onConfirm: function () {
            }
        },
        kvEditor: {
            visible: false,
            mode: 'create',
            key: '',
            value: '',
            leaseId: null
        }
    },
    computed: {
        hasCurrentConnection: function () {
            return !!this.currentNodeId;
        },
        canConnect: function () {
            return !!this.validateConnectFormWithMode(false);
        },
        hasKvFormKey: function () {
            return !!this.trimString(this.kvForm.key);
        },
        hasWatchFormKey: function () {
            return !!this.trimString(this.watchForm.key);
        },
        hasSelectedKv: function () {
            return !!this.selectedKv;
        },
        currentEndpointLabel: function () {
            return this.displayConnectionLabel(this.currentNodeId);
        },
        activeViewLabel: function () {
            for (var i = 0; i < this.navItems.length; i++) {
                if (this.navItems[i].key === this.activeView) {
                    return this.navItems[i].label;
                }
            }
            return '-';
        },
        normalizedConnectEndpoint: function () {
            return this.connectionEndpoint(this.connectForm);
        },
        leaderDisplay: function () {
            if (!this.leaderNodeId) {
                return '';
            }
            var conn = this.findConnectionByNodeId(this.leaderNodeId);
            return conn ? this.connectionEndpoint(conn) : '-';
        },
        prettyResult: function () {
            if (this.result === null || typeof this.result === 'undefined') {
                return '暂无结果';
            }
            return JSON.stringify(this.result, null, 2);
        },
        leaderNodeId: function () {
            var keys = Object.keys(this.nodeStatusMap || {});
            for (var i = 0; i < keys.length; i++) {
                var status = this.nodeStatusMap[keys[i]] || {};
                if ((status.role || '').toUpperCase() === 'LEADER') {
                    return keys[i];
                }
            }
            for (var j = 0; j < keys.length; j++) {
                var leaderId = (this.nodeStatusMap[keys[j]] || {}).leaderId;
                if (!leaderId) {
                    continue;
                }
                for (var k = 0; k < this.connections.length; k++) {
                    var conn = this.connections[k];
                    if (conn.nodeId === leaderId || this.connectionEndpoint(conn) === leaderId) {
                        return conn.nodeId;
                    }
                }
            }
            return '';
        },
        onlineNodeCount: function () {
            return this.connections.length;
        },
        clusterHealthLabel: function () {
            if (!this.connections.length) {
                return '未连接';
            }
            if (this.onlineNodeCount === this.connections.length && this.leaderNodeId) {
                return '健康';
            }
            if (this.onlineNodeCount > 0) {
                return '部分异常';
            }
            return '不可用';
        },
        clusterHealthText: function () {
            return '集群：' + this.clusterHealthLabel;
        },
        clusterHealthTagType: function () {
            if (this.clusterHealthLabel === '健康') {
                return 'success';
            }
            if (this.clusterHealthLabel === '未连接') {
                return 'info';
            }
            return this.clusterHealthLabel === '不可用' ? 'danger' : 'warning';
        },
        treeData: function () {
            return this.buildTree(this.keyItems);
        },
        treeForElement: function () {
            return (this.treeData && this.treeData.children) ? this.treeData.children : [];
        },
        watchSessionListSorted: function () {
            var sessionList = this.watchSessions ? this.watchSessions.slice() : [];
            sessionList.sort(function (leftSession, rightSession) {
                return Number(leftSession.watchId || 0) - Number(rightSession.watchId || 0);
            });
            return sessionList;
        },
        selectedWatchEventList: function () {
            var selectedWatchId = Number(this.selectedWatchId);
            if (!selectedWatchId) {
                return [];
            }
            return this.watchEventListByWatchId[selectedWatchId] || [];
        },
        selectedWatchEventsText: function () {
            if (!this.selectedWatchEventList.length) {
                return '暂无 Watch 事件';
            }
            return this.selectedWatchEventList.map(function (watchEvent) {
                var endpointPrefix = watchEvent.endpoint ? ('[' + watchEvent.endpoint + '] ') : '';
                return endpointPrefix + watchEvent.text;
            }).join('\n');
        },
        allWatchEventsText: function () {
            var watchIdList = Object.keys(this.watchEventListByWatchId || {});
            if (!watchIdList.length) {
                return '暂无 Watch 事件';
            }
            var mergedEventList = [];
            for (var i = 0; i < watchIdList.length; i++) {
                var watchId = watchIdList[i];
                var watchEventList = this.watchEventListByWatchId[watchId] || [];
                for (var j = 0; j < watchEventList.length; j++) {
                    mergedEventList.push(watchEventList[j]);
                }
            }
            mergedEventList.sort(function (leftEvent, rightEvent) {
                return Number(rightEvent.at || 0) - Number(leftEvent.at || 0);
            });
            return mergedEventList.map(function (watchEvent) {
                var endpointPrefix = watchEvent.endpoint ? ('[' + watchEvent.endpoint + '] ') : '';
                return endpointPrefix + watchEvent.text;
            }).join('\n');
        },
        leaseSessionListSorted: function () {
            var sessionList = [];
            var leaseSessionStateByLeaseId = this.leaseSessionStateByLeaseId || {};
            var leaseIdList = Object.keys(leaseSessionStateByLeaseId);
            for (var index = 0; index < leaseIdList.length; index++) {
                sessionList.push(leaseSessionStateByLeaseId[leaseIdList[index]]);
            }
            sessionList.sort(function (leftSession, rightSession) {
                if (!!leftSession.active !== !!rightSession.active) {
                    return leftSession.active ? -1 : 1;
                }
                return Number(leftSession.leaseId || 0) - Number(rightSession.leaseId || 0);
            });
            return sessionList;
        },
        selectedLeaseSession: function () {
            var selectedLeaseId = Number(this.selectedLeaseSessionId);
            if (!selectedLeaseId) {
                return null;
            }
            return (this.leaseSessionStateByLeaseId || {})[String(selectedLeaseId)] || null;
        },
        leaseOperationType: function () {
            return this.leaseOperationSummary ? this.leaseOperationSummary.type : '';
        },
        leaseOperationLeaseList: function () {
            var response = this.leaseOperationResult || {};
            var data = response.data || {};
            if (this.leaseOperationType === 'LEASE_LIST') {
                return data.leases || [];
            }
            if (this.leaseOperationType === 'LEASE_GRANT' || this.leaseOperationType === 'LEASE_TTL') {
                return data.lease ? [data.lease] : [];
            }
            return [];
        },
        leaseOperationPrimaryLease: function () {
            return this.leaseOperationLeaseList.length ? this.leaseOperationLeaseList[0] : null;
        },
        selectedLeaseSessionEventsText: function () {
            var selectedLeaseId = String(this.selectedLeaseSessionId || '');
            if (!selectedLeaseId) {
                return '请先选择一个 LeaseHandle 会话';
            }
            var leaseEventList = this.leaseEventListByLeaseId[selectedLeaseId] || [];
            if (!leaseEventList.length) {
                return '该 LeaseHandle 会话暂无状态事件';
            }
            return leaseEventList.map(function (leaseEvent) {
                return '[' + leaseEvent.atText + '] ' + leaseEvent.text;
            }).join('\n');
        },
        selectedKvValueFormat: function () {
            if (!this.selectedKv) {
                return '';
            }
            var value = this.selectedKv.value;
            try {
                JSON.parse(value);
                return 'JSON';
            } catch (e) {
                return 'TEXT';
            }
        },
        selectedKvValueDisplay: function () {
            if (!this.selectedKv) {
                return '';
            }
            var value = this.selectedKv.value;
            try {
                return JSON.stringify(JSON.parse(value), null, 2);
            } catch (e) {
                return value;
            }
        },
        visibleOperationLogs: function () {
            return (this.operationLogs || []).slice(0, 50);
        },
        txnReadableCompare: function () {
            var compareKey = this.trimString(this.txnForm.compareKey);
            if (!compareKey) {
                return '尚未设置 Compare Key';
            }
            var compareData = this.txnForm.compareFieldType === 'VALUE'
                ? ('"' + (this.txnForm.compareValue || '') + '"')
                : this.safeValue(this.txnForm.compareLongValue);
            return compareKey + ' ' + this.txnOperatorLabel(this.txnForm.compareOperatorType)
                + ' ' + this.txnFieldLabel(this.txnForm.compareFieldType) + ' ' + compareData;
        },
        txnReadableSuccessBranch: function () {
            return this.txnBranchReadableSummary('success');
        },
        txnReadableFailureBranch: function () {
            return this.txnBranchReadableSummary('failure');
        },
        txnResultBranchLabel: function () {
            var txnData = this.txnResultData();
            if (!txnData) {
                return '-';
            }
            return txnData.succeeded ? 'Then（Compare=true）' : 'Else（Compare=false）';
        },
        txnResultRevision: function () {
            var txnData = this.txnResultData();
            return txnData ? this.safeValue(txnData.revision) : '-';
        },
        txnResultResponses: function () {
            var txnData = this.txnResultData();
            return txnData && txnData.responses ? txnData.responses : [];
        },
        currentOperationNodeStatus: function () {
            if (!this.currentNodeId) {
                return null;
            }
            return this.nodeStatusMap[this.currentNodeId] || null;
        },
        globalGuideTagType: function () {
            if (!this.connections.length) {
                return 'info';
            }
            if (!this.hasCurrentConnection) {
                return 'warning';
            }
            if (!this.leaderNodeId) {
                return 'warning';
            }
            return 'success';
        },
        globalGuideTitle: function () {
            if (!this.connections.length) {
                return '先连接一个节点';
            }
            if (!this.hasCurrentConnection) {
                return '先选择当前操作连接';
            }
            if (!this.leaderNodeId) {
                return '节点已连接，但还没有探测到 Leader';
            }
            return '当前可以直接开始操作';
        },
        globalGuideMessage: function () {
            if (!this.connections.length) {
                return '进入“连接管理”，至少连接一个节点。请填写目标节点的 host 和 port，再补充其他节点。';
            }
            if (!this.hasCurrentConnection) {
                return '你已经连上集群，但还没有选中“当前操作连接”。左侧下拉框决定本次操作发往哪个节点。';
            }
            if (!this.leaderNodeId) {
                return '当前诊断还没识别出 Leader。可以先刷新节点状态，或切到“节点状态”看各节点 term / revision。';
            }
            return '当前操作节点是 ' + (this.currentEndpointLabel || '-') + '。如果你是第一次使用，建议从 KV Put/Get 开始，再试 Watch、Lease、Txn。';
        },
        globalGuideNextStep: function () {
            if (!this.connections.length) {
                return '下一步：去连接管理新增连接';
            }
            if (!this.hasCurrentConnection) {
                return '下一步：在左侧选择当前操作连接';
            }
            return '下一步：去操作中心或数据浏览执行验证';
        },
        leaderHashValue: function () {
            if (!this.leaderNodeId) {
                return null;
            }
            var hashValue = this.kvStateHashMap[this.leaderNodeId];
            return typeof hashValue === 'undefined' ? null : hashValue;
        },
        leaderRevisionValue: function () {
            if (!this.leaderNodeId) {
                return null;
            }
            var leaderStatus = this.nodeStatusMap[this.leaderNodeId] || {};
            return typeof leaderStatus.currentRevision === 'undefined' ? null : leaderStatus.currentRevision;
        },
        nodeRiskCount: function () {
            var count = 0;
            for (var i = 0; i < this.connections.length; i++) {
                if (this.nodeRiskLevel(this.connections[i].nodeId) !== 'low') {
                    count++;
                }
            }
            return count;
        },
        currentOperationRoleLabel: function () {
            return this.currentNodeId ? this.nodeRole(this.currentNodeId) : '-';
        },
        hashConsistencySummary: function () {
            var hashValueList = [];
            var nodeIdList = Object.keys(this.kvStateHashMap || {});
            for (var i = 0; i < nodeIdList.length; i++) {
                if (typeof this.kvStateHashMap[nodeIdList[i]] !== 'undefined' && this.kvStateHashMap[nodeIdList[i]] !== null) {
                    hashValueList.push(String(this.kvStateHashMap[nodeIdList[i]]));
                }
            }
            if (!hashValueList.length) {
                return '未计算';
            }
            var uniqueHashMap = {};
            for (var j = 0; j < hashValueList.length; j++) {
                uniqueHashMap[hashValueList[j]] = true;
            }
            return Object.keys(uniqueHashMap).length === 1 ? '一致' : '不一致';
        },
        hashConsistencyTagType: function () {
            if (this.hashConsistencySummary === '一致') {
                return 'success';
            }
            if (this.hashConsistencySummary === '未计算') {
                return 'info';
            }
            return 'danger';
        },
        revisionConsistencySummary: function () {
            var revisionValueList = [];
            var nodeIdList = Object.keys(this.nodeStatusMap || {});
            for (var i = 0; i < nodeIdList.length; i++) {
                revisionValueList.push(String((this.nodeStatusMap[nodeIdList[i]] || {}).currentRevision));
            }
            if (!revisionValueList.length) {
                return '未探测';
            }
            var uniqueRevisionMap = {};
            for (var j = 0; j < revisionValueList.length; j++) {
                uniqueRevisionMap[revisionValueList[j]] = true;
            }
            return Object.keys(uniqueRevisionMap).length === 1 ? '一致' : '存在差异';
        },
        revisionConsistencyTagType: function () {
            if (this.revisionConsistencySummary === '一致') {
                return 'success';
            }
            if (this.revisionConsistencySummary === '未探测') {
                return 'info';
            }
            return 'warning';
        },
        selectedNodeDiagnosticHint: function () {
            if (!this.currentOperationNodeStatus) {
                return '当前还没有选中操作节点。';
            }
            return '当前操作节点角色=' + this.currentOperationRoleLabel
                + '，revision=' + this.safeValue(this.currentOperationNodeStatus.currentRevision)
                + '，compactRevision=' + this.safeValue(this.currentOperationNodeStatus.compactRevision)
                + '，leaseCount=' + this.safeValue(this.currentOperationNodeStatus.leaseCount)
                + '，watchCount=' + this.safeValue(this.currentOperationNodeStatus.watchCount) + '。';
        },
        comparePrefixNodeRows: function () {
            var rowList = [];
            var response = this.comparePrefixOperationResult || {};
            var rangeResponseList = response.data || [];
            for (var i = 0; i < this.connections.length; i++) {
                var connection = this.connections[i] || {};
                var rangeResponse = rangeResponseList[i] || {};
                rowList.push({
                    nodeId: connection.nodeId || ('node-' + i),
                    endpoint: this.connectionEndpoint(connection),
                    success: rangeResponse.success !== false,
                    count: this.safeValue(rangeResponse.count),
                    revision: this.safeValue(rangeResponse.revision),
                    message: rangeResponse.message || '',
                    fingerprint: this.comparePrefixFingerprint(rangeResponse)
                });
            }
            return rowList;
        },
        comparePrefixConsistencySummary: function () {
            var rowList = this.comparePrefixNodeRows;
            if (!rowList.length) {
                return '未执行';
            }
            var fingerprintMap = {};
            for (var i = 0; i < rowList.length; i++) {
                fingerprintMap[rowList[i].fingerprint] = true;
            }
            return Object.keys(fingerprintMap).length === 1 ? '一致' : '存在差异';
        },
        comparePrefixConsistencyTagType: function () {
            if (this.comparePrefixConsistencySummary === '一致') {
                return 'success';
            }
            if (this.comparePrefixConsistencySummary === '未执行') {
                return 'info';
            }
            return 'warning';
        },
        compactResultData: function () {
            return this.compactOperationResult && this.compactOperationResult.data ? this.compactOperationResult.data : null;
        },
        compactFriendlyMessage: function () {
            var compactData = this.compactResultData;
            if (!compactData) {
                return '还没有执行 Compact。';
            }
            return 'compactRevision 已推进到 ' + this.safeValue(compactData.compactRevision)
                + '，当前状态机 revision 是 ' + this.safeValue(compactData.currentRevision) + '。';
        }
    },
    mounted: function () {
        this.pageDestroyed = false;
        this.connectionLoadConnections();
        this.webSocketConnect();
        this.watchLoadSessions();
        this.leaseLoadSessions();
        this.startBrowserAutoRefresh();
    },
    beforeDestroy: function () {
        this.pageDestroyed = true;
        if (this.browserAutoRefreshTimer) {
            clearInterval(this.browserAutoRefreshTimer);
            this.browserAutoRefreshTimer = null;
        }
        if (this.browserRealtimeRefreshTimer) {
            clearTimeout(this.browserRealtimeRefreshTimer);
            this.browserRealtimeRefreshTimer = null;
        }
        if (this.webSocketReconnectTimer) {
            clearTimeout(this.webSocketReconnectTimer);
            this.webSocketReconnectTimer = null;
        }
        if (this.webSocketClient) {
            this.webSocketClient.close();
            this.webSocketClient = null;
        }
    },
    methods: {
        // ==================== Common Utils ====================
        notify: function (type, message) {
            if (this.$message) {
                this.$message({type: type, message: message, showClose: true, duration: type === 'error' ? 5000 : 3000});
            } else {
                alert(message);
            }
        },
        trimString: function (value) {
            return value === null || typeof value === 'undefined' ? '' : String(value).trim();
        },
        selectInputContents: function () {
            var target = typeof document !== 'undefined' ? document.activeElement : null;
            if (!target || typeof target.select !== 'function') {
                return;
            }
            setTimeout(function () {
                try {
                    target.select();
                } catch (ignore) {
                }
            }, 0);
        },
        connectionEndpoint: function (conn) {
            if (!conn) {
                return '';
            }
            return consoleUtils.normalizeEndpoint(conn.host, conn.port);
        },
        findConnectionByNodeId: function (nodeId) {
            for (var i = 0; i < this.connections.length; i++) {
                if (this.connections[i].nodeId === nodeId) {
                    return this.connections[i];
                }
            }
            return null;
        },
        findConnectionByEndpoint: function (endpoint) {
            for (var i = 0; i < this.connections.length; i++) {
                if (this.connectionEndpoint(this.connections[i]) === endpoint) {
                    return this.connections[i];
                }
            }
            return null;
        },
        displayConnectionLabel: function (nodeId) {
            var conn = this.findConnectionByNodeId(nodeId);
            if (!conn) {
                return '-';
            }
            return this.connectionEndpoint(conn);
        },
        displayEndpointByNodeId: function (nodeId) {
            var conn = this.findConnectionByNodeId(nodeId);
            if (!conn) {
                return '-';
            }
            return this.connectionEndpoint(conn);
        },
        watchSessionEndpoint: function (watchSession) {
            if (!watchSession) {
                return '-';
            }
            return this.displayEndpointByNodeId(watchSession.nodeId);
        },
        operationBlockedReason: function () {
            if (this.hasCurrentConnection) {
                return '';
            }
            return '请先在左侧选择连接，或进入“连接管理”新增连接后再操作。';
        },
        hasPositiveLeaseId: function () {
            var leaseId = Number(this.leaseForm.leaseId);
            return !!leaseId && leaseId > 0;
        },
        hasPositiveCompactRevision: function () {
            var compactRevision = Number(this.compactRevision);
            return !!compactRevision && compactRevision > 0;
        },
        actionDisabledReason: function (actionCode) {
            var requiresConnectionActions = {
                'browser.create': true,
                'browser.edit': true,
                'browser.delete': true,
                'kv.put': true,
                'kv.get': true,
                'kv.rangePrefix': true,
                'kv.delete': true,
                'kv.deletePrefix': true,
                'txn.execute': true,
                'watch.start': true,
                'lease.grant': true,
                'lease.ttl': true,
                'lease.list': true,
                'lease.revoke': true,
                'compact.execute': true
            };
            if (requiresConnectionActions[actionCode] && !this.hasCurrentConnection) {
                return '请先在左侧选择一个当前操作连接。';
            }
            if ((actionCode === 'browser.edit' || actionCode === 'browser.delete') && !this.hasSelectedKv) {
                return '请先从左侧目录中选中一个 Key。';
            }
            if ((actionCode === 'kv.put' || actionCode === 'kv.get' || actionCode === 'kv.rangePrefix'
                || actionCode === 'kv.delete' || actionCode === 'kv.deletePrefix') && !this.hasKvFormKey) {
                return '请先填写 Key / Prefix。';
            }
            if (actionCode === 'txn.execute' && !this.trimString(this.txnForm.compareKey)) {
                return '请先填写 Txn Compare Key。';
            }
            if (actionCode === 'watch.start' && !this.hasWatchFormKey) {
                return '请先填写 Watch 的 Key / Prefix。';
            }
            if ((actionCode === 'lease.ttl' || actionCode === 'lease.revoke') && !this.hasPositiveLeaseId()) {
                return '请先填写大于 0 的 LeaseId。';
            }
            if (actionCode === 'compact.comparePrefix' && !this.connections.length) {
                return '请先至少连接一个节点。';
            }
            if (actionCode === 'compact.execute' && !this.hasPositiveCompactRevision()) {
                return '请先填写大于 0 的 Compact Revision。';
            }
            return '';
        },
        displayLogEndpoint: function (log) {
            if (!log) {
                return '-';
            }
            return this.displayEndpointByNodeId(log.nodeId);
        },
        validateConnectForm: function () {
            return this.validateConnectFormWithMode(true);
        },
        validateConnectFormWithMode: function (enableNotify) {
            var host = this.trimString(this.connectForm.host);
            var port = Number(this.connectForm.port);
            if (!host) {
                if (enableNotify) {
                    this.notify('warning', '请填写 Host');
                }
                return null;
            }
            if (!port || port < 1 || port > 65535) {
                if (enableNotify) {
                    this.notify('warning', 'Port 必须是 1 到 65535 之间的数字');
                }
                return null;
            }
            var endpoint = host.toLowerCase() + ':' + port;
            var duplicated = this.findConnectionByEndpoint(endpoint);
            if (duplicated) {
                if (enableNotify) {
                    this.notify('error', 'Endpoint 已存在：' + endpoint + '。请先断开已有连接，不能重复连接同一个节点。');
                }
                return null;
            }
            return {host: host, port: port};
        },
        requireKey: function (value, actionName) {
            var key = this.trimString(value);
            if (!key) {
                this.notify('warning', (actionName || '操作') + '需要填写 Key，不能留空。');
                return '';
            }
            return key;
        },
        requirePrefix: function (value, actionName) {
            var prefix = this.trimString(value);
            if (!prefix) {
                this.notify('warning', (actionName || 'Prefix 查询') + '需要填写 Prefix。当前后端不支持空 Prefix 查询全部，请输入明确前缀，例如 /config/ 或 1。');
                return '';
            }
            return prefix;
        },
        requirePositiveNumber: function (value, label) {
            var number = Number(value);
            if (!number || number <= 0) {
                this.notify('warning', label + ' 必须是大于 0 的数字');
                return null;
            }
            return number;
        },
        requireNonNegativeNumber: function (value, label) {
            var number = Number(value);
            if (isNaN(number) || number < 0) {
                this.notify('warning', label + ' 必须是大于等于 0 的数字');
                return null;
            }
            return number;
        },
        // ==================== Request Context ====================
        selectedConnection: function () {
            if (!this.currentNodeId) {
                this.notify('warning', '请先选择当前操作连接');
                throw new Error('connection required');
            }
            var currentConnection = this.findConnectionByNodeId(this.currentNodeId);
            if (!currentConnection) {
                this.notify('warning', '当前连接不存在，请重新选择连接');
                throw new Error('connection not found');
            }
            return currentConnection;
        },
        selectedEndpointParams: function () {
            var currentConnection = this.selectedConnection();
            return {
                host: currentConnection.host,
                port: currentConnection.port,
                nodeId: currentConnection.nodeId
            };
        },
        apiBusinessSuccess: function (response) {
            if (!response || response.code !== 0) {
                return false;
            }
            return !(response.data && response.data.success === false);
        },
        normalizeBusinessMessage: function (meta, response) {
            response = response || {};
            var data = response.data || {};
            var message = response.message || data.message || '';
            if (meta && meta.type === 'CONNECT' && message) {
                var lowerMessage = String(message).toLowerCase();
                if (lowerMessage.indexOf('connection refused') >= 0
                    || lowerMessage.indexOf('connect timed out') >= 0
                    || lowerMessage.indexOf('no route to host') >= 0
                    || lowerMessage.indexOf('connectexception') >= 0) {
                    return '连接失败：无法连接到目标节点，请确认节点已启动且 host/port 正确。';
                }
            }
            if (message && message.indexOf('watch subscribe failed after retries') >= 0) {
                return 'Watch 订阅失败。请检查当前连接是否可用，并优先连接 Leader 节点后重试。';
            }
            if (message && message.indexOf('watch message handling failed') >= 0) {
                return 'Watch 事件处理失败，当前会话已关闭。请重新创建 Watch 订阅。';
            }
            if (data.success === false) {
                message = data.message || message || '业务操作失败';
                if (this.isLikelyLeaderRedirect(message)) {
                    var mappedConn = this.findConnectionByLeaderHint(message);
                    var mappedLabel = mappedConn ? this.displayConnectionLabel(mappedConn.nodeId) : message;
                    return '当前请求未在 Leader 上完成，Leader 提示为：' + mappedLabel + '。请切换到 Leader 连接，或在连接管理中配置包含全部节点的集群连接。';
                }
                return message;
            }
            return message || (response.code === 0 ? 'success' : 'failed');
        },
        isLikelyLeaderRedirect: function (message) {
            if (!message) {
                return false;
            }
            var text = String(message).trim();
            if (!text) {
                return false;
            }
            return !!this.findConnectionByLeaderHint(text) || /^node[\w.-]*$/i.test(text);
        },
        findConnectionByLeaderHint: function (hint) {
            if (!hint) {
                return null;
            }
            for (var i = 0; i < this.connections.length; i++) {
                var conn = this.connections[i];
                if (conn.nodeId === hint || this.connectionEndpoint(conn) === hint) {
                    return conn;
                }
            }
            return null;
        },
        // ==================== API Lifecycle ====================
        uiRefreshAll: function () {
            this.connectionLoadConnections();
            this.watchLoadSessions();
            this.leaseLoadSessions();
            this.clusterLoadNodeStatus();
            this.browserRefresh();
        },
        handleApi: function (promise, meta) {
            var _this = this;
            var start = new Date().getTime();
            meta = meta || {};
            return promise.then(function (res) {
                var cost = new Date().getTime() - start;
                var ok = _this.apiBusinessSuccess(res.data);
                if (!meta.silent) {
                    _this.result = res.data;
                    _this.resultSummary = _this.buildResultSummary(meta, res.data);
                    _this.addLog(meta.type || 'API', meta.target || '-', meta.nodeId || _this.currentNodeId || '-', ok ? '成功' : '失败', cost);
                    if (!ok) {
                        _this.notify('error', _this.normalizeBusinessMessage(meta, res.data));
                    }
                }
                return res.data;
            }).catch(function (error) {
                var cost = new Date().getTime() - start;
                var errorData = error.response ? error.response.data : {code: 500, message: error.message};
                if (typeof errorData === 'string') {
                    errorData = {code: 500, message: errorData};
                }
                if (!meta.silent) {
                    _this.result = errorData;
                    _this.resultSummary = _this.buildResultSummary(meta, errorData);
                    _this.addLog(meta.type || 'API', meta.target || '-', meta.nodeId || _this.currentNodeId || '-', '失败', cost);
                    _this.notify('error', _this.normalizeBusinessMessage(meta, errorData));
                }
                return errorData;
            });
        },
        buildResultSummary: function (meta, response) {
            response = response || {};
            return {
                success: this.apiBusinessSuccess(response),
                type: meta.type || 'API',
                target: meta.target || '',
                message: this.normalizeBusinessMessage(meta, response)
            };
        },
        recordLeaseOperationResult: function (meta, response) {
            this.leaseOperationResult = response || null;
            this.leaseOperationSummary = this.buildResultSummary(meta, response || {});
        },
        recordTxnOperationResult: function (meta, response) {
            this.txnOperationResult = response || null;
            this.txnOperationSummary = this.buildResultSummary(meta, response || {});
        },
        recordComparePrefixOperationResult: function (meta, response) {
            this.comparePrefixOperationResult = response || null;
            this.comparePrefixOperationSummary = this.buildResultSummary(meta, response || {});
        },
        recordCompactOperationResult: function (meta, response) {
            this.compactOperationResult = response || null;
            this.compactOperationSummary = this.buildResultSummary(meta, response || {});
        },
        handleLeaseApi: function (promise, meta, onResolved) {
            var _this = this;
            return this.handleApi(promise, meta).then(function (res) {
                _this.recordLeaseOperationResult(meta, res);
                if (_this.apiBusinessSuccess(res) && onResolved) {
                    onResolved.call(_this, res);
                }
                return res;
            });
        },
        addLog: function (type, target, nodeId, status, cost) {
            this.operationLogs.unshift({
                id: new Date().getTime() + '-' + Math.random(),
                time: this.formatTime(new Date()),
                type: type,
                target: target,
                nodeId: nodeId,
                status: status,
                cost: cost
            });
            if (this.operationLogs.length > 200) {
                this.operationLogs.pop();
            }
        },
        formatTime: function (date) {
            function pad(n) {
                return n < 10 ? '0' + n : n;
            }

            return date.getFullYear() + '-' + pad(date.getMonth() + 1) + '-' + pad(date.getDate()) + ' ' +
                pad(date.getHours()) + ':' + pad(date.getMinutes()) + ':' + pad(date.getSeconds());
        },
        // ==================== Connection + Cluster ====================
        connectionLoadConnections: function () {
            var _this = this;
            this.handleApi(apiClient.connections.list(), {type: 'CONNECTION_LIST', target: 'all', silent: true}).then(function (res) {
                _this.connections = res && res.data ? res.data : [];
                if (_this.currentNodeId && !_this.findConnectionByNodeId(_this.currentNodeId)) {
                    _this.currentNodeId = '';
                    _this.selectedKv = null;
                    _this.keyItems = [];
                }
                if (!_this.currentNodeId && _this.connections.length > 0) {
                    _this.currentNodeId = _this.connections[0].nodeId;
                }
                if (_this.currentNodeId && _this.activeView === 'browser') {
                    _this.browserRefresh(true);
                }
                if (_this.connections.length > 0) {
                    _this.clusterLoadNodeStatus(true);
                } else {
                    _this.nodeStatusMap = {};
                    _this.kvStateHashMap = {};
                }
                _this.watchLoadSessions();
                _this.leaseLoadSessions();
            });
        },
        clusterLoadNodeStatus: function (silent) {
            var _this = this;
            return this.handleApi(apiClient.cluster.listNodeStatusOnAllNodes(), {type: 'STATUS', target: 'all', silent: !!silent}).then(function (res) {
                _this.nodeStatusMap = {};
                var nodeStatusResponseList = res && res.data ? res.data : [];
                for (var i = 0; i < nodeStatusResponseList.length; i++) {
                    var nodeStatusResponse = nodeStatusResponseList[i] || {};
                    if (nodeStatusResponse.nodeId) {
                        Vue.set(_this.nodeStatusMap, nodeStatusResponse.nodeId, nodeStatusResponse);
                    }
                }
                _this.clusterLoadAllNodeHashes(true);
                return res;
            });
        },
        clusterLoadNodeHash: function (connection, silent) {
            var _this = this;
            if (!connection || !connection.host || !connection.port) {
                return Promise.resolve();
            }
            var nodeId = connection.nodeId || this.connectionEndpoint(connection);
            return this.handleApi(apiClient.cluster.computeKvStateHashOnEndpoint(connection.host, connection.port, requestBuilder.cluster.buildKvStateHash()), {
                type: 'HASH',
                target: nodeId,
                nodeId: nodeId,
                silent: !!silent
            }).then(function (res) {
                if (_this.apiBusinessSuccess(res) && res.data && typeof res.data.hash !== 'undefined') {
                    Vue.set(_this.kvStateHashMap, nodeId, res.data.hash);
                }
                return res;
            });
        },
        clusterLoadAllNodeHashes: function (silent) {
            for (var i = 0; i < this.connections.length; i++) {
                var connection = this.connections[i];
                if (connection && connection.nodeId) {
                    this.clusterLoadNodeHash(connection, !!silent);
                }
            }
        },
        connectionConnectNode: function () {
            var _this = this;
            var request = this.validateConnectForm();
            if (!request) {
                return;
            }
            this.handleApi(apiClient.connections.connect(request), {
                type: 'CONNECT',
                target: this.connectionEndpoint(request)
            }).then(function (res) {
                if (_this.apiBusinessSuccess(res) && res.data && res.data.nodeId) {
                    _this.currentNodeId = res.data.nodeId;
                    _this.connectionLoadConnections();
                    _this.notify('success', '连接成功：' + _this.connectionEndpoint(request));
                }
            });
        },
        connectionConfirmDisconnect: function (nodeId) {
            var _this = this;
            this.openModal('断开连接确认', '断开连接会停止该节点上的控制台操作，请确认是否继续。', nodeId, function () {
                _this.connectionDisconnectNode(nodeId);
            });
        },
        connectionDisconnectNode: function (nodeId) {
            var _this = this;
            var targetConnection = this.findConnectionByNodeId(nodeId);
            if (!targetConnection) {
                this.notify('warning', '目标连接不存在');
                return;
            }
            this.closeModal();
            this.handleApi(apiClient.connections.disconnect({
                host: targetConnection.host,
                port: targetConnection.port
            }), {
                type: 'DISCONNECT',
                target: this.connectionEndpoint(targetConnection),
                nodeId: nodeId
            }).then(function (res) {
                if (res.code === 0) {
                    if (_this.currentNodeId === nodeId) {
                        _this.currentNodeId = '';
                        _this.selectedKv = null;
                        _this.keyItems = [];
                    }
                    _this.connectionLoadConnections();
                }
            });
        },
        connectionUseNode: function (nodeId) {
            this.currentNodeId = nodeId;
            this.activeView = 'browser';
            this.browserRefresh();
        },
        browserRefresh: function (silent) {
            if (!this.currentNodeId) {
                this.keyItems = [];
                this.selectedKv = null;
                this.browserLoadError = '';
                return;
            }
            var endpointParams;
            try {
                endpointParams = this.selectedEndpointParams();
            } catch (ignored) {
                this.keyItems = [];
                this.selectedKv = null;
                return;
            }
            var _this = this;
            var prefix = this.trimString(this.browserPrefix);
            this.browserPrefix = prefix;
            this.browserLoadError = '';
            if (!silent) {
                this.browserLoading = true;
            }
            var rangeRequestBody = prefix ? requestBuilder.mvcc.buildRangeByPrefix(prefix) : requestBuilder.mvcc.buildRangeAll();
            var request = apiClient.mvcc.rangeOnEndpoint(endpointParams.host, endpointParams.port, rangeRequestBody);
            this.handleApi(request, {
                type: prefix ? 'PREFIX' : 'ALL_KEYS',
                target: prefix || '全部 Key',
                nodeId: endpointParams.nodeId,
                silent: !!silent
            }).then(function (res) {
                if (!silent) {
                    _this.browserLoading = false;
                }
                if (_this.apiBusinessSuccess(res) && res.data) {
                    _this.keyItems = res.data.items || [];
                    _this.browserLoadError = '';
                    _this.lastBrowserRefreshAt = _this.formatTime(new Date());
                    if (_this.selectedKv) {
                        var updated = _this.findItemByKey(_this.selectedKv.key);
                        _this.selectedKv = updated || null;
                    }
                    return;
                }
                _this.keyItems = [];
                _this.selectedKv = null;
                _this.browserLoadError = _this.normalizeBusinessMessage({type: prefix ? 'PREFIX' : 'ALL_KEYS'}, res);
            }).catch(function (error) {
                if (!silent) {
                    _this.browserLoading = false;
                }
                _this.keyItems = [];
                _this.selectedKv = null;
                _this.browserLoadError = error && error.message ? error.message : '数据目录加载失败';
            });
        },
        // ==================== Browser Tree + KV ====================
        buildTree: function (items) {
            return consoleUtils.buildTree(items || []);
        },
        onTreeNodeClick: function (data) {
            if (!data || data.type !== 'key') {
                return;
            }
            this.selectTreeNode(data);
        },
        selectTreeNode: function (node) {
            this.selectedKv = node.item;
            this.kvForm.key = node.key;
            this.kvForm.value = node.item.value;
            this.kvForm.leaseId = node.item.leaseId;
        },
        findItemByKey: function (key) {
            for (var i = 0; i < this.keyItems.length; i++) {
                if (this.keyItems[i].key === key) {
                    return this.keyItems[i];
                }
            }
            return null;
        },
        kvEditorOpen: function (mode, baseKey) {
            this.kvEditor.visible = true;
            this.kvEditor.mode = mode;
            if (mode === 'edit') {
                var item = this.findItemByKey(baseKey) || this.selectedKv || {};
                this.kvEditor.key = item.key || baseKey || '';
                this.kvEditor.value = item.value || '';
                this.kvEditor.leaseId = item.leaseId || null;
            } else {
                var prefix = baseKey || this.browserPrefix || '';
                this.kvEditor.key = prefix;
                this.kvEditor.value = '';
                this.kvEditor.leaseId = null;
            }
        },
        kvEditorSubmit: function () {
            var _this = this;
            var key = this.requireKey(this.kvEditor.key, this.kvEditor.mode === 'create' ? '新增 Key' : '编辑 Key');
            if (!key) {
                return;
            }
            this.kvEditor.key = key;
            this.handleApi(apiClient.mvcc.put(requestBuilder.mvcc.buildPut(
                key,
                this.kvEditor.value,
                this.kvEditor.leaseId)), {type: this.kvEditor.mode === 'create' ? 'PUT' : 'UPDATE', target: this.kvEditor.key, nodeId: this.currentNodeId}).then(function (res) {
                if (_this.apiBusinessSuccess(res)) {
                    _this.kvEditor.visible = false;
                    _this.browserRefresh(true);
                }
            });
        },
        putKv: function () {
            var _this = this;
            var key = this.requireKey(this.kvForm.key, 'Put');
            if (!key) {
                return;
            }
            this.kvForm.key = key;
            this.handleApi(apiClient.mvcc.put(requestBuilder.mvcc.buildPut(
                key,
                this.kvForm.value,
                this.kvForm.leaseId)), {type: 'PUT', target: key, nodeId: this.currentNodeId}).then(function (res) {
                if (_this.apiBusinessSuccess(res)) {
                    _this.browserRefresh(true);
                }
            });
        },
        getKv: function () {
            var _this = this;
            var key = this.requireKey(this.kvForm.key, 'Get');
            if (!key) {
                return;
            }
            var endpointParams;
            try {
                endpointParams = this.selectedEndpointParams();
            } catch (ignored) {
                return;
            }
            this.kvForm.key = key;
            this.handleApi(apiClient.mvcc.getOnEndpoint(endpointParams.host, endpointParams.port, requestBuilder.mvcc.buildGet(key)),
                {type: 'GET', target: key, nodeId: endpointParams.nodeId}).then(function (res) {
                if (res.code === 0 && res.data && res.data.value !== null && typeof res.data.value !== 'undefined') {
                    _this.selectedKv = {
                        key: key,
                        value: res.data.value,
                        createRevision: res.data.createRevision,
                        modRevision: res.data.modRevision,
                        version: res.data.version,
                        leaseId: res.data.leaseId
                    };
                }
            });
        },
        rangePrefix: function () {
            var _this = this;
            var prefix = this.requirePrefix(this.kvForm.key, 'Prefix 查询');
            if (!prefix) {
                return;
            }
            var endpointParams;
            try {
                endpointParams = this.selectedEndpointParams();
            } catch (ignored) {
                return;
            }
            this.kvForm.key = prefix;
            this.browserPrefix = prefix;
            this.handleApi(apiClient.mvcc.rangeOnEndpoint(endpointParams.host, endpointParams.port, requestBuilder.mvcc.buildRangeByPrefix(prefix)),
                {type: 'PREFIX', target: prefix, nodeId: endpointParams.nodeId}).then(function (res) {
                if (res.code === 0 && res.data) {
                    _this.keyItems = res.data.items || [];
                    _this.activeView = 'browser';
                }
            });
        },
        confirmDeleteKey: function (key) {
            key = this.requireKey(key, 'Delete');
            if (!key) {
                return;
            }
            var _this = this;
            this.openModal('删除 Key 确认', '删除后无法撤销，请确认要删除该 Key。', key, function () {
                _this.deleteKv(key);
            });
        },
        deleteKv: function (key) {
            var _this = this;
            this.closeModal();
            this.handleApi(apiClient.mvcc.deleteByKey(requestBuilder.mvcc.buildDelete(key)),
                {type: 'DELETE', target: key, nodeId: this.currentNodeId}).then(function (res) {
                if (_this.apiBusinessSuccess(res)) {
                    if (_this.selectedKv && _this.selectedKv.key === key) {
                        _this.selectedKv = null;
                    }
                    _this.browserRefresh(true);
                }
            });
        },
        confirmDeletePrefix: function (prefix) {
            prefix = this.requirePrefix(prefix, 'Delete Prefix');
            if (!prefix) {
                return;
            }
            var _this = this;
            this.openModal('删除 Prefix 确认', '该操作会删除 Prefix 下所有 Key，无法撤销。请重新输入 Prefix 后确认。', prefix, function () {
                _this.deletePrefix(prefix);
            });
        },
        deletePrefix: function (prefix) {
            var _this = this;
            this.closeModal();
            this.handleApi(apiClient.mvcc.deleteRange(requestBuilder.mvcc.buildDeleteRangeByPrefix(prefix)),
                {type: 'DELETE_PREFIX', target: prefix, nodeId: this.currentNodeId}).then(function (res) {
                if (_this.apiBusinessSuccess(res)) {
                    _this.selectedKv = null;
                    _this.browserRefresh(true);
                }
            });
        },
        // ==================== Txn ====================
        txnOperationNeedsValue: function (operationType) {
            return operationType === 'PUT';
        },
        txnOperationSupportsPrefix: function (operationType) {
            return operationType === 'RANGE' || operationType === 'DELETE_RANGE';
        },
        txnFieldLabel: function (fieldType) {
            if (fieldType === 'VALUE') {
                return 'VALUE';
            }
            if (fieldType === 'VERSION') {
                return 'VERSION';
            }
            if (fieldType === 'CREATE_REVISION') {
                return 'CREATE_REVISION';
            }
            if (fieldType === 'MOD_REVISION') {
                return 'MOD_REVISION';
            }
            return fieldType || '-';
        },
        txnOperatorLabel: function (operatorType) {
            if (operatorType === 'EQUAL') {
                return '=';
            }
            if (operatorType === 'NOT_EQUAL') {
                return '!=';
            }
            if (operatorType === 'GREATER') {
                return '>';
            }
            if (operatorType === 'LESS') {
                return '<';
            }
            return operatorType || '-';
        },
        txnOperationTypeLabel: function (operationType) {
            return operationType || '-';
        },
        txnBranchReadableSummary: function (branchType) {
            var isSuccessBranch = branchType === 'success';
            var operationType = isSuccessBranch ? this.txnForm.successOperationType : this.txnForm.failureOperationType;
            var key = this.trimString(isSuccessBranch ? this.txnForm.successKey : this.txnForm.failureKey);
            var value = isSuccessBranch ? this.txnForm.successValue : this.txnForm.failureValue;
            var leaseId = isSuccessBranch ? this.txnForm.successLeaseId : this.txnForm.failureLeaseId;
            var prefixMatch = isSuccessBranch ? this.txnForm.successPrefixMatch : this.txnForm.failurePrefixMatch;
            if (!operationType || !key) {
                return '尚未配置';
            }
            if (operationType === 'PUT') {
                return 'PUT ' + key + ' = "' + (value || '') + '"' + (Number(leaseId) > 0 ? (' (leaseId=' + leaseId + ')') : '');
            }
            if (operationType === 'DELETE') {
                return 'DELETE ' + key;
            }
            if (operationType === 'GET') {
                return 'GET ' + key;
            }
            if (operationType === 'RANGE') {
                return (prefixMatch ? 'RANGE PREFIX ' : 'RANGE ') + key;
            }
            if (operationType === 'DELETE_RANGE') {
                return (prefixMatch ? 'DELETE PREFIX ' : 'DELETE_RANGE ') + key;
            }
            return operationType + ' ' + key;
        },
        txnUseSelectedKvAsCompareKey: function () {
            if (!this.selectedKv || !this.selectedKv.key) {
                this.notify('warning', '请先在数据浏览中选中一个 Key');
                return;
            }
            this.txnForm.compareKey = this.selectedKv.key;
            this.notify('success', '已把当前选中 Key 带入 Compare');
        },
        txnUseSelectedKvAsBranchKey: function (branchType) {
            if (!this.selectedKv || !this.selectedKv.key) {
                this.notify('warning', '请先在数据浏览中选中一个 Key');
                return;
            }
            if (branchType === 'success') {
                this.txnForm.successKey = this.selectedKv.key;
            } else {
                this.txnForm.failureKey = this.selectedKv.key;
            }
            this.notify('success', '已把当前选中 Key 带入 ' + (branchType === 'success' ? 'Then' : 'Else') + ' 分支');
        },
        txnUseCurrentLeaseIdInBranch: function (branchType) {
            if (!this.hasPositiveLeaseId()) {
                this.notify('warning', 'Lease 页当前没有可用的 LeaseId');
                return;
            }
            if (branchType === 'success') {
                this.txnForm.successLeaseId = this.leaseForm.leaseId;
            } else {
                this.txnForm.failureLeaseId = this.leaseForm.leaseId;
            }
            this.notify('success', '已把当前 LeaseId 带入 ' + (branchType === 'success' ? 'Then' : 'Else') + ' 分支');
        },
        validateTxnBranchOperation: function (operationType, key, branchLabel) {
            if (!operationType) {
                this.notify('warning', branchLabel + ' 分支未选择操作类型');
                return false;
            }
            if (!this.trimString(key)) {
                this.notify('warning', branchLabel + ' 分支缺少 Key');
                return false;
            }
            if (operationType === 'PUT' && !this.trimString(branchLabel === 'Then' ? this.txnForm.successValue : this.txnForm.failureValue)) {
                this.notify('warning', branchLabel + ' 分支是 PUT 时必须填写 Value');
                return false;
            }
            return true;
        },
        txnResultData: function () {
            return this.txnOperationResult && this.txnOperationResult.data ? this.txnOperationResult.data : null;
        },
        txnResponseDataSummary: function (operationResponse) {
            operationResponse = operationResponse || {};
            var operationType = operationResponse.operationType;
            var data = operationResponse.data || {};
            if (operationType === 'PUT') {
                return 'revision=' + this.safeValue(data.revision);
            }
            if (operationType === 'DELETE') {
                return 'deleted=' + this.safeValue(data.deletedCount) + ', revision=' + this.safeValue(data.revision);
            }
            if (operationType === 'GET') {
                var getValue = typeof data.value === 'undefined' || data.value === null || data.value === '' ? '(empty)' : data.value;
                return 'value=' + getValue + ', revision=' + this.safeValue(data.revision);
            }
            if (operationType === 'RANGE') {
                return 'count=' + this.safeValue(data.count) + ', revision=' + this.safeValue(data.revision);
            }
            if (operationType === 'DELETE_RANGE') {
                return 'deleted=' + this.safeValue(data.deletedCount) + ', revision=' + this.safeValue(data.revision);
            }
            return JSON.stringify(data);
        },
        txnExecute: function () {
            var compareKey = this.requireKey(this.txnForm.compareKey, 'Txn Compare');
            if (!compareKey) {
                return;
            }
            this.txnForm.compareKey = compareKey;
            if (this.txnForm.compareFieldType !== 'VALUE') {
                var compareLongValue = this.requireNonNegativeNumber(this.txnForm.compareLongValue, 'Txn Compare 数值');
                if (compareLongValue === null) {
                    return;
                }
                this.txnForm.compareLongValue = compareLongValue;
            }
            if (!this.validateTxnBranchOperation(this.txnForm.successOperationType, this.txnForm.successKey, 'Then')) {
                return;
            }
            if (!this.validateTxnBranchOperation(this.txnForm.failureOperationType, this.txnForm.failureKey, 'Else')) {
                return;
            }
            this.txnForm.successKey = this.trimString(this.txnForm.successKey);
            this.txnForm.failureKey = this.trimString(this.txnForm.failureKey);
            var _this = this;
            this.handleApi(apiClient.txn.execute(requestBuilder.txn.buildExecute(this.txnForm)), {
                type: 'TXN_EXECUTE',
                target: compareKey,
                nodeId: this.currentNodeId
            }).then(function (res) {
                _this.recordTxnOperationResult({
                    type: 'TXN_EXECUTE',
                    target: compareKey,
                    nodeId: _this.currentNodeId
                }, res);
            });
        },
        // ==================== Watch ====================
        watchPrepareFromTree: function (prefix) {
            this.watchForm.key = prefix || '/';
            this.watchForm.prefix = true;
            this.activeView = 'operations';
            this.activeOperationTab = 'watch';
        },
        watchStart: function () {
            var _this = this;
            var key = this.watchForm.prefix ? this.requirePrefix(this.watchForm.key, 'Watch Prefix') : this.requireKey(this.watchForm.key, 'Watch');
            if (!key) {
                return;
            }
            var endpointParams;
            try {
                endpointParams = this.selectedEndpointParams();
            } catch (ignored) {
                return;
            }
            this.watchForm.key = key;
            this.handleApi(apiClient.watch.startOnEndpoint(
                    endpointParams.host,
                    endpointParams.port,
                    requestBuilder.watch.buildSubscribe(key, this.watchForm.prefix)),
                {type: 'WATCH_START', target: key, nodeId: endpointParams.nodeId}).then(function () {
                _this.watchLoadSessions();
            });
        },
        watchLoadSessions: function () {
            var _this = this;
            this.handleApi(apiClient.watch.list(), {type: 'WATCH_LIST', target: 'all', silent: true}).then(function (res) {
                _this.watchSessions = res && res.data ? res.data : [];
                _this.rebuildWatchSessionState();
            });
        },
        watchCancelById: function (watchId) {
            var _this = this;
            this.handleApi(apiClient.watch.cancel(watchId), {
                type: 'WATCH_STOP',
                target: watchId,
                nodeId: this.currentNodeId
            }).then(function () {
                _this.watchLoadSessions();
            });
        },
        watchSelectById: function (watchId) {
            this.selectedWatchId = String(watchId || '');
        },
        watchSessionTagType: function (watchSession) {
            return watchSession && watchSession.active ? 'success' : 'info';
        },
        watchSessionEventCount: function (watchId) {
            var watchEventList = this.watchEventListByWatchId[watchId] || [];
            return watchEventList.length;
        },
        rebuildWatchSessionState: function () {
            var watchSessionStateByWatchId = {};
            var activeWatchIdSet = {};
            for (var index = 0; index < this.watchSessions.length; index++) {
                var watchSession = this.watchSessions[index];
                if (!watchSession || !watchSession.watchId) {
                    continue;
                }
                watchSessionStateByWatchId[watchSession.watchId] = watchSession;
                activeWatchIdSet[String(watchSession.watchId)] = true;
            }
            var watchEventListByWatchId = this.watchEventListByWatchId || {};
            var watchIdList = Object.keys(watchEventListByWatchId);
            for (var i = 0; i < watchIdList.length; i++) {
                if (!activeWatchIdSet[watchIdList[i]]) {
                    delete watchEventListByWatchId[watchIdList[i]];
                }
            }
            this.watchSessionStateByWatchId = watchSessionStateByWatchId;
            this.watchEventListByWatchId = watchEventListByWatchId;
            if (this.selectedWatchId && !watchSessionStateByWatchId[this.selectedWatchId]) {
                this.selectedWatchId = '';
            }
            if (!this.selectedWatchId && this.watchSessionListSorted.length > 0) {
                this.selectedWatchId = String(this.watchSessionListSorted[0].watchId);
            }
        },
        webSocketAppendWatchEventByWatchId: function (watchId, event, nodeId) {
            if (!watchId) {
                return;
            }
            var endpoint = this.displayEndpointByNodeId(nodeId);
            var watchEventListByWatchId = this.watchEventListByWatchId || {};
            var watchEventList = watchEventListByWatchId[watchId];
            if (!watchEventList) {
                watchEventList = [];
                Vue.set(watchEventListByWatchId, watchId, watchEventList);
            }
            watchEventList.unshift({
                id: new Date().getTime() + '-' + Math.random(),
                at: new Date().getTime(),
                nodeId: nodeId || '',
                endpoint: endpoint && endpoint !== '-' ? endpoint : '',
                text: JSON.stringify(event)
            });
            if (watchEventList.length > 200) {
                watchEventList.pop();
            }
            var totalEventCount = 0;
            var watchIdList = Object.keys(watchEventListByWatchId);
            for (var index = 0; index < watchIdList.length; index++) {
                totalEventCount += (watchEventListByWatchId[watchIdList[index]] || []).length;
            }
            if (totalEventCount > 2000) {
                this.trimWatchEventsByTotalLimit(2000);
            }
            this.watchEventListByWatchId = watchEventListByWatchId;
        },
        trimWatchEventsByTotalLimit: function (maxTotalCount) {
            var watchIdList = Object.keys(this.watchEventListByWatchId || {});
            var overflowCount = 0;
            var currentTotalCount = 0;
            for (var index = 0; index < watchIdList.length; index++) {
                currentTotalCount += (this.watchEventListByWatchId[watchIdList[index]] || []).length;
            }
            if (currentTotalCount <= maxTotalCount) {
                return;
            }
            overflowCount = currentTotalCount - maxTotalCount;
            while (overflowCount > 0) {
                var removed = false;
                for (var i = 0; i < watchIdList.length && overflowCount > 0; i++) {
                    var watchEventList = this.watchEventListByWatchId[watchIdList[i]] || [];
                    if (!watchEventList.length) {
                        continue;
                    }
                    watchEventList.pop();
                    overflowCount--;
                    removed = true;
                }
                if (!removed) {
                    break;
                }
            }
        },
        webSocketClearWatchEvents: function () {
            this.watchEventListByWatchId = {};
        },
        // ==================== Lease ====================
        resolveLeaseTtlSeconds: function () {
            var rawValue = this.leaseForm.ttlSeconds;
            if (rawValue === null || typeof rawValue === 'undefined' || String(rawValue).trim() === '') {
                this.leaseForm.ttlSeconds = 10;
                return 10;
            }
            var ttl = this.requirePositiveNumber(rawValue, 'TTL 秒');
            if (!ttl) {
                return null;
            }
            this.leaseForm.ttlSeconds = ttl;
            return ttl;
        },
        grantLease: function () {
            var ttl = this.resolveLeaseTtlSeconds();
            if (!ttl) {
                return;
            }
            this.handleLeaseApi(apiClient.lease.grant(requestBuilder.lease.buildGrant(this.leaseForm.leaseId, ttl)),
                {type: 'LEASE_GRANT', target: this.leaseForm.leaseId || 'auto', nodeId: this.currentNodeId},
                this.syncLeaseFormFromOperationLease);
        },
        ttlLease: function () {
            var leaseId = this.requirePositiveNumber(this.leaseForm.leaseId, 'LeaseId');
            if (!leaseId) {
                return;
            }
            this.leaseForm.leaseId = leaseId;
            this.handleLeaseApi(apiClient.lease.ttl(requestBuilder.lease.buildTtl(this.leaseForm.leaseId)),
                {type: 'LEASE_TTL', target: this.leaseForm.leaseId, nodeId: this.currentNodeId},
                this.syncLeaseFormFromOperationLease);
        },
        listLease: function () {
            this.handleLeaseApi(apiClient.lease.list(requestBuilder.lease.buildList()), {
                type: 'LEASE_LIST',
                target: 'all',
                nodeId: this.currentNodeId
            });
        },
        confirmRevokeLease: function () {
            var leaseId = this.requirePositiveNumber(this.leaseForm.leaseId, 'LeaseId');
            if (!leaseId) {
                return;
            }
            this.leaseForm.leaseId = leaseId;
            var _this = this;
            var id = String(this.leaseForm.leaseId);
            this.openModal('Revoke Lease 确认', '撤销 Lease 会删除绑定到该 Lease 的 Key，请确认是否继续。', id, function () {
                _this.revokeLease();
            });
        },
        revokeLease: function () {
            var _this = this;
            this.closeModal();
            this.handleLeaseApi(apiClient.lease.revoke(requestBuilder.lease.buildRevoke(this.leaseForm.leaseId)),
                {type: 'LEASE_REVOKE', target: this.leaseForm.leaseId, nodeId: this.currentNodeId},
                function () {
                    _this.leaseLoadSessions();
                });
        },
        kvUseCurrentLeaseId: function () {
            var leaseId = this.requirePositiveNumber(this.leaseForm.leaseId, 'LeaseId');
            if (!leaseId) {
                return;
            }
            this.kvForm.leaseId = leaseId;
            if (this.selectedKv) {
                this.kvForm.key = this.selectedKv.key || this.kvForm.key;
                this.kvForm.value = this.selectedKv.value || this.kvForm.value;
            }
            this.notify('success', '已把 LeaseId 带入 KV 表单。执行 Put 即可把当前 Key 绑定到该 Lease。');
        },
        resolveLeaseBindingTarget: function (leaseView) {
            if (leaseView && Number(leaseView.leaseId || 0) > 0) {
                return leaseView;
            }
            if (this.leaseInnerTab === 'session') {
                return this.selectedLeaseSession;
            }
            if (this.leaseOperationPrimaryLease && Number(this.leaseOperationPrimaryLease.leaseId || 0) > 0) {
                return this.leaseOperationPrimaryLease;
            }
            if (this.hasPositiveLeaseId()) {
                return {
                    leaseId: this.leaseForm.leaseId,
                    ttlSeconds: this.leaseForm.ttlSeconds
                };
            }
            return this.selectedLeaseSession;
        },
        leaseGoToKvBinding: function (leaseView) {
            var targetLeaseView = this.resolveLeaseBindingTarget(leaseView);
            var leaseId = Number(targetLeaseView && targetLeaseView.leaseId ? targetLeaseView.leaseId : this.leaseForm.leaseId);
            if (!leaseId) {
                this.notify('warning', '没有可用于绑定的 LeaseId，请先 Grant、TTL 查询，或选择一个 LeaseHandle 会话。');
                return;
            }
            this.leaseUseLeaseView({leaseId: leaseId, ttlSeconds: this.leaseForm.ttlSeconds});
            this.activeOperationTab = 'kv';
            this.kvUseCurrentLeaseId();
        },
        leaseLoadSessions: function () {
            var _this = this;
            this.handleApi(apiClient.lease.listSessions(), {
                type: 'LEASE_SESSION_LIST',
                target: 'all',
                silent: true
            }).then(function (res) {
                _this.leaseSessions = res && res.data ? res.data : [];
                _this.rebuildLeaseSessionState(_this.leaseSessions);
            });
        },
        leaseSessionStart: function () {
            var _this = this;
            var leaseId = this.requirePositiveNumber(this.leaseForm.leaseId, 'LeaseId');
            if (!leaseId) {
                return;
            }
            this.leaseInnerTab = 'session';
            this.leaseForm.leaseId = leaseId;
            this.handleApi(apiClient.lease.startSession(requestBuilder.lease.buildSessionStart(leaseId)), {
                type: 'LEASE_SESSION_START',
                target: leaseId,
                nodeId: this.currentNodeId
            }).then(function (res) {
                if (_this.apiBusinessSuccess(res) && res.data) {
                    _this.selectedLeaseSessionId = Number(res.data.leaseId || 0);
                }
                _this.leaseLoadSessions();
            });
        },
        leaseSessionGrantAndStart: function () {
            var _this = this;
            var ttlSeconds = this.resolveLeaseTtlSeconds();
            if (!ttlSeconds) {
                return;
            }
            this.leaseInnerTab = 'session';
            this.handleApi(apiClient.lease.grantAndStartSession(requestBuilder.lease.buildSessionGrantStart(this.leaseForm.leaseId, ttlSeconds)), {
                type: 'LEASE_SESSION_GRANT_START',
                target: this.leaseForm.leaseId || 'auto',
                nodeId: this.currentNodeId
            }).then(function (res) {
                if (_this.apiBusinessSuccess(res) && res.data) {
                    _this.selectedLeaseSessionId = Number(res.data.leaseId || 0);
                    _this.leaseUseSession(res.data);
                }
                _this.leaseLoadSessions();
            });
        },
        leaseSessionStop: function (leaseId) {
            var _this = this;
            this.handleApi(apiClient.lease.stopSession(leaseId), {
                type: 'LEASE_SESSION_STOP',
                target: leaseId,
                nodeId: this.currentNodeId
            }).then(function () {
                _this.markLeaseSessionClosed(leaseId);
                _this.leaseLoadSessions();
            });
        },
        leaseSelectSession: function (leaseSession) {
            var leaseId = leaseSession && typeof leaseSession === 'object'
                ? leaseSession.leaseId
                : leaseSession;
            this.selectedLeaseSessionId = Number(leaseId || 0);
        },
        leaseUseLeaseView: function (leaseView) {
            if (!leaseView) {
                return;
            }
            this.leaseForm.leaseId = Number(leaseView.leaseId || 0) || null;
            if (Number(leaseView.ttlSeconds || 0) > 0) {
                this.leaseForm.ttlSeconds = Number(leaseView.ttlSeconds);
            }
        },
        leaseUseSession: function (leaseSession) {
            if (!leaseSession) {
                return;
            }
            this.leaseInnerTab = 'session';
            this.selectedLeaseSessionId = Number(leaseSession.leaseId || 0);
            this.leaseUseLeaseView(leaseSession);
        },
        leaseCopyKeys: function (keys) {
            var keyText = (keys || []).join(', ');
            this.copyText(keyText);
            this.notify('success', '已复制 Lease 关联 Key 列表。');
        },
        syncLeaseFormFromOperationLease: function (response) {
            var leaseView = response && response.data ? response.data.lease : null;
            if (leaseView) {
                this.leaseUseLeaseView(leaseView);
            }
        },
        leaseSessionEventCount: function (leaseId) {
            return (this.leaseEventListByLeaseId[String(leaseId)] || []).length;
        },
        leaseSessionTagType: function (leaseSession) {
            if (!leaseSession) {
                return 'info';
            }
            return leaseSession.active ? 'success' : 'info';
        },
        rebuildLeaseSessionState: function (activeLeaseSessionList) {
            var leaseSessionStateByLeaseId = {};
            var currentStateMap = this.leaseSessionStateByLeaseId || {};
            var activeLeaseIdSet = {};
            var currentLeaseIdList = Object.keys(currentStateMap);
            for (var index = 0; index < currentLeaseIdList.length; index++) {
                leaseSessionStateByLeaseId[currentLeaseIdList[index]] = Object.assign({}, currentStateMap[currentLeaseIdList[index]]);
            }
            for (var i = 0; i < (activeLeaseSessionList || []).length; i++) {
                var leaseSession = activeLeaseSessionList[i];
                if (!leaseSession || !leaseSession.leaseId) {
                    continue;
                }
                var leaseIdKey = String(leaseSession.leaseId);
                leaseSessionStateByLeaseId[leaseIdKey] = Object.assign({}, leaseSessionStateByLeaseId[leaseIdKey] || {}, leaseSession, {active: true});
                activeLeaseIdSet[leaseIdKey] = true;
            }
            var leaseIdList = Object.keys(leaseSessionStateByLeaseId);
            for (var j = 0; j < leaseIdList.length; j++) {
                if (!activeLeaseIdSet[leaseIdList[j]]) {
                    leaseSessionStateByLeaseId[leaseIdList[j]].active = false;
                }
            }
            this.leaseSessionStateByLeaseId = leaseSessionStateByLeaseId;
            this.ensureSelectedLeaseSession();
        },
        webSocketAppendLeaseSessionEvent: function (leaseId, messageType, leaseSession) {
            if (!leaseId) {
                return;
            }
            var leaseEventListByLeaseId = this.leaseEventListByLeaseId || {};
            var leaseIdKey = String(leaseId);
            var leaseEventList = leaseEventListByLeaseId[leaseIdKey];
            if (!leaseEventList) {
                leaseEventList = [];
                Vue.set(leaseEventListByLeaseId, leaseIdKey, leaseEventList);
            }
            leaseEventList.unshift({
                id: new Date().getTime() + '-' + Math.random(),
                at: new Date().getTime(),
                atText: this.formatTime(new Date()),
                text: messageType + ' ' + JSON.stringify(leaseSession || {})
            });
            if (leaseEventList.length > 120) {
                leaseEventList.pop();
            }
            this.leaseEventListByLeaseId = leaseEventListByLeaseId;
        },
        markLeaseSessionClosed: function (leaseId) {
            var leaseIdKey = String(leaseId || '');
            if (!leaseIdKey || !this.leaseSessionStateByLeaseId[leaseIdKey]) {
                return;
            }
            this.leaseSessionStateByLeaseId = Object.assign({}, this.leaseSessionStateByLeaseId, (function (currentSession) {
                var patch = {};
                patch[leaseIdKey] = Object.assign({}, currentSession, {active: false});
                return patch;
            })(this.leaseSessionStateByLeaseId[leaseIdKey]));
            this.ensureSelectedLeaseSession();
        },
        ensureSelectedLeaseSession: function () {
            if (this.selectedLeaseSessionId && this.selectedLeaseSession) {
                return;
            }
            if (this.leaseSessionListSorted.length > 0) {
                this.selectedLeaseSessionId = Number(this.leaseSessionListSorted[0].leaseId || 0);
            } else {
                this.selectedLeaseSessionId = 0;
            }
        },
        // ==================== Compact + Consistency ====================
        comparePrefix: function () {
            var prefix = this.requirePrefix(this.comparePrefixValue, '多节点 Prefix 对比');
            if (!prefix) {
                return;
            }
            this.comparePrefixValue = prefix;
            var _this = this;
            this.handleApi(apiClient.cluster.rangeOnAllNodes(requestBuilder.cluster.buildRangeByPrefix(prefix)),
                {type: 'COMPARE_PREFIX', target: prefix, nodeId: 'all'}).then(function (res) {
                _this.recordComparePrefixOperationResult({
                    type: 'COMPARE_PREFIX',
                    target: prefix,
                    nodeId: 'all'
                }, res);
            });
        },
        confirmCompact: function () {
            if (!this.compactRevision || this.compactRevision < 1) {
                this.notify('warning', '请输入大于 0 的 compact revision');
                return;
            }
            var _this = this;
            var revision = String(this.compactRevision);
            this.openModal('Compact 确认', 'Compact 会压缩指定 revision 之前的历史版本，可能影响历史查询和 Watch 行为。', revision, function () {
                _this.compact();
            });
        },
        compact: function () {
            var _this = this;
            this.closeModal();
            this.handleApi(apiClient.compact.execute(requestBuilder.compact.buildCompact(this.compactRevision)),
                {type: 'COMPACT', target: this.compactRevision, nodeId: this.currentNodeId}).then(function (res) {
                _this.recordCompactOperationResult({
                    type: 'COMPACT',
                    target: _this.compactRevision,
                    nodeId: _this.currentNodeId
                }, res);
                _this.clusterLoadNodeStatus(true);
            });
        },
        // ==================== Browser Auto Refresh ====================
        startBrowserAutoRefresh: function () {
            var _this = this;
            if (this.browserAutoRefreshTimer) {
                clearInterval(this.browserAutoRefreshTimer);
            }
            this.browserAutoRefreshTimer = setInterval(function () {
                if (_this.browserAutoRefreshEnabled && _this.activeView === 'browser' && _this.currentNodeId) {
                    _this.browserRefresh(true);
                }
            }, 2000);
        },
        scheduleBrowserRealtimeRefresh: function () {
            var _this = this;
            if (!this.browserAutoRefreshEnabled || this.activeView !== 'browser' || !this.currentNodeId) {
                return;
            }
            if (this.browserRealtimeRefreshTimer) {
                clearTimeout(this.browserRealtimeRefreshTimer);
            }
            this.browserRealtimeRefreshTimer = setTimeout(function () {
                _this.browserRealtimeRefreshTimer = null;
                if (_this.browserAutoRefreshEnabled && _this.activeView === 'browser' && _this.currentNodeId) {
                    _this.browserRefresh(true);
                }
            }, 150);
        },
        isKvChangeRelevant: function (nodeId, payload) {
            if (!this.currentNodeId || !payload) {
                return false;
            }
            var eventNodeId = nodeId || (payload.watchSessionResponse && payload.watchSessionResponse.nodeId);
            if (eventNodeId && eventNodeId !== this.currentNodeId) {
                return false;
            }
            var prefix = this.trimString(this.browserPrefix);
            if (!prefix) {
                return true;
            }
            if (payload.prefix) {
                var changedPrefix = String(payload.prefix);
                if (changedPrefix.indexOf(prefix) === 0 || prefix.indexOf(changedPrefix) === 0) {
                    return true;
                }
            }
            var keys = [];
            if (payload.key) {
                keys.push(payload.key);
            }
            if (payload.keyList && payload.keyList.length) {
                for (var k = 0; k < payload.keyList.length; k++) {
                    if (payload.keyList[k]) {
                        keys.push(payload.keyList[k]);
                    }
                }
            }
            if (payload.watchEventViewList && payload.watchEventViewList.length) {
                for (var i = 0; i < payload.watchEventViewList.length; i++) {
                    var watchEventView = payload.watchEventViewList[i] || {};
                    var watchEventValueView = watchEventView.keyValueView || {};
                    if (watchEventValueView.key) {
                        keys.push(watchEventValueView.key);
                    }
                }
            }
            if (payload.watchNotification && payload.watchNotification.events) {
                for (var m = 0; m < payload.watchNotification.events.length; m++) {
                    var watchNotificationEvent = payload.watchNotification.events[m] || {};
                    var watchNotificationValueView = watchNotificationEvent.keyValueView || {};
                    if (watchNotificationValueView.key) {
                        keys.push(watchNotificationValueView.key);
                    }
                }
            }
            if (!keys.length) {
                return true;
            }
            for (var j = 0; j < keys.length; j++) {
                if (String(keys[j]).indexOf(prefix) === 0) {
                    return true;
                }
            }
            return false;
        },
        // ==================== WebSocket Runtime ====================
        webSocketConnect: function () {
            var _this = this;
            if (this.pageDestroyed) {
                return;
            }
            if (this.webSocketClient && (this.webSocketClient.readyState === WebSocket.OPEN || this.webSocketClient.readyState === WebSocket.CONNECTING)) {
                return;
            }
            var protocol = window.location.protocol === 'https:' ? 'wss://' : 'ws://';
            var socket = new WebSocket(protocol + window.location.host + '/ws/console');
            this.webSocketClient = socket;
            socket.onopen = function () {
                _this.webSocketConnected = true;
                _this.webSocketStats.lastConnectedAt = _this.formatTime(new Date());
                if (_this.webSocketReconnectTimer) {
                    clearTimeout(_this.webSocketReconnectTimer);
                    _this.webSocketReconnectTimer = null;
                }
            };
            socket.onclose = function () {
                _this.webSocketConnected = false;
                _this.webSocketClient = null;
                if (_this.pageDestroyed) {
                    return;
                }
                if (_this.webSocketReconnectTimer) {
                    clearTimeout(_this.webSocketReconnectTimer);
                }
                _this.webSocketReconnectTimer = setTimeout(function () {
                    _this.webSocketReconnectTimer = null;
                    _this.webSocketStats.reconnectCount += 1;
                    _this.webSocketConnect();
                }, 2000);
            };
            socket.onerror = function () {
                _this.webSocketConnected = false;
                _this.webSocketStats.lastErrorMessage = 'websocket error';
            };
            socket.onmessage = function (event) {
                var parsedMessage = consoleUtils.safeParseJson(event.data);
                if (!parsedMessage.ok) {
                    _this.webSocketStats.parseErrorCount += 1;
                    _this.webSocketStats.lastErrorMessage = String(parsedMessage.error && parsedMessage.error.message ? parsedMessage.error.message : 'json parse error');
                    console.error('invalid websocket payload', parsedMessage.error, event.data);
                    return;
                }
                _this.webSocketStats.messageCount += 1;
                _this.webSocketStats.lastMessageAt = _this.formatTime(new Date());
                var webSocketMessage = parsedMessage.data;
                var messageType = webSocketMessage.messageType;
                var payload = webSocketMessage.payload;
                var messageNodeId = webSocketMessage.nodeId;
                if (messageType === 'NODE_STATUS') {
                    var nodeStatus = _this.extractNodeStatusFromPayload(payload);
                    if (nodeStatus && messageNodeId) {
                        Vue.set(_this.nodeStatusMap, messageNodeId, nodeStatus);
                        var targetConnection = _this.findConnectionByNodeId(messageNodeId);
                        if (targetConnection) {
                            _this.clusterLoadNodeHash(targetConnection, true);
                        }
                    }
                } else if (messageType === 'CONNECTIONS') {
                    _this.connections = payload || [];
                } else if (messageType === 'WATCH_EVENT') {
                    _this.handleWatchWebSocketMessage(messageType, payload, messageNodeId);
                    if (_this.isKvChangeRelevant(messageNodeId, payload)) {
                        _this.scheduleBrowserRealtimeRefresh();
                    }
                } else if (messageType === 'KV_CHANGED') {
                    if (_this.isKvChangeRelevant(messageNodeId, payload)) {
                        _this.scheduleBrowserRealtimeRefresh();
                    }
                } else if (messageType === 'WATCH_CREATED' || messageType === 'WATCH_CANCELED' || messageType === 'WATCH_ERROR') {
                    _this.watchLoadSessions();
                    _this.handleWatchWebSocketMessage(messageType, payload, messageNodeId);
                } else if (messageType === 'LEASE_SESSION_CREATED'
                    || messageType === 'LEASE_SESSION_UPDATED'
                    || messageType === 'LEASE_SESSION_CLOSED'
                    || messageType === 'LEASE_SESSION_ERROR') {
                    _this.handleLeaseSessionWebSocketMessage(messageType, payload);
                }
            };
        },
        handleWatchWebSocketMessage: function (messageType, payload, nodeId) {
            if (messageType === 'WATCH_EVENT') {
                var watchSessionResponse = payload && payload.watchSessionResponse ? payload.watchSessionResponse : {};
                var watchId = Number(watchSessionResponse.watchId || 0);
                if (!watchId) {
                    return;
                }
                this.webSocketAppendWatchEventByWatchId(watchId, payload, nodeId);
                return;
            }
            if (messageType === 'WATCH_CREATED' || messageType === 'WATCH_CANCELED') {
                var watchSession = payload && payload.watchSessionResponse ? payload.watchSessionResponse : {};
                var changedWatchId = Number(watchSession.watchId || 0);
                if (!changedWatchId) {
                    return;
                }
                this.webSocketAppendWatchEventByWatchId(changedWatchId, {
                    eventType: messageType,
                    watchSessionResponse: watchSession
                }, nodeId);
                return;
            }
            if (messageType === 'WATCH_ERROR') {
                var watchErrorSession = payload && payload.watchSessionResponse ? payload.watchSessionResponse : {};
                var watchErrorWatchId = Number(watchErrorSession.watchId || this.selectedWatchId || 0);
                if (watchErrorWatchId) {
                    this.webSocketAppendWatchEventByWatchId(watchErrorWatchId, {
                        eventType: 'WATCH_ERROR',
                        watchSessionResponse: watchErrorSession
                    }, nodeId);
                }
            }
        },
        handleLeaseSessionWebSocketMessage: function (messageType, payload) {
            var leaseSession = payload || {};
            var leaseId = Number(leaseSession.leaseId || 0);
            if (!leaseId) {
                return;
            }
            this.webSocketAppendLeaseSessionEvent(leaseId, messageType, leaseSession);
            if (messageType === 'LEASE_SESSION_CLOSED') {
                leaseSession.active = false;
            }
            this.upsertLeaseSession(leaseSession);
        },
        upsertLeaseSession: function (leaseSession) {
            if (!leaseSession || !leaseSession.leaseId) {
                return;
            }
            var leaseId = Number(leaseSession.leaseId);
            var leaseIdKey = String(leaseId);
            var nextStateMap = Object.assign({}, this.leaseSessionStateByLeaseId);
            nextStateMap[leaseIdKey] = Object.assign({}, nextStateMap[leaseIdKey] || {}, leaseSession);
            this.leaseSessionStateByLeaseId = nextStateMap;
            this.ensureSelectedLeaseSession();
        },
        // ==================== UI Navigation ====================
        uiOpenConnectionsView: function () {
            this.activeView = 'connections';
        },
        uiOpenBrowserView: function () {
            this.activeView = 'browser';
            this.browserRefresh(true);
        },
        uiOpenOperationsView: function () {
            this.activeView = 'operations';
        },
        // ==================== Node Status Rendering ====================
        extractNodeStatusFromPayload: function (payload) {
            if (!payload) {
                return null;
            }
            if (payload.nodeStatusResponse) {
                return payload.nodeStatusResponse;
            }
            if (payload.role || payload.currentTerm || payload.currentRevision || payload.nodeId) {
                return payload;
            }
            return null;
        },
        nodeMetric: function (nodeId, name) {
            var status = this.nodeStatusMap[nodeId] || {};
            if (name === 'term') {
                return this.safeValue(status.currentTerm);
            }
            if (name === 'revision') {
                return this.safeValue(status.currentRevision);
            }
            if (name === 'commitIndex') {
                return this.safeValue(status.commitIndex);
            }
            if (name === 'keyCount') {
                return this.safeValue(status.keyCount);
            }
            if (name === 'hash') {
                return this.safeValue(this.kvStateHashMap[nodeId]);
            }
            return this.safeValue(status[name]);
        },
        nodeRole: function (nodeId) {
            var status = this.nodeStatusMap[nodeId] || {};
            if (status.role) {
                return String(status.role).toUpperCase();
            }
            return this.leaderNodeId === nodeId ? 'LEADER' : 'FOLLOWER';
        },
        roleTagType: function (nodeId) {
            return this.nodeRole(nodeId) === 'LEADER' ? 'primary' : 'info';
        },
        isLeader: function (nodeId) {
            return this.nodeRole(nodeId) === 'LEADER';
        },
        nodeRevisionState: function (nodeId) {
            var status = this.nodeStatusMap[nodeId] || {};
            if (typeof status.currentRevision === 'undefined') {
                return '未知';
            }
            if (this.leaderRevisionValue === null) {
                return '待对比';
            }
            return Number(status.currentRevision) === Number(this.leaderRevisionValue) ? '一致' : '落后/超前';
        },
        nodeRevisionTagType: function (nodeId) {
            var state = this.nodeRevisionState(nodeId);
            if (state === '一致') {
                return 'success';
            }
            if (state === '待对比' || state === '未知') {
                return 'info';
            }
            return 'warning';
        },
        nodeHashState: function (nodeId) {
            var hashValue = this.kvStateHashMap[nodeId];
            if (typeof hashValue === 'undefined') {
                return '未计算';
            }
            if (this.leaderHashValue === null) {
                return '待对比';
            }
            return String(hashValue) === String(this.leaderHashValue) ? '一致' : '不一致';
        },
        nodeHashTagType: function (nodeId) {
            var state = this.nodeHashState(nodeId);
            if (state === '一致') {
                return 'success';
            }
            if (state === '待对比' || state === '未计算') {
                return 'info';
            }
            return 'danger';
        },
        nodeRiskLevel: function (nodeId) {
            if (!this.nodeStatusMap[nodeId]) {
                return 'medium';
            }
            if (this.nodeHashState(nodeId) === '不一致') {
                return 'high';
            }
            if (this.nodeRevisionState(nodeId) === '落后/超前') {
                return 'medium';
            }
            return 'low';
        },
        nodeRiskTagType: function (nodeId) {
            var riskLevel = this.nodeRiskLevel(nodeId);
            if (riskLevel === 'high') {
                return 'danger';
            }
            if (riskLevel === 'medium') {
                return 'warning';
            }
            return 'success';
        },
        nodeRiskText: function (nodeId) {
            var riskLevel = this.nodeRiskLevel(nodeId);
            if (riskLevel === 'high') {
                return '高风险';
            }
            if (riskLevel === 'medium') {
                return '需关注';
            }
            return '正常';
        },
        nodeRiskHint: function (nodeId) {
            if (!this.nodeStatusMap[nodeId]) {
                return '当前节点还没有成功探测状态。';
            }
            if (this.nodeHashState(nodeId) === '不一致') {
                return '该节点的 KV 哈希与 Leader 不一致，说明状态机结果存在差异，需优先排查。';
            }
            if (this.nodeRevisionState(nodeId) === '落后/超前') {
                return '该节点 revision 与 Leader 不一致，可能还在追日志或诊断时点不一致。';
            }
            return '当前节点与 Leader 的 revision / hash 看起来正常。';
        },
        comparePrefixFingerprint: function (rangeResponse) {
            rangeResponse = rangeResponse || {};
            var itemList = rangeResponse.items || [];
            var normalizedItemList = itemList.map(function (item) {
                return {
                    key: item.key,
                    value: item.value,
                    modRevision: item.modRevision,
                    version: item.version,
                    leaseId: item.leaseId
                };
            });
            return JSON.stringify({
                success: rangeResponse.success !== false,
                count: rangeResponse.count || 0,
                revision: rangeResponse.revision || 0,
                items: normalizedItemList
            });
        },
        // ==================== Modal + Clipboard ====================
        safeValue: function (value) {
            return value === null || typeof value === 'undefined' || value === '' ? '-' : value;
        },
        openModal: function (title, message, confirmText, onConfirm) {
            this.modal.visible = true;
            this.modal.title = title;
            this.modal.message = message;
            this.modal.confirmText = String(confirmText || '');
            this.modal.input = '';
            this.modal.onConfirm = onConfirm;
        },
        closeModal: function () {
            this.modal.visible = false;
            this.modal.title = '';
            this.modal.message = '';
            this.modal.confirmText = '';
            this.modal.input = '';
            this.modal.onConfirm = function () {
            };
        },
        copyText: function (text) {
            if (navigator.clipboard) {
                navigator.clipboard.writeText(text || '');
                return;
            }
            var textarea = document.createElement('textarea');
            textarea.value = text || '';
            document.body.appendChild(textarea);
            textarea.select();
            document.execCommand('copy');
            document.body.removeChild(textarea);
        },
        // ==================== Operation Log Export ====================
        uiCopyLogs: function () {
            var content = this.visibleOperationLogs.map(function (log) {
                return [log.time, log.type, log.target, log.nodeId, log.status, log.cost + 'ms'].join('\t');
            }).join('\n');
            this.copyText(content);
        }
    }
});



