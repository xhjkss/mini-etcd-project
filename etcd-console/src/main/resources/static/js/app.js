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
        valueViewMode: 'text',
        connectForm: {
            host: '127.0.0.1',
            port: 2379
        },
        kvForm: {
            key: '',
            value: '',
            leaseId: null
        },
        watchForm: {
            key: '',
            prefix: false
        },
        watchSessions: [],
        watchEvents: [],
        watchEventOnlyCurrentConnection: true,
        onboardingGuideVisible: true,
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
            ttlSeconds: 10
        },
        comparePrefixValue: '',
        compactRevision: 0,
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
        onboardingActiveStep: function () {
            if (!this.connections.length) {
                return 0;
            }
            if (!this.hasCurrentConnection) {
                return 1;
            }
            return 2;
        },
        showOnboardingGuide: function () {
            return this.onboardingGuideVisible;
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
        webSocketStatsLabel: function () {
            return '消息' + this.webSocketStats.messageCount + ' | 重连' + this.webSocketStats.reconnectCount + ' | 解析错' + this.webSocketStats.parseErrorCount;
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
        filteredWatchEvents: function () {
            var _this = this;
            if (!this.watchEventOnlyCurrentConnection || !this.currentNodeId) {
                return this.watchEvents;
            }
            return this.watchEvents.filter(function (watchEvent) {
                return !watchEvent.nodeId || watchEvent.nodeId === _this.currentNodeId;
            });
        },
        watchEventsText: function () {
            if (!this.filteredWatchEvents.length) {
                return '暂无 Watch 事件';
            }
            return this.filteredWatchEvents.map(function (watchEvent) {
                var endpointPrefix = watchEvent.endpoint ? ('[' + watchEvent.endpoint + '] ') : '';
                return endpointPrefix + watchEvent.text;
            }).join('\n');
        },
        formattedSelectedValue: function () {
            if (!this.selectedKv) {
                return '';
            }
            var value = this.selectedKv.value;
            if (this.valueViewMode === 'json') {
                try {
                    return JSON.stringify(JSON.parse(value), null, 2);
                } catch (e) {
                    return '当前 Value 不是合法 JSON：\n\n' + value;
                }
            }
            return value;
        }
    },
    mounted: function () {
        this.pageDestroyed = false;
        this.connectionLoadConnections();
        this.webSocketConnect();
        this.watchLoadSessions();
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
                    _this.notify('error', errorData.message || error.message || '操作失败');
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
        fillLocalCluster: function () {
            this.connectForm = {
                host: '127.0.0.1',
                port: 2379
            };
            this.notify('success', '已填入 127.0.0.1:2379。node2/node3 请修改端口后分别连接。');
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
                    _this.browserRefresh();
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
                    _this.browserRefresh();
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
                    _this.browserRefresh();
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
                    _this.browserRefresh();
                }
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
        // ==================== Lease ====================
        grantLease: function () {
            var ttl = this.requirePositiveNumber(this.leaseForm.ttlSeconds, 'TTL 秒');
            if (!ttl) {
                return;
            }
            this.leaseForm.ttlSeconds = ttl;
            this.handleApi(apiClient.lease.grant(requestBuilder.lease.buildGrant(this.leaseForm.leaseId, ttl)),
                {type: 'LEASE_GRANT', target: this.leaseForm.leaseId || 'auto', nodeId: this.currentNodeId});
        },
        ttlLease: function () {
            var leaseId = this.requirePositiveNumber(this.leaseForm.leaseId, 'LeaseId');
            if (!leaseId) {
                return;
            }
            this.leaseForm.leaseId = leaseId;
            this.handleApi(apiClient.lease.ttl(requestBuilder.lease.buildTtl(this.leaseForm.leaseId)),
                {type: 'LEASE_TTL', target: this.leaseForm.leaseId, nodeId: this.currentNodeId});
        },
        listLease: function () {
            this.handleApi(apiClient.lease.list(requestBuilder.lease.buildList()), {
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
            this.closeModal();
            this.handleApi(apiClient.lease.revoke(requestBuilder.lease.buildRevoke(this.leaseForm.leaseId)),
                {type: 'LEASE_REVOKE', target: this.leaseForm.leaseId, nodeId: this.currentNodeId});
        },
        // ==================== Compact + Consistency ====================
        comparePrefix: function () {
            var prefix = this.requirePrefix(this.comparePrefixValue, '多节点 Prefix 对比');
            if (!prefix) {
                return;
            }
            this.comparePrefixValue = prefix;
            this.handleApi(apiClient.cluster.rangeOnAllNodes(requestBuilder.cluster.buildRangeByPrefix(prefix)),
                {type: 'COMPARE_PREFIX', target: prefix, nodeId: 'all'});
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
                {type: 'COMPACT', target: this.compactRevision, nodeId: this.currentNodeId}).then(function () {
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
                    _this.webSocketAppendWatchEvent(payload, messageNodeId);
                    if (_this.isKvChangeRelevant(messageNodeId, payload)) {
                        _this.scheduleBrowserRealtimeRefresh();
                    }
                } else if (messageType === 'KV_CHANGED') {
                    if (_this.isKvChangeRelevant(messageNodeId, payload)) {
                        _this.scheduleBrowserRealtimeRefresh();
                    }
                } else if (messageType === 'WATCH_CREATED' || messageType === 'WATCH_CANCELED' || messageType === 'WATCH_ERROR') {
                    _this.watchLoadSessions();
                    _this.webSocketAppendWatchEvent(webSocketMessage, messageNodeId);
                } else if (messageType === 'LEASE_TTL') {
                    _this.result = payload;
                    _this.resultSummary = {success: true, type: 'LEASE_TTL', target: messageNodeId || '', message: 'TTL 推送'};
                }
            };
        },
        webSocketAppendWatchEvent: function (event, nodeId) {
            var endpoint = this.displayEndpointByNodeId(nodeId);
            this.watchEvents.unshift({
                id: new Date().getTime() + '-' + Math.random(),
                nodeId: nodeId || '',
                endpoint: endpoint && endpoint !== '-' ? endpoint : '',
                text: JSON.stringify(event)
            });
            if (this.watchEvents.length > 100) {
                this.watchEvents.pop();
            }
        },
        webSocketClearWatchEvents: function () {
            this.watchEvents = [];
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
        uiCloseOnboardingGuide: function () {
            this.onboardingGuideVisible = false;
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
            var content = this.operationLogs.map(function (log) {
                return [log.time, log.type, log.target, log.nodeId, log.status, log.cost + 'ms'].join('\t');
            }).join('\n');
            this.copyText(content);
        }
    }
});



