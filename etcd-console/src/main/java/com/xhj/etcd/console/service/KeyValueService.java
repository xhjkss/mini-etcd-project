package com.xhj.etcd.console.service;

import com.xhj.etcd.console.model.response.websocket.KeyValueChangedPayload;
import com.xhj.etcd.console.model.response.websocket.KeyValueChangeOperationType;
import com.xhj.etcd.console.model.response.websocket.WebSocketMessageType;
import com.xhj.etcd.console.websocket.ConsoleWebSocketGateway;
import com.xhj.etcd.kernel.etcd.etcdrpc.DeleteRangeRequest;
import com.xhj.etcd.kernel.etcd.etcdrpc.DeleteRangeResponse;
import com.xhj.etcd.kernel.etcd.etcdrpc.DeleteRequest;
import com.xhj.etcd.kernel.etcd.etcdrpc.DeleteResponse;
import com.xhj.etcd.kernel.etcd.etcdrpc.GetRequest;
import com.xhj.etcd.kernel.etcd.etcdrpc.GetResponse;
import com.xhj.etcd.kernel.etcd.etcdrpc.PutRequest;
import com.xhj.etcd.kernel.etcd.etcdrpc.PutResponse;
import com.xhj.etcd.kernel.etcd.etcdrpc.RangeRequest;
import com.xhj.etcd.kernel.etcd.etcdrpc.RangeResponse;
import com.xhj.etcd.rpc.NodeEndpoint;
import com.xhj.etcd.sdk.client.EtcdClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * KeyValueService
 *
 * @author XJks
 * @description KeyValue 控制台服务，负责 KV 请求编排与变更事件推送。
 */
@Service
public class KeyValueService {

    /**
     * 全量 RANGE 查询时使用的最小起始 key（非空，满足内核 key 校验）。
     */
    private static final String RANGE_ALL_START_KEY = "!";

    /**
     * 全量 RANGE 查询时使用的上界 key（左闭右开排他上界）。
     */
    private static final String RANGE_ALL_END_KEY_EXCLUSIVE = "\uffff\uffff\uffff";

    // ==================== 依赖组件 ====================
    /**
     * 连接服务。
     */
    @Autowired
    private ConnectionService connectionService;

    /**
     * WebSocket 推送服务。
     */
    @Autowired
    private ConsoleWebSocketGateway consoleWebSocketGateway;

    /**
     * 执行 GET 请求。
     */
    public GetResponse getOnEndpoint(String host, int port, GetRequest getRequest) {
        if (getRequest == null) {
            throw new IllegalArgumentException("getRequest must not be null");
        }
        EtcdClient etcdClient = connectionService.getEtcdClient();
        if (getRequest.isLinearizableRead()) {
            // 线性一致读必须由 Leader 处理，避免“用户选择的是 follower 节点”时直接返回 leader hint 错误。
            return etcdClient.get(getRequest);
        }
        NodeEndpoint nodeEndpoint = connectionService.requireConnectedEndpoint(host, port);
        return etcdClient.getOnEndpoint(nodeEndpoint, getRequest);
    }

    /**
     * 执行 RANGE 请求。
     */
    public RangeResponse rangeOnEndpoint(String host, int port, RangeRequest rangeRequest) {
        if (rangeRequest == null) {
            throw new IllegalArgumentException("rangeRequest must not be null");
        }
        normalizeRangeRequestForBrowseAll(rangeRequest);
        EtcdClient etcdClient = connectionService.getEtcdClient();
        if (rangeRequest.isLinearizableRead()) {
            // 线性一致 RANGE 必须走 Leader 路由，避免在 follower 上直接返回 leader hint。
            return etcdClient.range(rangeRequest);
        }
        NodeEndpoint nodeEndpoint = connectionService.requireConnectedEndpoint(host, port);
        return etcdClient.rangeOnEndpoint(nodeEndpoint, rangeRequest);
    }

    /**
     * 执行 PUT 请求并推送 KV_CHANGED 事件。
     */
    public PutResponse put(PutRequest putRequest) {
        EtcdClient etcdClient = connectionService.getEtcdClient();
        PutResponse putResponse = etcdClient.put(putRequest);
        publishKvChangedIfSuccess(
                KeyValueChangeOperationType.PUT,
                putRequest.getKey(),
                null,
                putResponse == null ? 0L : putResponse.getRevision());
        return putResponse;
    }

    /**
     * 执行 DELETE 请求并推送 KV_CHANGED 事件。
     */
    public DeleteResponse delete(DeleteRequest deleteRequest) {
        EtcdClient etcdClient = connectionService.getEtcdClient();
        DeleteResponse deleteResponse = etcdClient.delete(deleteRequest);
        publishKvChangedIfSuccess(
                KeyValueChangeOperationType.DELETE,
                deleteRequest.getKey(),
                null,
                deleteResponse == null ? 0L : deleteResponse.getRevision());
        return deleteResponse;
    }

    /**
     * 执行 DELETE_RANGE 请求并推送 KV_CHANGED 事件。
     */
    public DeleteRangeResponse deleteRange(DeleteRangeRequest deleteRangeRequest) {
        if (deleteRangeRequest == null) {
            throw new IllegalArgumentException("deleteRangeRequest must not be null");
        }
        EtcdClient etcdClient = connectionService.getEtcdClient();
        DeleteRangeResponse deleteRangeResponse = etcdClient.deleteRange(deleteRangeRequest);
        publishKvChangedIfSuccess(
                KeyValueChangeOperationType.DELETE_RANGE,
                null,
                deleteRangeRequest.getStartKey(),
                deleteRangeResponse == null ? 0L : deleteRangeResponse.getRevision());
        return deleteRangeResponse;
    }

    /**
     * 仅在 revision 有效时推送 KV_CHANGED 事件。
     */
    private void publishKvChangedIfSuccess(KeyValueChangeOperationType operationType,
                                           String key,
                                           String prefix,
                                           long revision) {
        if (revision <= 0L) {
            return;
        }
        KeyValueChangedPayload keyValueChangedPayload = new KeyValueChangedPayload();
        keyValueChangedPayload.setOperationType(operationType);
        keyValueChangedPayload.setKey(key);
        keyValueChangedPayload.setPrefix(prefix);
        keyValueChangedPayload.setRevision(revision);
        /**
         * TODO:
         *  KV_CHANGED 不再携带 nodeId。
         *  原因：写请求可能经历 leader redirect/切主，写后再读取 currentEndpoint 不能稳定反映“本次写命中的节点”。
         *  若继续携带不稳定 nodeId，会让前端分节点过滤出现误判（该刷不刷 / 刷错节点）。
         *  当前策略：只发布变更事实（operationType/key/prefix/revision），由前端按当前视图 + key/prefix 决定是否刷新。
         */
        consoleWebSocketGateway.broadcastEvent(WebSocketMessageType.KV_CHANGED, null, keyValueChangedPayload);
    }

    /**
     * 归一化 RANGE 请求，兼容控制台“全量浏览”场景。
     *
     * <p>当前内核要求 startKey 非空；当控制台请求“全部 key”且传入空 startKey 时，
     * 这里统一转换为 [RANGE_ALL_START_KEY, RANGE_ALL_END_KEY_EXCLUSIVE) 区间。</p>
     */
    private void normalizeRangeRequestForBrowseAll(RangeRequest rangeRequest) {
        if (rangeRequest.isPrefixMatch()) {
            return;
        }
        if (!isBlank(rangeRequest.getStartKey())) {
            return;
        }
        rangeRequest.setStartKey(RANGE_ALL_START_KEY);
        if (isBlank(rangeRequest.getEndKeyExclusive())) {
            rangeRequest.setEndKeyExclusive(RANGE_ALL_END_KEY_EXCLUSIVE);
        }
    }

    /**
     * 字符串判空。
     */
    private boolean isBlank(String value) {
        return value == null || value.trim().length() == 0;
    }

}
