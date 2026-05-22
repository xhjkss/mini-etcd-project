package com.xhj.etcd.console.model.response.websocket;

/**
 * KeyValueChangeOperationType
 *
 * @author XJks
 * @description KV 变更操作类型。
 */
public enum KeyValueChangeOperationType {
    /**
     * 单 key put 写入。
     */
    PUT,

    /**
     * 单 key 删除。
     */
    DELETE,

    /**
     * 区间/前缀删除。
     */
    DELETE_RANGE,

    /**
     * 来源于 watch 推送的变更。
     */
    WATCH
}
