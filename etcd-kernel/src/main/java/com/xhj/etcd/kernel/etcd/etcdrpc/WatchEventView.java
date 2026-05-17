package com.xhj.etcd.kernel.etcd.etcdrpc;

import lombok.Data;

import java.io.Serializable;

/**
 * WatchEventView
 *
 * @author XJks
 * @description Watch 事件响应条目。
 */
@Data
public class WatchEventView implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * 事件类型。
     */
    private WatchEventType eventType;

    /**
     * 事件对应的 KV 视图。
     */
    private KeyValueView keyValue;

    /**
     * 构造 Watch 事件条目。
     */
    public static WatchEventView of(WatchEventType eventType, KeyValueView keyValue) {
        WatchEventView eventView = new WatchEventView();
        eventView.setEventType(eventType);
        eventView.setKeyValue(keyValue);
        return eventView;
    }
}
