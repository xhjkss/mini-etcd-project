package com.xhj.etcd.console.model.response.websocket;

import com.xhj.etcd.kernel.etcd.etcdrpc.WatchEventView;
import lombok.Data;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * KeyValueChangedPayload
 *
 * @author XJks
 * @description KV 变更消息负载。
 */
@Data
public class KeyValueChangedPayload implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * 变更操作类型，例如 PUT、DELETE、DELETE_RANGE、WATCH。
     */
    private KeyValueChangeOperationType operationType;

    /**
     * 受影响单 key。
     */
    private String key;

    /**
     * 受影响前缀。
     */
    private String prefix;

    /**
     * 变更对应 revision。
     */
    private long revision;

    /**
     * watch 场景下的事件列表，直接复用 etcdrpc.WatchEventView。
     */
    private List<WatchEventView> watchEventViewList = new ArrayList<>();
}
