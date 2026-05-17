package com.xhj.etcd.kernel.etcd.etcdrpc;

import lombok.Data;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * WatchSubscribeResponse
 *
 * @author XJks
 * @description Watch 订阅响应。
 */
@Data
public class WatchSubscribeResponse implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * Watch 会话 ID。
     */
    private long watchId;

    /**
     * 当前状态机 revision。
     */
    private long currentRevision;

    /**
     * 当前会话下一次事件读取的起始 revision（含）。
     */
    private long nextRevision;

    /**
     * 回放事件列表。
     */
    private List<WatchEventView> events = new ArrayList<>();

    /**
     * 构造 Watch 订阅响应。
     */
    public static WatchSubscribeResponse of(long watchId, long currentRevision, long nextRevision, List<WatchEventView> events) {
        WatchSubscribeResponse response = new WatchSubscribeResponse();
        response.setWatchId(watchId);
        response.setCurrentRevision(currentRevision);
        response.setNextRevision(nextRevision);
        if (events != null) {
            response.setEvents(new ArrayList<>(events));
        }
        return response;
    }
}
