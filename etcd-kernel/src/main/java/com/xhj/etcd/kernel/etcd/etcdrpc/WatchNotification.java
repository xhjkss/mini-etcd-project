package com.xhj.etcd.kernel.etcd.etcdrpc;

import lombok.Data;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * WatchNotification
 *
 * @author XJks
 * @description Watch 长连接通知。
 */
@Data
public class WatchNotification implements Serializable {

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
     * 下一次事件读取起始 revision（含）。
     */
    private long nextRevision;

    /**
     * 是否已取消。
     */
    private boolean canceled;

    /**
     * 是否因 compact 边界取消。
     */
    private boolean compacted;

    /**
     * 触发 compacted 取消时的 compactRevision。
     */
    private long compactRevision;

    /**
     * 事件列表。
     */
    private List<WatchEventView> events = new ArrayList<>();

    /**
     * 构造普通通知。
     */
    public static WatchNotification of(long watchId, long currentRevision, long nextRevision, List<WatchEventView> events) {
        WatchNotification response = new WatchNotification();
        response.setWatchId(watchId);
        response.setCurrentRevision(currentRevision);
        response.setNextRevision(nextRevision);
        if (events != null) {
            response.setEvents(new ArrayList<>(events));
        }
        return response;
    }

    /**
     * 构造 compacted 取消通知。
     */
    public static WatchNotification compactedCancel(long watchId, long currentRevision, long compactRevision) {
        WatchNotification response = new WatchNotification();
        response.setWatchId(watchId);
        response.setCurrentRevision(currentRevision);
        response.setNextRevision(currentRevision + 1L);
        response.setCanceled(true);
        response.setCompacted(true);
        response.setCompactRevision(compactRevision);
        return response;
    }
}
