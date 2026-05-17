package com.xhj.etcd.kernel.etcd.etcdrpc;

import lombok.Data;

import java.io.Serializable;

/**
 * WatchCancelResponse
 *
 * @author XJks
 * @description Watch 取消响应。
 */
@Data
public class WatchCancelResponse implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * Watch 会话 ID。
     */
    private long watchId;

    /**
     * 是否成功取消。
     */
    private boolean canceled;

    /**
     * 当前状态机 revision。
     */
    private long currentRevision;

    /**
     * 构造 Watch 取消响应。
     */
    public static WatchCancelResponse of(long watchId, boolean canceled, long currentRevision) {
        WatchCancelResponse response = new WatchCancelResponse();
        response.setWatchId(watchId);
        response.setCanceled(canceled);
        response.setCurrentRevision(currentRevision);
        return response;
    }
}
