package com.xhj.etcd.kernel.etcd.etcdrpc;

import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * WatchCancelRequest
 *
 * @author XJks
 * @description Watch 取消请求。
 */
@Data
@NoArgsConstructor
public class WatchCancelRequest implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * Watch 会话 ID。
     */
    private long watchId;

    public WatchCancelRequest(long watchId) {
        this.watchId = watchId;
    }
}
