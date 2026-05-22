package com.xhj.etcd.console.model.response.websocket;

import com.xhj.etcd.console.model.response.watch.WatchSessionResponse;
import com.xhj.etcd.kernel.etcd.etcdrpc.WatchNotification;
import lombok.Data;

import java.io.Serializable;

/**
 * WatchNotificationPayload
 *
 * @author XJks
 * @description Watch 事件通知负载。
 */
@Data
public class WatchNotificationPayload implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * watch 会话信息。
     */
    private WatchSessionResponse watchSessionResponse;

    /**
     * Watch 通知，直接复用 etcdrpc.WatchNotification。
     */
    private WatchNotification watchNotification;
}
