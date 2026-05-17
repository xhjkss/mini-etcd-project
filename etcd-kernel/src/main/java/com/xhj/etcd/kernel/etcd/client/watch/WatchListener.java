package com.xhj.etcd.kernel.etcd.client.watch;

import com.xhj.etcd.kernel.etcd.etcdrpc.WatchCancelResponse;
import com.xhj.etcd.kernel.etcd.etcdrpc.WatchNotification;
import com.xhj.etcd.kernel.etcd.etcdrpc.WatchSubscribeResponse;

/**
 * WatchListener
 *
 * @author XJks
 * @description Watch 长连接监听器。
 */
public interface WatchListener {

    /**
     * Watch 订阅成功回调。
     */
    void onSubscribed(WatchSubscribeResponse response);

    /**
     * Watch 通知回调。
     */
    void onNotification(WatchNotification response);

    /**
     * Watch 取消回调。
     */
    void onCanceled(WatchCancelResponse response);

    /**
     * Watch 异常回调。
     */
    void onError(Throwable cause);
}
