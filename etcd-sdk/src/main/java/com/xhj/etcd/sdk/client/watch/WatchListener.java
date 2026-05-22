package com.xhj.etcd.sdk.client.watch;

import com.xhj.etcd.kernel.etcd.etcdrpc.WatchCancelResponse;
import com.xhj.etcd.kernel.etcd.etcdrpc.WatchNotification;
import com.xhj.etcd.kernel.etcd.etcdrpc.WatchSubscribeResponse;

/**
 * WatchListener
 *
 * @author XJks
 * @description Watch 长连接监听器。
 *
 * <p>
 * TODO:
 *  一个 WatchListener 实例应只绑定一个 watchHandle（与一个 watch 订阅一对一）。
 *  不能复用同一个 WatchListener 实例去承载多个并发 watch，否则会增加回调状态串线风险。
 * </p>
 */
public abstract class WatchListener {

    /**
     * 当前监听器绑定的 watch 句柄。
     */
    private volatile WatchHandle watchHandle;

    /**
     * 绑定当前监听器对应的 watch 句柄。
     */
    public final void bindWatchHandle(WatchHandle watchHandle) {
        this.watchHandle = watchHandle;
    }

    /**
     * 获取当前监听器绑定的 watch 句柄。
     */
    public final WatchHandle getWatchHandle() {
        return watchHandle;
    }

    /**
     * Watch 订阅成功回调。
     */
    public abstract void onSubscribed(WatchSubscribeResponse response);

    /**
     * Watch 通知回调。
     */
    public abstract void onNotification(WatchNotification response);

    /**
     * Watch 取消回调。
     */
    public abstract void onCanceled(WatchCancelResponse response);

    /**
     * Watch 异常回调。
     */
    public abstract void onError(Throwable cause);
}
