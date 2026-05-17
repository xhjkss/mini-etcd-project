package com.xhj.etcd.sdk.client.watch;

/**
 * WatchHandle
 *
 * @author XJks
 * @description Watch 长连接句柄。
 *
 * <p>TODO: 句柄只保留最小控制面能力，不公开 watchId、endpoint 等实现细节。</p>
 */
public interface WatchHandle {

    /**
     * 发送取消请求。
     */
    void cancel();

    /**
     * 关闭句柄。
     */
    void close();

    /**
     * 是否已关闭。
     */
    boolean isClosed();
}
