package com.xhj.etcd.sdk.client.watch;

import com.xhj.etcd.rpc.NodeEndpoint;

/**
 * WatchHandle
 *
 * @author XJks
 * @description Watch 长连接句柄。
 */
public interface WatchHandle {

    /**
     * 获取 watchId。
     */
    long getWatchId();

    /**
     * 获取订阅节点 endpoint。
     */
    NodeEndpoint getEndpoint();

    /**
     * 获取订阅起始 key。
     */
    String getStartKey();

    /**
     * 是否前缀订阅。
     */
    boolean isPrefixMatch();

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
