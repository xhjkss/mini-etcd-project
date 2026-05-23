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
     *
     * <p>TODO: watchId 由服务端在 subscribe 成功后分配并返回。SUBSCRIBING 阶段返回 0；收到成功 ACK 后返回正数 watchId。</p>
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
     * 关闭句柄。
     *
     * <p>关闭语义：SDK 会先尝试向服务端发送 cancel 请求；无论 cancel 成功、失败或超时，
     * 最终都会执行本地关闭收敛，保证句柄进入 CLOSED 状态。</p>
     */
    void close();

    /**
     * 是否已关闭。
     */
    boolean isClosed();
}
