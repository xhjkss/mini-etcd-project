package com.xhj.etcd.rpc;

/**
 * RpcStream
 *
 * @author XJks
 * @description RPC 流式通道接口，负责向已经建立的 stream 继续发送消息。
 */
public interface RpcStream {

    // ==================== Stream 标识 ====================

    /**
     * 返回当前 RPC stream 对应的消息ID。
     */
    String getRpcMessageId();

    // ==================== Stream 内请求 ====================

    /**
     * 在当前 stream 上继续发送一个 client -> server 请求。
     *
     * <p>该方法只会发送 {@link RpcMessageType#REQUEST} 类型消息，不支持调用方指定任意消息类型。
     * 主要用于 watch cancel 这类需要复用 stream id 的控制请求。
     */
    void sendRequest(String serviceName, String methodName, Object request);

    // ==================== Stream 生命周期 ====================

    /**
     * 判断当前句柄是否已经关闭。
     */
    boolean isClosed();

    /**
     * 关闭当前句柄或客户端。
     */
    void close();
}
