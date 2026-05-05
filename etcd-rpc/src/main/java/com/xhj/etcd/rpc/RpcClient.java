package com.xhj.etcd.rpc;

/**
 * RpcClient
 *
 * @author XJks
 * @description RPC 客户端接口，定义同步调用、单向发送、流式请求和心跳能力。
 */
public interface RpcClient {

    // ==================== 同步调用 ====================

    /**
     * 发起同步 RPC 调用并等待目标节点返回响应。
     */
    <T> T call(NodeEndpoint endpoint, String serviceName, String methodName, Object request, Class<T> responseClass);

    // ==================== 单向发送 ====================

    /**
     * 发送单向 RPC 消息。
     */
    void send(NodeEndpoint endpoint, String serviceName, String methodName, Object request);

    // ==================== 流式请求 ====================

    /**
     * 打开 RPC 流式通道。
     */
    RpcStream openStream(String streamId, NodeEndpoint endpoint, String serviceName, String methodName, Object request, RpcMessageHandler handler);

    // ==================== 心跳 ====================

    /**
     * 向目标节点发送心跳探测。
     */
    boolean heartbeat(NodeEndpoint endpoint);

    // ==================== 关闭 ====================

    /**
     * 关闭当前组件并释放资源。
     */
    void shutdown();
}
