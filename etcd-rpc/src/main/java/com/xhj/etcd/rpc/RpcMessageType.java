package com.xhj.etcd.rpc;

/**
 * RpcMessageType
 *
 * @author XJks
 * @description RPC 消息类型枚举，用于区分请求、响应、心跳和流式事件。
 *
 * <p>枚举值用于区分固定协议分支，调用方应避免使用魔法字符串或魔法数字。</p>
 */
public enum RpcMessageType {
    /**
     * 客户端 -> 服务端的普通请求。
     */
    REQUEST,

    /**
     * 服务端 -> 客户端的普通响应。
     */
    RESPONSE,

    /**
     * 服务端 -> 客户端的流式推送消息，例如 watch 事件。
     */
    STREAM,

    /**
     * 客户端 <-> 服务端的心跳消息。
     */
    HEARTBEAT,

    /**
     * 服务端 -> 客户端的错误响应，或客户端内部生成的失败消息。
     */
    ERROR
}
