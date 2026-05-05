package com.xhj.etcd.rpc;

import lombok.Data;

import java.io.Serializable;

/**
 * RpcMessage
 *
 * @author XJks
 * @description RPC 消息模型，承载一次网络传输中的消息类型、服务路由、协议载荷和框架异常信息。
 */
@Data
public class RpcMessage implements Serializable {

    // ==================== Protocol base info ====================

    /**
     * RPC 消息类型。
     *
     * <p>用于区分普通请求、普通响应、流式请求、流式响应、流式关闭等不同协议语义。</p>
     */
    private RpcMessageType type;

    /**
     * RPC 消息 ID，用于关联请求、响应和流式通道。
     * <p>
     * TODO: 同一次 RPC 调用的请求和响应必须使用相同 rpcMessageId；流式 RPC 场景下，后续数据帧也依赖该 ID 归属到同一条逻辑通道。</p>
     */
    private String rpcMessageId;

    // ==================== Service route info ====================

    /**
     * RPC 服务名。
     *
     * <p>服务端根据 serviceName 定位已经注册的服务实例。</p>
     */
    private String serviceName;

    /**
     * RPC 方法名。
     *
     * <p>服务端在 serviceName 对应的服务实例中，根据 methodName 分发到具体方法。</p>
     */
    private String methodName;

    // ==================== Payload info ====================

    /**
     * 序列化后的协议数据。
     *
     * <p>请求消息中通常表示请求对象，响应消息中通常表示响应对象；RpcMessage 只承载字节数据，不关心具体业务类型。</p>
     */
    private byte[] data;

    // ==================== RPC framework error info ====================

    /**
     * RPC 框架层异常信息。
     *
     * <p>该字段只表示服务不存在、方法不存在、反序列化失败、调用异常等框架层错误；业务失败应由具体响应对象表达，不应该写入该字段。</p>
     */
    private String errorMessage;
}