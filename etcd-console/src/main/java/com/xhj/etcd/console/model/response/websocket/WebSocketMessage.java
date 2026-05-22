package com.xhj.etcd.console.model.response.websocket;

import lombok.Data;

import java.io.Serializable;

/**
 * WebSocketMessage
 *
 * @author XJks
 * @description WebSocket 统一消息信封。
 *
 * <p>前后端统一通过该结构传递 WebSocket 消息，避免直接传输松散 Map。</p>
 */
@Data
public class WebSocketMessage<T> implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * 消息类型。
     */
    private WebSocketMessageType messageType;

    /**
     * 节点 ID，可为空（例如全局广播）。
     */
    private String nodeId;

    /**
     * 消息负载。
     */
    private T payload;
}
