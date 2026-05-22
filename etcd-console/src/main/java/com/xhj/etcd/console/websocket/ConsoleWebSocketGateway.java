package com.xhj.etcd.console.websocket;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.xhj.etcd.console.model.response.websocket.WebSocketMessage;
import com.xhj.etcd.console.model.response.websocket.WebSocketMessageType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.ConcurrentWebSocketSessionDecorator;
import org.springframework.web.socket.handler.TextWebSocketHandler;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * ConsoleWebSocketGateway
 *
 * @author XJks
 * @description 控制台 WebSocket 网关：统一承载会话生命周期管理与消息广播。
 */
@Component
public class ConsoleWebSocketGateway extends TextWebSocketHandler {

    // ==================== 依赖组件 ====================
    /**
     * JSON 编码器。
     */
    @Autowired
    private ObjectMapper objectMapper;

    // ==================== 运行时状态 ====================
    /**
     * 当前在线的 WebSocket 会话集合。
     */
    private final Map<String, WebSocketSession> activeWebSocketSessionMap = new ConcurrentHashMap<>();

    /**
     * 连接建立后注册会话。
     */
    @Override
    public void afterConnectionEstablished(WebSocketSession session) {
        addSession(session);
    }

    /**
     * 当前网关不处理客户端文本消息，业务请求统一走 HTTP API。
     */
    @Override
    protected void handleTextMessage(WebSocketSession session, TextMessage message) {
        // 当前 WebSocket 只负责服务端推送，业务请求统一走 HTTP API。
    }

    /**
     * 连接关闭后移除会话。
     */
    @Override
    public void afterConnectionClosed(WebSocketSession session, CloseStatus status) {
        removeSession(session);
    }

    /**
     * 连接传输异常后移除会话。
     */
    @Override
    public void handleTransportError(WebSocketSession session, Throwable exception) {
        removeSession(session);
    }

    /**
     * 注册一个 WebSocket 会话。
     */
    public void addSession(WebSocketSession session) {
        if (session == null) {
            return;
        }
        // 使用并发写保护会话，避免同一 session 并发 send 时抛出 TEXT_PARTIAL_WRITING。
        WebSocketSession safeSession = session instanceof ConcurrentWebSocketSessionDecorator
                ? session
                : new ConcurrentWebSocketSessionDecorator(session, 10_000, 1_048_576);
        activeWebSocketSessionMap.put(session.getId(), safeSession);
    }

    /**
     * 移除一个 WebSocket 会话。
     */
    public void removeSession(WebSocketSession session) {
        if (session == null) {
            return;
        }
        activeWebSocketSessionMap.remove(session.getId());
    }

    /**
     * 发送一条控制台事件消息。
     *
     * @param messageType 事件类型
     * @param nodeId      节点 ID
     * @param payload     事件负载
     */
    public void broadcastEvent(WebSocketMessageType messageType, String nodeId, Object payload) {
        // 1) 统一封装消息信封，前端按 messageType + nodeId + payload 解包处理。
        WebSocketMessage<Object> webSocketMessage = new WebSocketMessage<>();
        webSocketMessage.setMessageType(messageType);
        webSocketMessage.setNodeId(nodeId);
        webSocketMessage.setPayload(payload);
        try {
            // 2) 主路径：序列化业务消息并广播给所有活跃会话。
            broadcastTextFrame(objectMapper.writeValueAsString(webSocketMessage));
        } catch (JsonProcessingException exception) {
            // 3) 兜底路径：业务 payload 序列化失败时，降级推送 CONSOLE_ERROR 事件。
            WebSocketMessage<String> errorMessage = new WebSocketMessage<>();
            errorMessage.setMessageType(WebSocketMessageType.CONSOLE_ERROR);
            errorMessage.setNodeId(nodeId);
            errorMessage.setPayload(exception.getMessage());
            try {
                broadcastTextFrame(objectMapper.writeValueAsString(errorMessage));
            } catch (JsonProcessingException ignored) {
                // 降级消息也序列化失败时直接吞掉，避免递归失败导致死循环。
            }
        }
    }

    /**
     * 向指定会话发送一条事件消息（仅该会话可见）。
     */
    public void sendEventToSession(String sessionId, WebSocketMessageType messageType, String nodeId, Object payload) {
        if (sessionId == null || sessionId.trim().length() == 0) {
            return;
        }
        WebSocketSession session = activeWebSocketSessionMap.get(sessionId);
        if (session == null || !session.isOpen()) {
            removeSessionBySessionId(sessionId);
            return;
        }
        WebSocketMessage<Object> webSocketMessage = new WebSocketMessage<>();
        webSocketMessage.setMessageType(messageType);
        webSocketMessage.setNodeId(nodeId);
        webSocketMessage.setPayload(payload);
        try {
            sendTextFrame(session, objectMapper.writeValueAsString(webSocketMessage));
        } catch (JsonProcessingException exception) {
            try {
                WebSocketMessage<String> errorMessage = new WebSocketMessage<>();
                errorMessage.setMessageType(WebSocketMessageType.CONSOLE_ERROR);
                errorMessage.setNodeId(nodeId);
                errorMessage.setPayload(exception.getMessage());
                sendTextFrame(session, objectMapper.writeValueAsString(errorMessage));
            } catch (JsonProcessingException ignored) {
                // 降级消息序列化失败时直接忽略。
            }
        }
    }

    /**
     * 是否存在活跃 WebSocket 会话。
     *
     * <p>用于上层调度器“按需广播”判定：无前端会话时可直接跳过状态轮询与推送。</p>
     */
    public boolean hasActiveSession() {
        return activeSessionCount() > 0;
    }

    /**
     * 获取当前活跃会话数量（会先清理已关闭会话）。
     */
    public int activeSessionCount() {
        for (WebSocketSession session : activeWebSocketSessionMap.values()) {
            if (!session.isOpen()) {
                removeSessionBySessionId(session.getId());
            }
        }
        return activeWebSocketSessionMap.size();
    }

    /**
     * 获取当前活跃会话 ID 列表（会先清理失活会话）。
     */
    public List<String> listActiveSessionIdList() {
        List<String> activeSessionIdList = new ArrayList<>();
        for (WebSocketSession session : activeWebSocketSessionMap.values()) {
            if (session == null || !session.isOpen()) {
                if (session != null) {
                    removeSessionBySessionId(session.getId());
                }
                continue;
            }
            activeSessionIdList.add(session.getId());
        }
        return activeSessionIdList;
    }

    /**
     * 广播原始文本消息到全部在线会话。
     */
    private void broadcastTextFrame(String textMessage) {
        // 遍历在线会话快照广播消息；遇到异常会话时就地剔除，保证后续广播可持续。
        for (WebSocketSession session : activeWebSocketSessionMap.values()) {
            if (!session.isOpen()) {
                removeSessionBySessionId(session.getId());
                continue;
            }
            if (!sendTextFrame(session, textMessage)) {
                removeSessionBySessionId(session.getId());
            }
        }
    }

    /**
     * 向会话发送文本帧。
     */
    private boolean sendTextFrame(WebSocketSession session, String textMessage) {
        try {
            session.sendMessage(new TextMessage(textMessage));
            return true;
        } catch (IOException | IllegalStateException exception) {
            return false;
        }
    }

    /**
     * 按 sessionId 移除会话并清理 bootstrap 集合。
     */
    private void removeSessionBySessionId(String sessionId) {
        if (sessionId == null || sessionId.trim().length() == 0) {
            return;
        }
        activeWebSocketSessionMap.remove(sessionId);
    }
}
