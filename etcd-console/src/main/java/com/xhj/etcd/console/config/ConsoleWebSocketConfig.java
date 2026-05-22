package com.xhj.etcd.console.config;

import com.xhj.etcd.console.websocket.ConsoleWebSocketGateway;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.socket.config.annotation.EnableWebSocket;
import org.springframework.web.socket.config.annotation.WebSocketConfigurer;
import org.springframework.web.socket.config.annotation.WebSocketHandlerRegistry;

/**
 * ConsoleWebSocketConfig
 *
 * @author XJks
 * @description 控制台 WebSocket 配置，负责注册 `/ws/console` 端点。
 */
@Configuration
@EnableWebSocket
public class ConsoleWebSocketConfig implements WebSocketConfigurer {

    // ==================== 依赖组件 ====================
    /**
     * 控制台 WebSocket 处理器。
     */
    @Autowired
    private ConsoleWebSocketGateway consoleWebSocketGateway;

    /**
     * 注册 WebSocket 处理器映射。
     */
    @Override
    public void registerWebSocketHandlers(WebSocketHandlerRegistry registry) {
        registry.addHandler(consoleWebSocketGateway, "/ws/console").setAllowedOrigins("*");
    }
}
