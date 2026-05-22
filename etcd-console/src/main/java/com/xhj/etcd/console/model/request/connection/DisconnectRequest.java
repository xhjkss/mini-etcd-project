package com.xhj.etcd.console.model.request.connection;

import lombok.Data;

/**
 * DisconnectRequest
 *
 * @author XJks
 * @description 控制台断连请求，使用 host 和 port 定位目标节点。
 */
@Data
public class DisconnectRequest {

    // ==================== 基础字段 ====================
    /**
     * 目标节点主机地址。
     */
    private String host;

    /**
     * 目标节点端口。
     */
    private int port;
}
