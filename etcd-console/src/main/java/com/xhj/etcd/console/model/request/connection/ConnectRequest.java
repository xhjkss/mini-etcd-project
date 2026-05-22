package com.xhj.etcd.console.model.request.connection;

import lombok.Data;

/**
 * ConnectRequest
 *
 * @author XJks
 * @description 控制台连接请求。
 */
@Data
public class ConnectRequest {
    /**
     * 目标节点主机地址。
     */
    private String host;

    /**
     * 目标节点端口。
     */
    private int port;
}
