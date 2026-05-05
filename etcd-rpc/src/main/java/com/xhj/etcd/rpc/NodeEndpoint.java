package com.xhj.etcd.rpc;

import lombok.Data;

import java.io.Serializable;

/**
 * NodeEndpoint
 *
 * @author XJks
 * @description 节点地址模型，用 nodeId、host 和 port 描述一个可访问的节点。
 */
@Data
public class NodeEndpoint implements Serializable {

    /**
     * 当前节点唯一标识，用于日志、路由和状态归属判断。
     */
    private String nodeId;
    /**
     * 节点主机地址。
     */
    private String host;
    /**
     * 节点端口。
     */
    private int port;

    public NodeEndpoint() {
    }

    public NodeEndpoint(String nodeId, String host, int port) {
        this.nodeId = nodeId;
        this.host = host;
        this.port = port;
    }

    public String endpointKey() {
        return host + ":" + port;
    }
}
