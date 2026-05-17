package com.xhj.etcd.kernel.etcd.etcdrpc;

import lombok.Data;

import java.io.Serializable;

/**
 * NodeStatusRequest
 *
 * @author XJks
 * @description 节点诊断请求。
 *
 * <p>当前阶段 NodeStatus 不需要额外参数，直接读取当前节点的 raft / kv / lease / watch 运行态信息。</p>
 */
@Data
public class NodeStatusRequest implements Serializable {

    private static final long serialVersionUID = 1L;
}
