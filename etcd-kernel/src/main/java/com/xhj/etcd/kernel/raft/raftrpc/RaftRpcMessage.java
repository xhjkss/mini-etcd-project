package com.xhj.etcd.kernel.raft.raftrpc;

import lombok.Data;

import java.io.Serializable;

/**
 * RaftRpcMessage
 *
 * @author XJks
 * @description Raft 节点间 RPC 消息信封，用于封装消息类型、目标节点和序列化后的 RPC 数据。
 *
 * <p>TODO: 该类是 RaftNode 和 EtcdNode 网络发送层之间的边界对象。
 * RaftNode 只负责生成需要发送的 RaftRpcMessage，并通过 RaftReady.messagesToSend 暴露；
 * EtcdNode 再根据 targetNodeId 找到目标节点地址，并通过 RPC 客户端实际发送。</p>
 */
@Data
public class RaftRpcMessage implements Serializable {

    /**
     * Raft RPC 消息类型。
     *
     * <p>用于区分 RequestVote、AppendEntries、InstallSnapshot 及其响应消息，
     * EtcdNode 发送前会根据该类型选择对应的远程方法。</p>
     */
    private RaftRpcMessageType type;

    /**
     * 目标节点 ID。
     *
     * <p>该字段只表示 Raft 集群内的目标节点标识，不直接保存网络地址；
     * EtcdNode 需要通过节点注册信息将 targetNodeId 转换为 NodeEndpoint 后再发送。</p>
     */
    private String targetNodeId;

    /**
     * 序列化后的 Raft RPC 数据。
     *
     * <p>具体数据类型由 type 决定，例如 RequestVoteRequest、AppendEntriesRequest、
     * AppendEntriesResponse 或 InstallSnapshotRequest 等。</p>
     */
    private byte[] data;
}