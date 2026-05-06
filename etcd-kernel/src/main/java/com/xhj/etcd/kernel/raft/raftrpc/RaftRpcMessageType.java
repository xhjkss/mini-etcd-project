package com.xhj.etcd.kernel.raft.raftrpc;

/**
 * RaftRpcMessageType
 *
 * @author XJks
 * @description Raft 节点间 RPC 消息类型枚举，用于标识 RaftRpcMessage 中承载的具体 RPC 数据类型。
 */
public enum RaftRpcMessageType {

    /**
     * RequestVote 请求。
     *
     * <p>候选人发起选举时，向其他节点请求投票。</p>
     */
    REQUEST_VOTE,

    /**
     * RequestVote 响应。
     *
     * <p>其他节点对候选人投票请求的处理结果。</p>
     */
    REQUEST_VOTE_RESPONSE,

    /**
     * AppendEntries 请求。
     *
     * <p>Leader 用于发送心跳、复制日志或推进 Follower 的 commitIndex。</p>
     */
    APPEND_ENTRIES,

    /**
     * AppendEntries 响应。
     *
     * <p>Follower 对心跳或日志复制请求的处理结果，Leader 据此更新复制进度。</p>
     */
    APPEND_ENTRIES_RESPONSE,

    /**
     * InstallSnapshot 请求。
     *
     * <p>Leader 在 Follower 日志落后过多时，向其发送快照数据。</p>
     */
    INSTALL_SNAPSHOT,

    /**
     * InstallSnapshot 响应。
     *
     * <p>Follower 对快照安装请求的处理结果，Leader 据此更新该节点的复制进度。</p>
     */
    INSTALL_SNAPSHOT_RESPONSE
}