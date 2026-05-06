package com.xhj.etcd.kernel.raft.event;

/**
 * RaftEventType
 *
 * @author XJks
 * @description Raft 内部事件类型枚举，用于标识 RaftNode 事件循环需要处理的输入类型。
 *
 * <p>TODO: RaftNode 通过事件队列串行处理本地提案、节点间 RPC、快照创建和 Ready 推进事件，避免多个线程直接并发修改 Raft 核心状态。</p>
 */
public enum RaftEventType {

    /**
     * 上层提交写命令事件。
     *
     * <p>通常由 EtcdNode 将业务命令序列化后提交给 RaftNode，用于生成新的本地日志提案。</p>
     */
    PROPOSE,

    /**
     * RequestVote 请求事件。
     *
     * <p>由其他 Raft 节点发起，用于候选人在选举期间向当前节点请求投票。</p>
     */
    REQUEST_VOTE,

    /**
     * RequestVote 响应事件。
     *
     * <p>由其他 Raft 节点返回，用于候选人统计投票结果并判断是否成为 Leader。</p>
     */
    REQUEST_VOTE_RESPONSE,

    /**
     * AppendEntries 请求事件。
     *
     * <p>由 Leader 发起，用于心跳保活或向 Follower 复制日志。</p>
     */
    APPEND_ENTRIES,

    /**
     * AppendEntries 响应事件。
     *
     * <p>由 Follower 返回，用于 Leader 更新该节点的复制进度，并尝试推进 commitIndex。</p>
     */
    APPEND_ENTRIES_RESPONSE,

    /**
     * InstallSnapshot 请求事件。
     *
     * <p>由 Leader 发起，用于 Follower 日志落后过多时直接安装快照。</p>
     */
    INSTALL_SNAPSHOT,

    /**
     * InstallSnapshot 响应事件。
     *
     * <p>由 Follower 返回，用于 Leader 更新该节点快照安装后的复制进度。</p>
     */
    INSTALL_SNAPSHOT_RESPONSE,

    /**
     * 创建快照事件。
     *
     * <p>由上层状态机生成快照数据后提交给 RaftNode，
     * RaftNode 根据快照覆盖的日志边界压缩本地 Raft 日志。</p>
     */
    CREATE_SNAPSHOT,

    /**
     * Ready 推进事件。
     *
     * <p>由 EtcdNode 在完成 RaftReady 中的持久化、消息发送和状态机 apply 后提交，
     * 用于通知 RaftNode 清理对应的 pending 状态。</p>
     */
    ADVANCE
}