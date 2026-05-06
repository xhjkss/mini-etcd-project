package com.xhj.etcd.kernel.raft.core;

/**
 * RaftRoleType
 *
 * @author XJks
 * @description Raft 节点角色枚举，描述节点在当前任期内参与选举和日志复制时的身份。
 */
public enum RaftRoleType {

    /**
     * Follower 角色。
     *
     * <p>被动接收 Leader 的心跳、日志复制和快照安装请求；
     * 在选举超时后会切换为 Candidate 并发起新一轮选举。</p>
     */
    FOLLOWER,

    /**
     * Candidate 角色。
     *
     * <p>节点发起选举后的过渡状态，会向其他节点发送 RequestVote 请求；
     * 获得多数派投票后切换为 Leader，收到合法 Leader 消息后回退为 Follower。</p>
     */
    CANDIDATE,

    /**
     * Leader 角色。
     *
     * <p>负责接收上层提案、追加本地日志，并通过 AppendEntries 或 InstallSnapshot
     * 将日志和快照复制到其他节点。</p>
     */
    LEADER
}