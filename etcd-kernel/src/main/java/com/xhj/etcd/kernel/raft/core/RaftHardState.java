package com.xhj.etcd.kernel.raft.core;

import lombok.Data;

import java.io.Serializable;

/**
 * RaftHardState
 *
 * @author XJks
 * @description Raft 持久化状态，记录节点在重启后仍必须保留的任期和投票信息。
 *
 * <p>TODO: currentTerm 和 votedFor 必须在节点对外发送响应前持久化，否则节点重启后可能在同一任期内重复投票，破坏 Raft 选举安全性。</p>
 */
@Data
public class RaftHardState implements Serializable {

    /**
     * 当前任期编号。
     *
     * <p>节点收到更大 term 的 RPC 请求或响应时，需要更新该字段并回退为 Follower。</p>
     */
    private long currentTerm;

    /**
     * 当前任期内已投票的 Candidate ID。
     *
     * <p>没有投票时为 null；同一任期内只能投票给一个 Candidate。</p>
     */
    private String votedFor;
}