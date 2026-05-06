package com.xhj.etcd.kernel.raft.raftrpc;

import lombok.Data;

import java.io.Serializable;

/**
 * RequestVoteResponse
 *
 * @author XJks
 * @description RequestVote RPC 响应，用于投票节点向 Candidate 返回投票结果。
 */
@Data
public class RequestVoteResponse implements Serializable {

    /**
     * 响应节点当前任期。
     *
     * <p>Candidate 收到更大 term 时需要更新本地任期并回退为 Follower；
     * 如果响应 term 小于 Candidate 当前任期，通常会被视为过期响应。</p>
     */
    private long term;

    /**
     * 投票响应节点 ID。
     *
     * <p>Candidate 根据该字段区分不同投票节点，避免重复统计同一节点的投票结果。</p>
     */
    private String voterId;

    /**
     * 是否同意投票给该 Candidate。
     *
     * <p>true 表示投票节点已经授票；Candidate 收到多数派同意后可以切换为 Leader。</p>
     */
    private boolean voteGranted;
}