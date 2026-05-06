package com.xhj.etcd.kernel.raft.raftrpc;

import lombok.Data;

import java.io.Serializable;

/**
 * RequestVoteRequest
 *
 * @author XJks
 * @description RequestVote RPC 请求，用于 Candidate 在选举期间向其他节点请求投票。
 */
@Data
public class RequestVoteRequest implements Serializable {

    /**
     * Candidate 当前任期。
     *
     * <p>接收节点会根据该字段判断请求是否过期；
     * 如果请求 term 小于本地 currentTerm，则应拒绝投票。</p>
     */
    private long term;

    /**
     * 请求投票的 Candidate 节点 ID。
     *
     * <p>接收节点在同一任期内只能投票给一个 Candidate，
     * 因此会结合 term 和 candidateId 判断是否可以授票。</p>
     */
    private String candidateId;

    /**
     * Candidate 最后一条日志的 index。
     *
     * <p>接收节点会结合 lastLogIndex 和 lastLogTerm 判断 Candidate 的日志是否至少和自己一样新。</p>
     */
    private long lastLogIndex;

    /**
     * Candidate 最后一条日志的 term。
     *
     * <p>Raft 投票时优先比较最后日志 term；term 相同时，再比较最后日志 index。</p>
     */
    private long lastLogTerm;
}