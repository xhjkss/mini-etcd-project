package com.xhj.etcd.kernel.raft.core;

import lombok.Data;

/**
 * RaftProposeResult
 *
 * @author XJks
 * @description RaftProposeResult 表示一次 propose 操作的结果。
 */
@Data
public class RaftProposeResult {

    /**
     * 是否被 leader 接受。
     */
    private boolean accepted;

    /**
     * 如果被接受，记录该日志条目的索引。
     */
    private long logIndex;

    /**
     * 当前 term。
     */
    private long currentTerm;

    /**
     * leader 节点 ID，如果不是 leader 则为空。
     */
    private String leaderId;

    /**
     * 错误信息。
     */
    private String errorMessage;

    private RaftProposeResult() {
    }

    /**
     * 构造一个被接受的 propose 结果。
     */
    public static RaftProposeResult accepted(long logIndex, long currentTerm) {
        RaftProposeResult result = new RaftProposeResult();
        result.setAccepted(true);
        result.setLogIndex(logIndex);
        result.setCurrentTerm(currentTerm);
        return result;
    }

    /**
     * 构造一个非 leader 的 propose 结果。
     */
    public static RaftProposeResult notLeader(String leaderId) {
        RaftProposeResult result = new RaftProposeResult();
        result.setAccepted(false);
        result.setLeaderId(leaderId);
        result.setErrorMessage("not leader");
        return result;
    }
}
