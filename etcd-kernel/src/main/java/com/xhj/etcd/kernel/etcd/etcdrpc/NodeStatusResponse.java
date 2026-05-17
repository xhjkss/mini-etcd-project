package com.xhj.etcd.kernel.etcd.etcdrpc;

import com.xhj.etcd.kernel.raft.core.RaftRoleType;
import lombok.Data;

import java.io.Serializable;

/**
 * NodeStatusResponse
 *
 * @author XJks
 * @description 节点诊断响应。
 *
 * <p>该响应只返回当前节点的本地运行态，不参与 Raft 共识。</p>
 */
@Data
public class NodeStatusResponse implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * 当前节点 ID。
     */
    private String nodeId;

    /**
     * 当前 Raft 角色。
     */
    private RaftRoleType role;

    /**
     * 当前任期。
     */
    private long currentTerm;

    /**
     * 当前已知 leader 节点 ID。
     */
    private String leaderId;

    /**
     * 当前提交索引。
     */
    private long commitIndex;

    /**
     * 当前已 apply 的最后一条日志索引。
     */
    private long lastAppliedIndex;

    /**
     * 当前 MVCC revision。
     */
    private long currentRevision;

    /**
     * 当前 compact revision。
     */
    private long compactRevision;

    /**
     * 当前可见 key 数量。
     */
    private int keyCount;

    /**
     * 当前 lease 数量。
     */
    private int leaseCount;

    /**
     * 当前 watch 会话数量。
     */
    private int watchCount;

    /**
     * 最新快照覆盖的日志 index。
     */
    private long snapshotLastIncludedIndex;

    /**
     * 最新快照覆盖的日志 term。
     */
    private long snapshotLastIncludedTerm;

    /**
     * 构造 NodeStatus 响应。
     */
    public static NodeStatusResponse of(String nodeId,
                                        RaftRoleType role,
                                        long currentTerm,
                                        String leaderId,
                                        long commitIndex,
                                        long lastAppliedIndex,
                                        long currentRevision,
                                        long compactRevision,
                                        int keyCount,
                                        int leaseCount,
                                        int watchCount,
                                        long snapshotLastIncludedIndex,
                                        long snapshotLastIncludedTerm) {
        NodeStatusResponse response = new NodeStatusResponse();
        response.setNodeId(nodeId);
        response.setRole(role);
        response.setCurrentTerm(currentTerm);
        response.setLeaderId(leaderId);
        response.setCommitIndex(commitIndex);
        response.setLastAppliedIndex(lastAppliedIndex);
        response.setCurrentRevision(currentRevision);
        response.setCompactRevision(compactRevision);
        response.setKeyCount(keyCount);
        response.setLeaseCount(leaseCount);
        response.setWatchCount(watchCount);
        response.setSnapshotLastIncludedIndex(snapshotLastIncludedIndex);
        response.setSnapshotLastIncludedTerm(snapshotLastIncludedTerm);
        return response;
    }
}
