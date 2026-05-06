package com.xhj.etcd.kernel.raft.raftrpc;

import lombok.Data;

import java.io.Serializable;

/**
 * InstallSnapshotRequest
 *
 * @author XJks
 * @description InstallSnapshot RPC 请求，用于 Leader 向落后过多的 Follower 发送快照数据。
 */
@Data
public class InstallSnapshotRequest implements Serializable {

    /**
     * Leader 当前任期。
     *
     * <p>Follower 收到更大 term 时需要更新本地任期；如果请求 term 小于本地 currentTerm，
     * 则应拒绝该快照安装请求。</p>
     */
    private long term;

    /**
     * Leader 节点 ID。
     *
     * <p>Follower 可通过该字段记录当前任期的 Leader。</p>
     */
    private String leaderId;

    /**
     * 快照覆盖的最后一条日志 index。
     *
     * <p>Follower 安装快照后，该 index 及其之前的日志都可以视为已经被快照覆盖。</p>
     */
    private long lastIncludedIndex;

    /**
     * 快照覆盖的最后一条日志 term。
     *
     * <p>该字段和 lastIncludedIndex 一起作为安装快照后的日志匹配边界。</p>
     */
    private long lastIncludedTerm;

    /**
     * Leader 已知已经提交的日志 index。
     *
     * <p>Follower 安装快照后，会结合该字段推进本地 commitIndex。</p>
     */
    private long leaderCommit;

    /**
     * 快照数据。
     *
     * <p>该字段由上层状态机生成并序列化，Follower 安装快照时会交给上层状态机恢复状态。</p>
     */
    private byte[] snapshotData;
}