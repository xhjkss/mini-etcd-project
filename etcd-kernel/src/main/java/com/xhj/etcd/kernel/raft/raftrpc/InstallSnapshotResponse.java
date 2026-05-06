package com.xhj.etcd.kernel.raft.raftrpc;

import lombok.Data;

import java.io.Serializable;

/**
 * InstallSnapshotResponse
 *
 * @author XJks
 * @description InstallSnapshot RPC 响应，用于 Follower 向 Leader 返回快照安装结果。
 */
@Data
public class InstallSnapshotResponse implements Serializable {

    /**
     * 响应节点当前任期。
     *
     * <p>Leader 收到更大 term 时需要更新本地任期并回退为 Follower；
     * 如果响应 term 小于 Leader 当前任期，通常会被视为过期响应。</p>
     */
    private long term;

    /**
     * 响应节点 ID。
     *
     * <p>Leader 根据该字段定位对应 Follower 的复制进度。</p>
     */
    private String followerId;

    /**
     * 是否成功安装快照。
     *
     * <p>true 表示 Follower 已经接受并安装该快照；
     * false 表示请求任期过期或快照边界不满足本地处理条件。</p>
     */
    private boolean success;

    /**
     * 成功安装的快照最后日志 index。
     *
     * <p>Leader 在收到成功响应后，可以据此更新该 Follower 的 matchIndex 和 nextIndex。</p>
     */
    private long lastIncludedIndex;
}