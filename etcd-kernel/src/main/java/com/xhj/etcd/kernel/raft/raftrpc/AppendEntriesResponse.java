package com.xhj.etcd.kernel.raft.raftrpc;

import lombok.Data;

import java.io.Serializable;

/**
 * AppendEntriesResponse
 *
 * @author XJks
 * @description AppendEntries RPC 响应，用于 Follower 向 Leader 返回心跳或日志复制处理结果。
 *
 * <p>TODO: Leader 会根据 success、matchIndex 和 rejectHint 更新 Follower 的复制进度；
 * 成功时推进 matchIndex，失败时回退 nextIndex 后重新复制。</p>
 */
@Data
public class AppendEntriesResponse implements Serializable {

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
     * <p>Leader 根据该字段定位对应 Follower 的复制进度，例如 nextIndex 和 matchIndex。</p>
     */
    private String followerId;

    /**
     * 日志复制或心跳处理是否成功。
     *
     * <p>true 表示 Follower 的 prevLogIndex / prevLogTerm 校验通过，并已经接受本次请求；
     * false 表示前置日志不匹配或请求任期过期。</p>
     */
    private boolean success;

    /**
     * Follower 当前与 Leader 匹配的最新日志 index。
     *
     * <p>复制成功时，Leader 会使用该字段推进对应 Follower 的 matchIndex，
     * 并尝试基于多数派 matchIndex 推进 commitIndex。</p>
     */
    private long matchIndex;

    /**
     * 拒绝复制时的回退提示 index。
     *
     * <p>复制失败时，Follower 可以通过该字段提示 Leader 下次从哪个 index 附近重新尝试复制，
     * 避免 Leader 每次只回退一条日志导致恢复速度过慢。</p>
     */
    private long rejectHint;
}