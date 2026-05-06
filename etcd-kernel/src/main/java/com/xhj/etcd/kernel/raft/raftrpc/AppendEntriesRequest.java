package com.xhj.etcd.kernel.raft.raftrpc;

import com.xhj.etcd.kernel.raft.log.RaftLogEntry;
import lombok.Data;

import java.io.Serializable;
import java.util.List;

/**
 * AppendEntriesRequest
 *
 * @author XJks
 * @description AppendEntries RPC 请求，用于 Leader 向 Follower 发送心跳、复制日志并推进提交进度。
 *
 * <p>TODO: AppendEntries 同时承担心跳和日志复制两类语义；
 * entries 为空时通常表示心跳，entries 非空时表示从 prevLogIndex 之后开始复制日志。</p>
 */
@Data
public class AppendEntriesRequest implements Serializable {

    /**
     * Leader 当前任期。
     *
     * <p>Follower 收到更大 term 时需要更新本地任期并回退为 Follower；
     * 如果请求 term 小于本地 currentTerm，则应拒绝该请求。</p>
     */
    private long term;

    /**
     * Leader 节点 ID。
     *
     * <p>Follower 可通过该字段记录当前任期的 Leader，用于重置选举计时和后续重定向写请求。</p>
     */
    private String leaderId;

    /**
     * 待复制日志前一条日志的 index。
     *
     * <p>Follower 会使用 prevLogIndex 和 prevLogTerm 校验本地日志是否与 Leader 对齐；
     * 只有前置日志匹配时，才会继续追加 entries 中的日志条目。</p>
     */
    private long prevLogIndex;

    /**
     * 待复制日志前一条日志的 term。
     *
     * <p>该字段和 prevLogIndex 一起用于日志匹配检查，避免不同任期的日志在同一 index 上被错误接续。</p>
     */
    private long prevLogTerm;

    /**
     * Leader 要复制给 Follower 的日志条目列表。
     *
     * <p>列表为空时表示本次请求主要用于心跳保活或推进 leaderCommit；
     * 列表非空时，Follower 会从 prevLogIndex 之后开始进行冲突检测和日志追加。</p>
     */
    private List<RaftLogEntry> entries;

    /**
     * Leader 已知已经提交的日志 index。
     *
     * <p>Follower 在日志匹配成功后，会根据该字段推进本地 commitIndex，
     * 但不能超过自己最后一条日志的 index。</p>
     */
    private long leaderCommit;
}