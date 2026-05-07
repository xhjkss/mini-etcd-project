package com.xhj.etcd.kernel.raft.core;

import com.xhj.etcd.kernel.raft.apply.RaftApplyMessage;
import com.xhj.etcd.kernel.raft.log.RaftLogEntry;
import com.xhj.etcd.kernel.raft.raftrpc.RaftRpcMessage;
import com.xhj.etcd.kernel.raft.snapshot.RaftSnapshot;
import lombok.Data;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * RaftReady
 *
 * @author XJks
 * @description Raft 就绪批次，封装 RaftNode 需要上层 EtcdNode 处理的状态变更、日志、快照、RPC 消息和 apply 消息。
 *
 * <p>TODO: RaftNode 只负责推进 Raft 协议状态，不直接执行持久化、网络发送和上层状态机 apply。这些副作用会通过 RaftReady 暴露给 EtcdNode，由 EtcdNode 处理完成后再通过 Advance 事件通知 RaftNode 清理暂存状态。</p>
 */
@Data
public class RaftReady implements Serializable {

    /**
     * 需要持久化的 Raft HardState。
     *
     * <p>通常包含 currentTerm、votedFor 等必须先于外部响应落盘的 Raft 持久状态。</p>
     */
    private RaftHardState hardStateToPersist;

    /**
     * 需要持久化的 Raft 日志条目。
     *
     * <p>Leader 本地提案产生的新日志、Follower 接收 Leader 复制过来的新日志，都会通过该列表交给 EtcdNode 落盘。</p>
     */
    private List<RaftLogEntry> entriesToPersist = new ArrayList<>();

    /**
     * 需要持久化的 Raft 快照。
     *
     * <p>该字段表示 Raft 层已经生成或接收到、需要写入本地存储的快照数据。</p>
     */
    private RaftSnapshot snapshotToPersist;

    /**
     * 需要应用到上层状态机的 Raft 快照。
     *
     * <p>该字段通常来自 InstallSnapshot 或本地恢复流程，EtcdNode 需要先用快照恢复状态机，再继续 apply 快照之后的 committed 日志。</p>
     */
    private RaftSnapshot snapshotToApply;

    /**
     * 需要发送给其他 Raft 节点的 RPC 消息。
     *
     * <p>包括 RequestVote、AppendEntries、InstallSnapshot 及其响应消息。
     * EtcdNode 负责将这些消息转换为实际 RPC 调用并发送给目标节点。</p>
     */
    private List<RaftRpcMessage> messagesToSend = new ArrayList<>();

    /**
     * 已经 committed、需要应用到上层状态机的日志命令。
     *
     * <p>EtcdNode 应按列表顺序 apply 这些消息，确保状态机推进顺序与 Raft commit 顺序一致。</p>
     */
    private List<RaftApplyMessage> committedEntries = new ArrayList<>();

    /**
     * 是否请求上层创建状态机快照。
     *
     * <p>该字段只表示 RaftNode 已经判断当前日志数量达到快照阈值；真正的状态机快照数据仍由 EtcdNode 生成。
     * EtcdNode 处理该请求后，会通过 submitRaftCreateSnapshotEvent 将快照数据回传给 RaftNode。</p>
     */
    private boolean snapshotCreateRequested;
}