package com.xhj.etcd.kernel.raft.storage;

import com.xhj.etcd.kernel.raft.core.RaftHardState;
import com.xhj.etcd.kernel.raft.log.RaftLogEntry;
import com.xhj.etcd.kernel.raft.snapshot.RaftSnapshot;
import lombok.Data;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * RaftPersistentState
 *
 * @author XJks
 * @description mini etcd 当前阶段的 Raft 持久状态聚合对象，统一保存 HardState、Snapshot、日志条目和状态机 apply 边界。
 */
@Data
public class RaftPersistentState implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * Raft HardState。
     *
     * <p>用于恢复 currentTerm / votedFor，避免节点重启后在同一任期重复投票。</p>
     */
    private RaftHardState hardState;

    /**
     * 最新 Raft 快照。
     *
     * <p>快照同时包含 Raft 日志压缩边界和上层状态机快照数据。</p>
     */
    private RaftSnapshot snapshot;

    /**
     * 快照边界之后仍需保留的日志条目。
     *
     * <p>如果 snapshot 存在，该列表只保存 lastIncludedIndex 之后的日志。</p>
     */
    private List<RaftLogEntry> entries = new ArrayList<>();

    /**
     * Etcd 状态机已经 apply 到的最高 Raft 日志 index。
     *
     * <p>启动恢复时，EtcdNode 会先用 snapshot.stateMachineData 恢复快照基线，
     * 再重放该 index 之前的持久化日志，使内存 KV Map 恢复到上次已 apply 的位置。</p>
     */
    private long lastAppliedRaftLogIndex;
}
