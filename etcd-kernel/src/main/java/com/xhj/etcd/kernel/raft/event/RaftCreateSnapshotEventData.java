package com.xhj.etcd.kernel.raft.event;

import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * RaftCreateSnapshotEventData
 *
 * @author XJks
 * @description Raft 创建快照事件数据，承载上层状态机快照内容和快照覆盖的日志边界。
 *
 * <p>TODO: 快照数据由上层状态机生成，Raft 层只关心该快照覆盖到哪个日志 index，并据此压缩对应范围内的 Raft 日志。</p>
 */
@Data
@NoArgsConstructor
public class RaftCreateSnapshotEventData implements Serializable {

    /**
     * 快照覆盖的最后一条日志索引。
     *
     * <p>Raft 日志中小于等于该 index 的条目可以在快照持久化完成后被压缩；
     * 后续节点恢复时，需要从该边界之后继续回放日志。</p>
     */
    private long lastIncludedIndex;

    /**
     * 状态机快照数据。
     *
     * <p>该字段由上层状态机序列化生成，Raft 层不解析具体内容，只负责将其作为快照载荷持久化和传输。</p>
     */
    private byte[] stateMachineData;
}