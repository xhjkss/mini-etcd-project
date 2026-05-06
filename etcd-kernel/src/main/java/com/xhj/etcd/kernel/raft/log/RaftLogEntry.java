package com.xhj.etcd.kernel.raft.log;

import lombok.Data;

import java.io.Serializable;

/**
 * RaftLogEntry
 *
 * @author XJks
 * @description Raft 日志条目，记录日志索引、任期编号和上层命令数据。
 *
 * <p>TODO: RaftLogEntry 是 Raft 复制和提交的最小日志单元。
 * Raft 层只关心 index、term 和 commandData 的复制一致性，不解析 commandData 的业务语义。</p>
 */
@Data
public class RaftLogEntry implements Serializable {

    /**
     * 日志索引。
     *
     * <p>用于标识日志条目在 Raft 日志中的位置，Leader 和 Follower 通过 index 对齐复制进度。</p>
     */
    private long index;

    /**
     * 日志任期编号。
     *
     * <p>表示该日志条目被 Leader 创建时的 term，用于日志匹配、冲突检测和提交安全性判断。</p>
     */
    private long term;

    /**
     * 上层命令数据。
     *
     * <p>该字段通常由 EtcdNode 序列化具体命令得到；
     * 日志 committed 后，Raft 层会通过 RaftApplyMessage 将该字节数据交给上层状态机 apply。</p>
     */
    private byte[] commandData;
}