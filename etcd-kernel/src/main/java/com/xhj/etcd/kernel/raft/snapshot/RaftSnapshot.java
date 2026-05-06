package com.xhj.etcd.kernel.raft.snapshot;

import lombok.Data;

import java.io.Serializable;

/**
 * RaftSnapshot
 *
 * @author XJks
 * @description Raft 快照模型，记录日志压缩后的边界信息和上层状态机快照数据。
 *
 * <p>TODO: Raft 层只关心快照覆盖到哪个日志 index/term，不解析 stateMachineData 的具体内容；状态机数据的生成和恢复由上层 Etcd 状态机负责。</p>
 */
@Data
public class RaftSnapshot implements Serializable {

    /**
     * 快照覆盖的最后一条 Raft 日志 index。
     *
     * <p>该 index 及其之前的日志已经被快照覆盖，后续可以从 RaftLogState 中压缩删除。</p>
     */
    private long lastIncludedIndex;

    /**
     * lastIncludedIndex 对应日志的 term。
     *
     * <p>日志压缩后，原始日志条目可能已经不存在，因此需要保留该 term 作为后续日志匹配边界。</p>
     */
    private long lastIncludedTerm;

    /**
     * 上层状态机快照数据。
     *
     * <p>该字段由上层状态机序列化生成，Raft 层只负责持久化和传输；
     * 安装快照时再交给上层状态机恢复具体数据。</p>
     */
    private byte[] stateMachineData;
}