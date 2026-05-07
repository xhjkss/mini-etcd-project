package com.xhj.etcd.kernel.raft.apply;

import lombok.Data;

import java.io.Serializable;

/**
 * RaftApplyMessage
 *
 * @author XJks
 * @description Raft 状态机 apply 消息，用于封装已经 committed、等待应用到上层状态机的日志命令。
 *
 * <p>TODO: 该类是 Raft 层向 Etcd 状态机层输出 committed 日志的边界对象。
 * RaftNode 只负责判断哪些日志已经 committed，并将日志中的 commandData 透传给 EtcdNode；
 * EtcdNode 再负责反序列化 EtcdCommand，并 apply 到具体状态机。</p>
 *
 * <p>职责边界：</p>
 * <ul>
 *     <li>只表达 Raft 日志已经提交、可以交给上层状态机 apply。</li>
 *     <li>不负责执行命令。</li>
 *     <li>不负责持久化日志。</li>
 *     <li>不解析 commandData 的具体业务语义。</li>
 * </ul>
 */
@Data
public class RaftApplyMessage implements Serializable {

    /**
     * 是否携带有效的日志命令。
     *
     * <p>true 表示该消息来自一条已经 committed 的普通日志，EtcdNode 可以读取 commandData 并 apply 到状态机。</p>
     *
     * <p>该字段不表示命令业务执行是否成功。
     * 命令是否执行成功由 EtcdNode apply 后生成的 EtcdCommandApplyResult 表达。</p>
     *
     * <p>当前 snapshot apply 通过 RaftReady.snapshotToApply 单独传递，
     * 不通过该字段表达。</p>
     */
    private boolean commandValid;

    /**
     * 已提交日志的索引位置。
     *
     * <p>EtcdNode apply committedEntries 时应按 logIndex 顺序处理，
     * 并可以记录已经 apply 到的最新日志位置，用于状态机恢复或快照生成。</p>
     */
    private long logIndex;

    /**
     * 日志条目承载的命令数据。
     *
     * <p>该字段是 Raft 层透传的序列化命令字节。
     * 通常由 EtcdNode 在 propose 阶段将 EtcdCommand 序列化得到；
     * 日志 committed 后，RaftNode 再通过该字段把原始命令字节交回 EtcdNode apply。</p>
     */
    private byte[] commandData;

    /**
     * 构造命令类型的 apply 消息。
     *
     * <p>该方法用于 Raft 日志提交后，将日志索引和命令数据转换为上层状态机可消费的 apply 消息。</p>
     *
     * @param logIndex    已提交日志的索引位置
     * @param commandData 日志条目承载的命令数据
     * @return Raft apply 消息
     */
    public static RaftApplyMessage command(long logIndex, byte[] commandData) {
        RaftApplyMessage message = new RaftApplyMessage();
        message.commandValid = true;
        message.logIndex = logIndex;
        message.commandData = commandData;
        return message;
    }
}