package com.xhj.etcd.kernel.raft.apply;

import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * RaftApplyMessage
 *
 * @author XJks
 * @description Raft 状态机 apply 消息，用于封装已经 committed、等待应用到上层状态机的日志命令。
 *
 * <p>TODO 高亮：该类只表达 Raft 日志提交后的 apply 语义，不负责执行命令、不负责持久化日志，
 * 也不感知上层状态机的具体业务类型。</p>
 */
@Data
@NoArgsConstructor
public class RaftApplyMessage {

    /**
     * 已提交日志的索引位置。
     *
     * <p>上层状态机可以通过该索引保证 apply 顺序，或记录已经应用到的最新日志位置。</p>
     */
    private long index;

    /**
     * 日志条目承载的命令数据。
     *
     * <p>该字段是 Raft 层透传的序列化命令字节，具体反序列化和业务执行由上层状态机负责。</p>
     */
    private byte[] commandData;

    /**
     * 构造命令类型的 apply 消息。
     *
     * <p>该方法用于 Raft 提交日志后，将日志索引和命令数据转换为状态机可消费的消息对象。</p>
     *
     * @param index       已提交日志的索引位置
     * @param commandData 日志条目承载的命令数据
     * @return Raft apply 消息
     */
    public static RaftApplyMessage command(long index, byte[] commandData) {
        RaftApplyMessage message = new RaftApplyMessage();
        message.setIndex(index);
        message.setCommandData(commandData);
        return message;
    }
}