package com.xhj.etcd.kernel.raft.event;

import lombok.Data;

import java.io.Serializable;

/**
 * RaftEvent
 *
 * @author XJks
 * @description Raft 内部事件信封，用于统一封装 RaftNode 事件循环需要处理的输入。
 *
 * <p>TODO: RaftEvent 只负责表达事件类型、事件标识和序列化后的事件数据；具体事件载荷由 RaftEventCodec 按 type 进行编解码，RaftEvent 本身不感知具体业务对象类型。</p>
 */
@Data
public class RaftEvent implements Serializable {

    /**
     * 事件类型。
     *
     * <p>RaftNode 事件循环根据该字段决定如何反序列化 eventData，以及分发到哪个处理逻辑。</p>
     */
    private RaftEventType type;

    /**
     * 事件唯一标识。
     *
     * <p>用于标识一次事件提交过程。对于需要返回处理结果的事件，eventId 可以作为请求方和事件循环之间关联 Future 或回调结果的标识。</p>
     */
    private String eventId;

    /**
     * 序列化后的事件数据。
     *
     * <p>不同事件类型对应不同的数据对象，例如 propose、advance、RPC 消息或创建快照事件；
     * 该字段只保存字节数据，具体类型由 RaftEventCodec 负责还原。</p>
     */
    private byte[] eventData;
}