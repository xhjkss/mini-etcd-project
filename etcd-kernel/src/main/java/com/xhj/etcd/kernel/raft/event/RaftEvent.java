package com.xhj.etcd.kernel.raft.event;

import lombok.Data;

import java.io.Serializable;

/**
 * RaftEvent
 *
 * @author XJks
 * @description RaftNode 内部事件信封，用于统一封装 raft-event-loop 需要处理的输入。
 *
 * <p>
 * TODO:
 *  RaftEvent 只在 RaftNode 当前 JVM 内部事件队列中流转，不进入网络、日志或磁盘，
 *  因此 data 可以直接保存具体事件对象，避免为 PROPOSE / ADVANCE 这类事件额外创建无意义的套壳对象。
 * </p>
 *
 * <p>data 常见类型：</p>
 * <ul>
 *     <li>PROPOSE：上层命令字节 byte[]。</li>
 *     <li>REQUEST_VOTE / APPEND_ENTRIES / INSTALL_SNAPSHOT：对应 Raft RPC 请求或响应对象。</li>
 *     <li>ADVANCE：EtcdNode 已处理完成的 RaftReady。</li>
 *     <li>CREATE_SNAPSHOT：RaftCreateSnapshotEventData。</li>
 * </ul>
 */
@Data
public class RaftEvent implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * 事件类型。
     */
    private RaftEventType type;

    /**
     * 事件唯一标识。
     *
     * <p>PROPOSE 事件需要通过该字段关联等待中的 propose future；其他无需响应的事件可以为空。</p>
     */
    private String eventId;

    /**
     * 事件数据对象。
     *
     * <p>该对象只在 RaftNode 内部事件循环流转，不做序列化；真正需要持久化和复制的是 RaftLogEntry.commandData。</p>
     */
    private Object data;
}