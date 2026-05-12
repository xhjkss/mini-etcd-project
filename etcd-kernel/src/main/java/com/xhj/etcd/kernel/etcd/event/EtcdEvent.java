package com.xhj.etcd.kernel.etcd.event;

import lombok.Data;

import java.io.Serializable;

/**
 * EtcdEvent
 *
 * @author XJks
 * @description EtcdNode 内部事件信封，用于把 RPC 线程收到的请求投递到 etcd-event-loop 串行处理。
 *
 * <p>
 * TODO:
 *  该类是 RPC 层进入 EtcdNode 主流程的边界对象。
 *  用户 RPC 请求不能直接构造 EtcdCommand 投递到 Raft；必须先封装为 EtcdEvent，
 *  由 etcd-event-loop 统一判断该事件是本地处理，还是需要转换为 EtcdCommand 进入 Raft。
 * </p>
 *
 * <p>设计边界：</p>
 * <ul>
 *     <li>EtcdEvent 只在当前 JVM 内部事件队列中流转，不进入网络、Raft 日志或磁盘持久化。</li>
 *     <li>data 可以直接保存 PutRequest、DeleteRequest、GetRequest、RangeRequest、DeleteRangeRequest 等请求对象。</li>
 *     <li>需要进入 Raft 日志的请求，会在 etcd-event-loop 中再转换为 EtcdCommand，并在提交 Raft 前序列化。</li>
 * </ul>
 */
@Data
public class EtcdEvent implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * 事件类型。
     *
     * <p>etcd-event-loop 根据该字段判断 data 应该按哪种请求类型读取，并决定后续处理路径。</p>
     */
    private EtcdEventType type;

    /**
     * 事件 ID。
     *
     * <p>需要等待 RPC 响应的事件会设置该字段，EtcdNode 通过 eventId 找回 pending future 并回填结果。</p>
     */
    private String eventId;

    /**
     * 事件数据对象。
     *
     * <p>该字段只保存 JVM 内部对象，不做序列化。当前阶段通常是 XxxRequest 请求对象。
     * 如果事件需要进入 Raft，EtcdNode 会将该对象放入 EtcdCommand.data，再把 EtcdCommand 整体序列化为 Raft 日志命令字节。</p>
     */
    private Object data;
}
