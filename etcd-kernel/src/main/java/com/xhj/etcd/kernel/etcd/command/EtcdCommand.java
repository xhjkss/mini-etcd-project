package com.xhj.etcd.kernel.etcd.command;

import lombok.Data;

import java.io.Serializable;

/**
 * EtcdCommand
 *
 * @author XJks
 * @description Etcd 命令信封，用于封装需要进入 Raft 日志顺序流的命令类型、命令 ID 和命令数据对象。
 *
 * <p>
 * TODO:
 *  该类是 Etcd 层提交给 Raft 层复制的命令边界对象。
 *  EtcdCommand.data 在 Etcd 层可以直接保存 PutRequest、DeleteRequest、GetRequest、RangeRequest、DeleteRangeRequest 等对象；
 *  但 EtcdCommand 进入 RaftLogEntry 前必须整体序列化为 byte[]，Raft 层只复制和提交这段字节，不解析业务语义。
 * </p>
 *
 * <p>交互流程：</p>
 * <p>1) etcd-event-loop 从 EtcdEvent 中取出 XxxRequest；</p>
 * <p>2) 如果该请求需要线性一致处理，则封装为 EtcdCommand；</p>
 * <p>3) EtcdCommand 整体序列化后提交给 RaftNode；</p>
 * <p>4) 日志 committed 后，RaftApplyMessage 把原始 commandData 交回 EtcdNode；</p>
 * <p>5) EtcdNode 反序列化 EtcdCommand，并根据 type 将 data 强转为对应 XxxRequest 执行。</p>
 */
@Data
public class EtcdCommand implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * 命令类型。
     */
    private EtcdCommandType type;

    /**
     * 命令唯一标识。
     *
     * <p>该字段由 EtcdNode 内部生成，不允许用户请求传入。
     * 它用于关联 propose 阶段、apply 阶段和 RPC 等待结果，并避免同一个 logIndex 被不同 Leader 重写时错误唤醒旧请求。</p>
     */
    private String commandId;

    /**
     * 命令数据对象。
     *
     * <p>当前阶段直接保存 XxxRequest 对象,该字段只在 EtcdCommand 内部表达业务载荷；EtcdCommand 写入 Raft 日志前会整体序列化。</p>
     */
    private Object data;
}
