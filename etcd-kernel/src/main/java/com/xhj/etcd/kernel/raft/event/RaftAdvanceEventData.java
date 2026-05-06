package com.xhj.etcd.kernel.raft.event;

import com.xhj.etcd.kernel.raft.core.RaftReady;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * RaftAdvanceEventData
 *
 * @author XJks
 * @description Raft Ready 推进事件数据，承载已经被上层处理完成的 RaftReady。
 *
 * <p>TODO: Advance 事件表示上层已经完成当前 Ready 中的持久化、消息发送和 apply 处理，RaftNode 收到该事件后才可以安全清理对应的暂存状态。</p>
 */
@Data
@NoArgsConstructor
public class RaftAdvanceEventData implements Serializable {

    /**
     * 需要推进的 RaftReady。
     *
     * <p>该对象用于让 RaftNode 确认本轮 Ready 中哪些 hardState、日志条目、RPC 消息和 committed entries 已经被上层消费完成。</p>
     */
    private RaftReady raftReady;
}