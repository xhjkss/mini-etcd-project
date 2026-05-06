package com.xhj.etcd.kernel.raft.event;

import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * RaftProposeEventData
 *
 * @author XJks
 * @description Raft 提案事件数据，承载上层提交给 RaftNode 的命令字节。
 *
 * <p>TODO: 该类只负责把上层命令作为字节数据透传给 Raft 层；RaftNode 不解析命令语义，只负责将其包装为日志条目并参与复制和提交。</p>
 */
@Data
@NoArgsConstructor
public class RaftProposeEventData implements Serializable {

    /**
     * 上层命令数据。
     *
     * <p>该字段通常由 EtcdNode 将具体业务命令序列化得到；
     * 日志 committed 后，Raft 层会通过 RaftApplyMessage 将该字节数据再交回上层状态机 apply。</p>
     */
    private byte[] commandData;
}