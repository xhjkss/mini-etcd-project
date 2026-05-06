package com.xhj.etcd.kernel.raft.core;

import lombok.Data;

import java.util.ArrayList;
import java.util.List;

/**
 * RaftConfig
 *
 * @author XJks
 * @description Raft 节点配置，描述当前节点的集群成员视图和 tick 驱动的超时参数。
 */
@Data
public class RaftConfig {

    /**
     * 当前节点之外的其他 Raft 节点 ID 列表。
     *
     * <p>当前节点自身 ID 不放入该列表；RaftNode 会基于自身节点 ID 和 peerNodeIds
     * 共同计算集群规模、投票多数派和日志复制目标节点。</p>
     */
    private List<String> peerNodeIds = new ArrayList<>();

    /**
     * 选举超时 tick 数。
     *
     * <p>Follower 或 Candidate 连续经过该数量的 tick 仍未收到有效 Leader 消息时，
     * 会进入新一轮选举流程。</p>
     */
    private int electionTimeoutTicks = 10;

    /**
     * 心跳超时 tick 数。
     *
     * <p>Leader 每经过该数量的 tick 会向其他节点发送一次 AppendEntries 心跳，
     * 用于维持 Leader 身份并重置 Follower 的选举计时。</p>
     */
    private int heartbeatTimeoutTicks = 3;
}