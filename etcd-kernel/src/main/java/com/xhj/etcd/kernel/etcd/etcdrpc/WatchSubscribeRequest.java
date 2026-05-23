package com.xhj.etcd.kernel.etcd.etcdrpc;

import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * WatchSubscribeRequest
 *
 * @author XJks
 * @description Watch 订阅请求。
 */
@Data
@NoArgsConstructor
public class WatchSubscribeRequest implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * 起始 key。
     */
    private String startKey;

    /**
     * 区间结束 key（左闭右开）。
     *
     * <p>当 prefixMatch=true 时，该字段会被服务端忽略，由 startKey 计算前缀结束边界。</p>
     */
    private String endKeyExclusive;

    /**
     * 是否按前缀匹配。
     */
    private boolean prefixMatch;

    /**
     * Watch 起始 revision。
     *
     * <p>0 表示从“当前最新 revision 的下一条事件”开始追踪。</p>
     */
    private long startRevision;

    /**
     * 创建时回放事件的最大条数。
     *
     * <p>0 表示不限制。</p>
     */
    private int maxEvents;

    /**
     * 是否要求由 Leader 处理订阅握手。
     *
     * <p>
     * TODO:
     *  true 时，Follower 会返回 notLeader + leaderId，客户端按 leaderId 跳转重试；
     *  false 时，允许在任意节点建立 watch，会话由该节点负责推送已提交事件。
     * </p>
     */
    private boolean leaderOnly;
}
