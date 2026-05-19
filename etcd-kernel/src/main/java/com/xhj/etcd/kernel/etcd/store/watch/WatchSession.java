package com.xhj.etcd.kernel.etcd.store.watch;

import io.netty.channel.Channel;
import lombok.Data;

import java.io.Serializable;

/**
 * WatchSession
 *
 * @author XJks
 * @description Watch 会话记录。
 */
@Data
public class WatchSession implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * Watch 会话 ID。
     */
    private long watchId;

    /**
     * 起始 key。
     */
    private String startKey;

    /**
     * 区间结束 key（左闭右开）。
     */
    private String endKeyExclusive;

    /**
     * 是否前缀匹配。
     */
    private boolean prefixMatch;

    /**
     * 下一次事件读取的起始 revision（含）。
     */
    private long nextRevision;

    /**
     * 绑定的订阅 channel。
     */
    private transient Channel channel;

    /**
     * 绑定的 rpcMessageId。
     */
    private String rpcMessageId;

    /**
     * 是否允许发送通知推送。
     *
     * <p>
     * TODO:
     *  subscribe 会话创建后先处于未激活状态，只有当 subscribe 响应首帧真正写回成功后，
     *  才将该标记切换为 true，避免“推送先于响应”导致客户端阶段错乱。
     * </p>
     */
    private boolean notificationPushEnabled;
}
