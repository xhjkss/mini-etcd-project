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
}
