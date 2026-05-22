package com.xhj.etcd.console.model.response.watch;

import lombok.Data;

/**
 * WatchSessionResponse
 *
 * @author XJks
 * @description watch 会话响应。
 */
@Data
public class WatchSessionResponse {
    /**
     * 节点 ID。
     */
    private String nodeId;

    /**
     * 订阅 key 或前缀起点。
     */
    private String key;

    /**
     * 是否按前缀订阅。
     */
    private boolean prefix;

    /**
     * watchId。
     */
    private long watchId;

    /**
     * 会话是否活跃。
     */
    private boolean active;
}
