package com.xhj.etcd.console.model.response.lease;

import lombok.Data;

import java.util.ArrayList;
import java.util.List;

/**
 * LeaseSessionResponse
 *
 * @author XJks
 * @description LeaseHandle 会话视图。
 */
@Data
public class LeaseSessionResponse {

    /**
     * leaseId。
     */
    private long leaseId;

    /**
     * 会话是否活跃。
     */
    private boolean active;

    /**
     * TTL 秒数。
     */
    private long ttlSeconds;

    /**
     * 最近一次已知剩余 TTL 秒数。
     */
    private long remainingSeconds;

    /**
     * 最近一次已知绑定 key 列表。
     */
    private List<String> keys = new ArrayList<>();
}
