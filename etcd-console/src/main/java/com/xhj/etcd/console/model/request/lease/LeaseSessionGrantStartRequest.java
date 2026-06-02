package com.xhj.etcd.console.model.request.lease;

import lombok.Data;

/**
 * LeaseSessionGrantStartRequest
 *
 * @author XJks
 * @description grant 并启动 KeepAlive 会话请求。
 */
@Data
public class LeaseSessionGrantStartRequest {

    /**
     * 可选 leaseId。
     *
     * <p>小于等于 0 时由服务端自动分配。</p>
     */
    private long leaseId;

    /**
     * TTL 秒数。
     */
    private long ttlSeconds;
}
