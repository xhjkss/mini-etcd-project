package com.xhj.etcd.kernel.etcd.etcdrpc;

import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * LeaseGrantRequest
 *
 * @author XJks
 * @description Lease 发放请求。
 */
@Data
@NoArgsConstructor
public class LeaseGrantRequest implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * LeaseId，0 表示由服务端自动分配。
     */
    private long leaseId;

    /**
     * TTL 秒数。
     */
    private long ttlSeconds;

    public LeaseGrantRequest(long leaseId, long ttlSeconds) {
        this.leaseId = leaseId;
        this.ttlSeconds = ttlSeconds;
    }
}
