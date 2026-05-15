package com.xhj.etcd.kernel.etcd.etcdrpc;

import lombok.Data;

import java.io.Serializable;

/**
 * LeaseTtlResponse
 *
 * @author XJks
 * @description Lease TTL 查询响应。
 */
@Data
public class LeaseTtlResponse implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * Lease 视图。
     */
    private LeaseView lease;

    /**
     * 构造 Lease TTL 响应。
     */
    public static LeaseTtlResponse of(LeaseView lease) {
        LeaseTtlResponse response = new LeaseTtlResponse();
        response.setLease(lease);
        return response;
    }
}
