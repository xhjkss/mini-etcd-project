package com.xhj.etcd.kernel.etcd.etcdrpc;

import lombok.Data;

import java.io.Serializable;

/**
 * LeaseKeepAliveResponse
 *
 * @author XJks
 * @description Lease 续租响应。
 */
@Data
public class LeaseKeepAliveResponse implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * Lease 视图。
     */
    private LeaseView lease;

    /**
     * 构造 Lease 续租响应。
     */
    public static LeaseKeepAliveResponse of(LeaseView lease) {
        LeaseKeepAliveResponse response = new LeaseKeepAliveResponse();
        response.setLease(lease);
        return response;
    }
}
