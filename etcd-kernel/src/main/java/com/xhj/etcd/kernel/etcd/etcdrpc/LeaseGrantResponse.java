package com.xhj.etcd.kernel.etcd.etcdrpc;

import lombok.Data;

import java.io.Serializable;

/**
 * LeaseGrantResponse
 *
 * @author XJks
 * @description Lease 发放响应。
 */
@Data
public class LeaseGrantResponse implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * Lease 视图。
     */
    private LeaseView lease;

    /**
     * 构造 Lease 发放响应。
     */
    public static LeaseGrantResponse of(LeaseView lease) {
        LeaseGrantResponse response = new LeaseGrantResponse();
        response.setLease(lease);
        return response;
    }
}
