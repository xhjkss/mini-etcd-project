package com.xhj.etcd.kernel.etcd.etcdrpc;

import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * LeaseKeepAliveRequest
 *
 * @author XJks
 * @description Lease 续租请求。
 */
@Data
@NoArgsConstructor
public class LeaseKeepAliveRequest implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * LeaseId。
     */
    private long leaseId;

    public LeaseKeepAliveRequest(long leaseId) {
        this.leaseId = leaseId;
    }
}
