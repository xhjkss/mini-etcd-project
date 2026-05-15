package com.xhj.etcd.kernel.etcd.etcdrpc;

import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * LeaseRevokeRequest
 *
 * @author XJks
 * @description Lease 撤销请求。
 */
@Data
@NoArgsConstructor
public class LeaseRevokeRequest implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * LeaseId。
     */
    private long leaseId;

    public LeaseRevokeRequest(long leaseId) {
        this.leaseId = leaseId;
    }
}
