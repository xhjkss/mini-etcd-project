package com.xhj.etcd.kernel.etcd.etcdrpc;

import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * LeaseTtlRequest
 *
 * @author XJks
 * @description Lease TTL 查询请求。
 */
@Data
@NoArgsConstructor
public class LeaseTtlRequest implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * LeaseId。
     */
    private long leaseId;

    public LeaseTtlRequest(long leaseId) {
        this.leaseId = leaseId;
    }
}
