package com.xhj.etcd.kernel.etcd.etcdrpc;

import lombok.Data;

import java.io.Serializable;

/**
 * LeaseRevokeResponse
 *
 * @author XJks
 * @description Lease 撤销响应。
 */
@Data
public class LeaseRevokeResponse implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * 被撤销的 leaseId。
     */
    private long leaseId;

    /**
     * 被删除的 key 数量。
     */
    private int deletedCount;

    /**
     * 撤销发生的 revision。
     */
    private long revision;

    /**
     * 构造 Lease 撤销响应。
     */
    public static LeaseRevokeResponse of(long leaseId, int deletedCount, long revision) {
        LeaseRevokeResponse response = new LeaseRevokeResponse();
        response.setLeaseId(leaseId);
        response.setDeletedCount(deletedCount);
        response.setRevision(revision);
        return response;
    }
}
