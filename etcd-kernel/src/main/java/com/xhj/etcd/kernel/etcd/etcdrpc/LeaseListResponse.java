package com.xhj.etcd.kernel.etcd.etcdrpc;

import lombok.Data;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * LeaseListResponse
 *
 * @author XJks
 * @description Lease 列表响应。
 */
@Data
public class LeaseListResponse implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * Lease 视图列表。
     */
    private List<LeaseView> leases = new ArrayList<>();

    /**
     * 构造 Lease 列表响应。
     */
    public static LeaseListResponse of(List<LeaseView> leases) {
        LeaseListResponse response = new LeaseListResponse();
        if (leases != null) {
            response.setLeases(new ArrayList<>(leases));
        }
        return response;
    }
}
