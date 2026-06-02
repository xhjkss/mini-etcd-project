package com.xhj.etcd.console.model.request.lease;

import lombok.Data;

/**
 * LeaseSessionStartRequest
 *
 * @author XJks
 * @description 复用已有 leaseId 启动 KeepAlive 会话请求。
 */
@Data
public class LeaseSessionStartRequest {

    /**
     * 目标 leaseId。
     */
    private long leaseId;
}
