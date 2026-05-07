package com.xhj.etcd.kernel.server.etcdrpc;

import lombok.Data;

import java.io.Serializable;

/**
 * PutResponse
 *
 * @author XJks
 * @description 当前阶段临时 KV 服务的 PUT 响应体。
 *
 * <p>阶段边界：</p>
 * <p>当前临时 KV 服务只验证 PUT 请求经过 RPC、Raft propose、Raft apply 后成功写入状态机的闭环。
 * 由于当前阶段尚未实现 MVCC revision、prevKv、lease 等完整 etcd 语义，因此 PUT 响应体暂时不携带额外字段。</p>
 *
 * <p>说明：</p>
 * <p>请求是否成功、错误信息、notLeader 等通用响应信息统一由 EtcdRpcResponse.header 表达；
 * PutResponse 只保留 PUT 操作特有的响应字段。当前阶段没有 PUT 专属响应字段，因此该类为空响应体。</p>
 */
@Data
public class PutResponse implements Serializable {

    private static final long serialVersionUID = 1L;

    public PutResponse() {
    }

    /**
     * 构造 PUT 成功响应体。
     *
     * <p>当前阶段没有 revision 等额外返回字段，因此只返回一个空响应体。
     * 成功状态由外层 EtcdRpcResponse.header 表达。</p>
     *
     * @return PUT 响应体
     */
    public static PutResponse success() {
        return new PutResponse();
    }
}