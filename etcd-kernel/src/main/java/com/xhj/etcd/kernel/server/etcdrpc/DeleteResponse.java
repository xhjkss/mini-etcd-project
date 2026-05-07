package com.xhj.etcd.kernel.server.etcdrpc;

import lombok.Data;

import java.io.Serializable;

/**
 * DeleteResponse
 *
 * @author XJks
 * @description 当前阶段临时 KV 服务的 DELETE 响应体，用于返回实际删除的 key 数量。
 *
 * <p>阶段边界：</p>
 * <p>当前临时 KV 服务只支持按精确 key 删除，因此响应体只保留当前阶段真实产生的 deletedKeys。</p>
 *
 * <p>说明：</p>
 * <p>请求是否成功、错误信息、notLeader 等通用响应信息统一由 EtcdRpcResponse.header 表达；
 * DeleteResponse 只保留 DELETE 操作特有的响应字段。</p>
 */
@Data
public class DeleteResponse implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * 本次 DELETE 操作实际删除的 key 数量。
     *
     * <p>当前阶段只支持精确 key 删除，因此该值通常为：</p>
     * <ul>
     *     <li>1：目标 key 存在，并已被删除；</li>
     *     <li>0：目标 key 不存在，没有删除任何数据。</li>
     * </ul>
     */
    private int deletedKeys;

    public DeleteResponse() {
    }

    /**
     * 构造 DELETE 响应体。
     *
     * @param deletedKeys 本次实际删除的 key 数量
     * @return DELETE 响应体
     */
    public static DeleteResponse of(int deletedKeys) {
        DeleteResponse response = new DeleteResponse();
        response.deletedKeys = deletedKeys;
        return response;
    }
}