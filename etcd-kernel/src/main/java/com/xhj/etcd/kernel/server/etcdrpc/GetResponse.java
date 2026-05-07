package com.xhj.etcd.kernel.server.etcdrpc;

import lombok.Data;

import java.io.Serializable;

/**
 * GetResponse
 *
 * @author XJks
 * @description 当前阶段临时 KV 服务的 GET 响应体，用于返回指定 key 对应的 value。
 *
 * <p>阶段边界：</p>
 * <p>当前临时 KV 服务只支持按精确 key 查询，响应体只保留当前阶段真实产生的 value。</p>
 *
 * <p>说明：</p>
 * <p>请求是否成功、错误信息、notLeader 等通用响应信息统一由 EtcdRpcResponse.header 表达；
 * GetResponse 只保留 GET 操作特有的响应字段。</p>
 */
@Data
public class GetResponse implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * 查询到的 value。
     *
     * <p>如果目标 key 不存在，该字段为 null。
     * 当前阶段 value 由临时 KV 状态机中的字节数组转换为字符串得到。</p>
     */
    private String value;

    public GetResponse() {
    }

    /**
     * 构造 GET 响应体。
     *
     * @param value 查询到的 value；key 不存在时为 null
     * @return GET 响应体
     */
    public static GetResponse of(String value) {
        GetResponse response = new GetResponse();
        response.value = value;
        return response;
    }
}