package com.xhj.etcd.kernel.server.etcdrpc;

import lombok.Data;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * ListKeysResponse
 *
 * @author XJks
 * @description 当前阶段临时 KV 服务的 LIST_KEYS 响应体，用于返回指定 group 下的 key 列表。
 *
 * <p>阶段边界：</p>
 * <p>当前临时 KV 服务只支持按 group 列出 key，响应体只保留当前阶段真实产生的 keys 和 count。</p>
 *
 * <p>说明：</p>
 * <p>请求是否成功、错误信息、notLeader 等通用响应信息统一由 EtcdRpcResponse.header 表达；
 * ListKeysResponse 只保留 LIST_KEYS 操作特有的响应字段。</p>
 */
@Data
public class ListKeysResponse implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * 查询到的 key 列表。
     *
     * <p>当前阶段该列表来自临时 KV 状态机中指定 group 的 key 集合。
     * EtcdNode apply ListKeysRequest 时会对 key 进行排序，保证测试和客户端读取结果稳定。</p>
     */
    private List<String> keys;

    /**
     * 查询到的 key 数量。
     *
     * <p>该字段与 keys.size() 保持一致。
     * 当前阶段保留该字段，便于客户端直接读取数量，也方便测试断言。</p>
     */
    private int count;

    public ListKeysResponse() {
    }

    /**
     * 构造 LIST_KEYS 响应体。
     *
     * <p>这里复制一份 keys，避免响应体直接持有状态机内部集合引用。</p>
     *
     * @param keys 查询到的 key 列表
     * @return LIST_KEYS 响应体
     */
    public static ListKeysResponse of(List<String> keys) {
        ListKeysResponse response = new ListKeysResponse();
        response.keys = keys == null ? new ArrayList<String>() : new ArrayList<String>(keys);
        response.count = response.keys.size();
        return response;
    }
}