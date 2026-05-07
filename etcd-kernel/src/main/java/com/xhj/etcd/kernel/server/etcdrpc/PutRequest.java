package com.xhj.etcd.kernel.server.etcdrpc;

import lombok.Data;

import java.io.Serializable;

/**
 * PutRequest
 *
 * @author XJks
 * @description 当前阶段临时 KV 服务的 PUT 请求，用于提交 key-value 写入。
 *
 * <p>阶段边界：</p>
 * <p>该类只服务于当前临时 KV 服务与 Raft/RPC 联调闭环，只保留 key 和 value。</p>
 *
 * <p>处理流程：</p>
 * <p>1) 客户端构造 PutRequest，并通过 EtcdClient 发送到 EtcdNode；</p>
 * <p>2) RPC handler 将 PutRequest 封装为 EtcdEvent 投递到 etcd-event-loop；</p>
 * <p>3) etcd-event-loop 将 PutRequest 放入 EtcdCommand，并提交 Raft propose；</p>
 * <p>4) 日志 committed 并 apply 后，临时 KV 状态机写入 key-value，并返回 PutResponse。</p>
 */
@Data
public class PutRequest implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * 待写入的 key。
     */
    private String key;

    /**
     * 待写入的 value。
     */
    private String value;

    public PutRequest() {
    }

    public PutRequest(String key, String value) {
        this.key = key;
        this.value = value;
    }
}
