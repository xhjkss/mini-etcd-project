package com.xhj.etcd.kernel.server.etcdrpc;

import lombok.Data;

import java.io.Serializable;

/**
 * DeleteRequest
 *
 * @author XJks
 * @description 当前阶段临时 KV 服务的 DELETE 请求，用于删除指定 key。
 *
 * <p>阶段边界：</p>
 * <p>该类只服务于当前临时 KV 服务与 Raft/RPC 联调闭环</p>
 *
 * <p>处理流程：</p>
 * <p>1) 客户端构造 DeleteRequest，并通过 EtcdClient 发送到 EtcdNode；</p>
 * <p>2) RPC handler 将 DeleteRequest 封装为 EtcdEvent 投递到 etcd-event-loop；</p>
 * <p>3) etcd-event-loop 将 DeleteRequest 放入 EtcdCommand，并提交 Raft propose；</p>
 * <p>4) 日志 committed 并 apply 后，临时 KV 状态机按 key 删除对应数据，并返回 DeleteResponse。</p>
 */
@Data
public class DeleteRequest implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * 待删除的 key。
     */
    private String key;

    public DeleteRequest() {
    }

    public DeleteRequest(String key) {
        this.key = key;
    }
}
