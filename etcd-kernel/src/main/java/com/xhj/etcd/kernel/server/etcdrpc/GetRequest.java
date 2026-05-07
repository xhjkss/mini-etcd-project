package com.xhj.etcd.kernel.server.etcdrpc;

import lombok.Data;

import java.io.Serializable;

/**
 * GetRequest
 *
 * @author XJks
 * @description 当前阶段临时 KV 服务的 GET 请求，用于读取指定 key 的值。
 *
 * <p>阶段边界：</p>
 * <p>该类只服务于当前临时 KV 服务与 Raft/RPC 联调闭环，只保留 key 和读一致性开关。</p>
 *
 * <p>读一致性语义：</p>
 * <ul>
 *     <li>linearizableRead=true：请求会被封装为 EtcdCommand 并进入 Raft 顺序流，committed/apply 后再读取状态机。</li>
 *     <li>linearizableRead=false：请求只进入 EtcdEvent，由 etcd-event-loop 直接读取当前节点本地状态机，可能读到旧数据。</li>
 * </ul>
 */
@Data
public class GetRequest implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * 待读取的 key。
     */
    private String key;

    /**
     * 是否执行线性一致读。
     *
     * <p>默认 true，保持当前 mini etcd 的强一致读语义。
     * 显式设置为 false 时，EtcdNode 不会把该读请求提交到 Raft，而是直接读取本节点本地状态机。</p>
     */
    private boolean linearizableRead = true;

    public GetRequest() {
    }

    public GetRequest(String key) {
        this.key = key;
    }

    public GetRequest(String key, boolean linearizableRead) {
        this.key = key;
        this.linearizableRead = linearizableRead;
    }
}
