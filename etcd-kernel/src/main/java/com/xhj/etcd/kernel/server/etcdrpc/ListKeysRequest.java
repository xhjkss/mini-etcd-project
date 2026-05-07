package com.xhj.etcd.kernel.server.etcdrpc;

import lombok.Data;

import java.io.Serializable;

/**
 * ListKeysRequest
 *
 * @author XJks
 * @description 当前阶段临时 KV 服务的 LIST_KEYS 请求，用于列出指定 group 下的所有 key。
 *
 * <p>阶段边界：</p>
 * <p>该类只服务于当前临时 KV 服务与 Raft/RPC 联调闭环，只保留 group 和读一致性开关。</p>
 *
 * <p>读一致性语义：</p>
 * <ul>
 *     <li>linearizableRead=true：请求会被封装为 EtcdCommand 并进入 Raft 顺序流，committed/apply 后再读取状态机。</li>
 *     <li>linearizableRead=false：请求只进入 EtcdEvent，由 etcd-event-loop 直接读取当前节点本地状态机，可能读到旧数据。</li>
 * </ul>
 */
@Data
public class ListKeysRequest implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * 待查询的 KV 分组。
     *
     * <p>如果 group 为 null，EtcdNode 会使用默认临时 KV group。</p>
     */
    private String group;

    /**
     * 是否执行线性一致读。
     *
     * <p>默认 true，保持当前 mini etcd 的强一致读语义。
     * 显式设置为 false 时，EtcdNode 不会把该读请求提交到 Raft，而是直接读取本节点本地状态机。</p>
     */
    private boolean linearizableRead = true;

    public ListKeysRequest() {
    }

    public ListKeysRequest(String group) {
        this.group = group;
    }

    public ListKeysRequest(String group, boolean linearizableRead) {
        this.group = group;
        this.linearizableRead = linearizableRead;
    }
}
