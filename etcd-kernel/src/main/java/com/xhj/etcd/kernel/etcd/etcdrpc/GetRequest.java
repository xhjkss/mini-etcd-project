package com.xhj.etcd.kernel.etcd.etcdrpc;

import lombok.Data;

import java.io.Serializable;

/**
 * GetRequest
 *
 * @author XJks
 * @description MVCC KV GET 请求。
 *
 * <p>该请求同时支持线性一致读和本地读两条路径：</p>
 * <ul>
 *     <li>linearizableRead=true 且 revision=0：走 Leader 路由并进入 Raft 顺序流。</li>
 *     <li>linearizableRead=false 或 revision>0：由 EtcdNode 在本地状态机读取。</li>
 * </ul>
 */
@Data
public class GetRequest implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * 待读取 key。
     */
    private String key;

    /**
     * 读取 revision，0 表示当前 revision。
     */
    private long revision;

    /**
     * 是否线性一致读。
     */
    private boolean linearizableRead = true;

    /**
     * 空构造。
     */
    public GetRequest() {
    }

    /**
     * 构造默认线性一致读请求。
     */
    public GetRequest(String key) {
        this.key = key;
    }

    /**
     * 构造指定线性一致语义的请求。
     */
    public GetRequest(String key, boolean linearizableRead) {
        this.key = key;
        this.linearizableRead = linearizableRead;
    }

    /**
     * 构造指定 revision 和线性一致语义的请求。
     */
    public GetRequest(String key, long revision, boolean linearizableRead) {
        this.key = key;
        this.revision = revision;
        this.linearizableRead = linearizableRead;
    }
}
