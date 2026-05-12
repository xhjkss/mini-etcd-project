package com.xhj.etcd.kernel.etcd.etcdrpc;

import lombok.Data;

import java.io.Serializable;

/**
 * DeleteRangeRequest
 *
 * @author XJks
 * @description MVCC KV DELETE_RANGE 请求。
 *
 * <p>该请求用于单 key 删除、区间删除和前缀删除。</p>
 */
@Data
public class DeleteRangeRequest implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * 起始 key。
     */
    private String startKey;

    /**
     * 区间结束 key（左闭右开）。
     *
     * <p>当 prefixMatch=true 时，该字段会被服务端忽略，由 startKey 计算前缀结束边界。</p>
     */
    private String endKeyExclusive;

    /**
     * 是否按前缀删除。
     */
    private boolean prefixMatch;

    /**
     * 是否返回删除前的值（prevKv 语义）。
     */
    private boolean prevKv;
}
