package com.xhj.etcd.kernel.etcd.etcdrpc;

import lombok.Data;

import java.io.Serializable;

/**
 * RangeRequest
 *
 * @author XJks
 * @description MVCC KV RANGE 请求。
 *
 * <p>该请求用于单 key 读取、区间读取和前缀读取。</p>
 */
@Data
public class RangeRequest implements Serializable {

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
     * 是否按前缀查询。
     */
    private boolean prefixMatch;

    /**
     * 最大返回条数。
     *
     * <p>0 表示不限制。</p>
     */
    private int limit;

    /**
     * 只返回 key，不返回 value。
     */
    private boolean keysOnly;

    /**
     * 只返回数量。
     *
     * <p>该模式下 items 可以为空，但 count 仍会返回真实匹配数量。</p>
     */
    private boolean countOnly;

    /**
     * 读取 revision，0 表示当前 revision。
     */
    private long revision;

    /**
     * 是否线性一致读。
     */
    private boolean linearizableRead = true;
}
