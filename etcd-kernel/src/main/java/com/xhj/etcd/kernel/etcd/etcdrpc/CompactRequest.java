package com.xhj.etcd.kernel.etcd.etcdrpc;

import lombok.Data;

import java.io.Serializable;

/**
 * CompactRequest
 *
 * @author XJks
 * @description MVCC 历史压缩请求。
 */
@Data
public class CompactRequest implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * 目标压缩 revision。
     *
     * <p>该值必须 > 0，且不能大于当前状态机 revision。</p>
     */
    private long revision;
}
