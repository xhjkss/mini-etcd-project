package com.xhj.etcd.kernel.etcd.etcdrpc;

import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * KvStateHashRequest
 *
 * @author XJks
 * @description MVCC 状态机哈希诊断请求。
 *
 * <p>revision=0 表示读取当前最新 revision 的状态哈希；大于 0 时读取指定 revision 的状态哈希。</p>
 */
@Data
@NoArgsConstructor
public class KvStateHashRequest implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * 目标 revision，0 表示当前最新 revision。
     */
    private long revision;

    public KvStateHashRequest(long revision) {
        this.revision = revision;
    }
}
