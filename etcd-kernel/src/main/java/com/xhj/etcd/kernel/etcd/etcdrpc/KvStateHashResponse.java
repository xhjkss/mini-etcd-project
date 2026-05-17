package com.xhj.etcd.kernel.etcd.etcdrpc;

import lombok.Data;

import java.io.Serializable;

/**
 * KvStateHashResponse
 *
 * @author XJks
 * @description MVCC 状态机哈希诊断响应。
 */
@Data
public class KvStateHashResponse implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * 当前状态机哈希值。
     */
    private long hash;

    /**
     * 实际参与哈希计算的 revision。
     */
    private long revision;

    /**
     * 当前 compact revision。
     */
    private long compactRevision;

    /**
     * 参与哈希的可见 key 数量。
     */
    private int keyCount;

    /**
     * 构造 KvStateHash 响应。
     */
    public static KvStateHashResponse of(long hash, long revision, long compactRevision, int keyCount) {
        KvStateHashResponse response = new KvStateHashResponse();
        response.setHash(hash);
        response.setRevision(revision);
        response.setCompactRevision(compactRevision);
        response.setKeyCount(keyCount);
        return response;
    }
}
