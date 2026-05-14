package com.xhj.etcd.kernel.etcd.etcdrpc;

import lombok.Data;

import java.io.Serializable;

/**
 * CompactResponse
 *
 * @author XJks
 * @description MVCC 历史压缩响应。
 */
@Data
public class CompactResponse implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * 压缩后的 compact revision。
     */
    private long compactRevision;

    /**
     * 当前状态机 revision。
     */
    private long currentRevision;

    /**
     * 构造 compact 响应。
     */
    public static CompactResponse of(long compactRevision, long currentRevision) {
        CompactResponse response = new CompactResponse();
        response.setCompactRevision(compactRevision);
        response.setCurrentRevision(currentRevision);
        return response;
    }
}
