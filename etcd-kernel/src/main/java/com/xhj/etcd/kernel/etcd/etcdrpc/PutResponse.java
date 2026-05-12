package com.xhj.etcd.kernel.etcd.etcdrpc;

import lombok.Data;

import java.io.Serializable;

/**
 * PutResponse
 *
 * @author XJks
 * @description MVCC KV PUT 响应。
 */
@Data
public class PutResponse implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * 写入后的 revision。
     */
    private long revision;

    /**
     * 构造 PUT 响应。
     */
    public static PutResponse of(long revision) {
        PutResponse response = new PutResponse();
        response.revision = revision;
        return response;
    }
}
