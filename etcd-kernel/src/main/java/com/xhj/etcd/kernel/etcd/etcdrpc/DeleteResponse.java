package com.xhj.etcd.kernel.etcd.etcdrpc;

import lombok.Data;

import java.io.Serializable;

/**
 * DeleteResponse
 *
 * @author XJks
 * @description MVCC KV DELETE 响应。
 */
@Data
public class DeleteResponse implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * 删除 key 数量。
     */
    private int deletedCount;

    /**
     * 删除发生的 revision。
     */
    private long revision;

    /**
     * 构造 DELETE 响应。
     */
    public static DeleteResponse of(int deletedCount, long revision) {
        DeleteResponse response = new DeleteResponse();
        response.deletedCount = deletedCount;
        response.revision = revision;
        return response;
    }
}
