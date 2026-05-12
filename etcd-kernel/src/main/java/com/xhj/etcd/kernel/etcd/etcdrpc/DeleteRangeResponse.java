package com.xhj.etcd.kernel.etcd.etcdrpc;

import lombok.Data;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * DeleteRangeResponse
 *
 * @author XJks
 * @description MVCC KV DELETE_RANGE 响应。
 */
@Data
public class DeleteRangeResponse implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * 是否成功。
     */
    private boolean success;

    /**
     * 删除数量。
     */
    private int deletedCount;

    /**
     * revision。
     */
    private long revision;

    /**
     * 删除前的值。
     */
    private List<KeyValueView> prevItems = new ArrayList<>();

    /**
     * 错误消息。
     */
    private String message;

    /**
     * 构造成功的 DELETE_RANGE 响应。
     */
    public static DeleteRangeResponse of(int deletedCount, long revision, List<KeyValueView> prevItems) {
        DeleteRangeResponse response = new DeleteRangeResponse();
        response.success = true;
        response.deletedCount = deletedCount;
        response.revision = revision;
        if (prevItems != null) {
            response.prevItems = new ArrayList<>(prevItems);
        }
        return response;
    }
}
