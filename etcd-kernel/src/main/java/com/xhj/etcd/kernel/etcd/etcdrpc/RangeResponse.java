package com.xhj.etcd.kernel.etcd.etcdrpc;

import lombok.Data;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * RangeResponse
 *
 * @author XJks
 * @description MVCC KV RANGE 响应。
 */
@Data
public class RangeResponse implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * 是否成功。
     */
    private boolean success;

    /**
     * 条目列表。
     */
    private List<KeyValueView> items = new ArrayList<>();

    /**
     * 数量。
     */
    private int count;

    /**
     * 修订号。
     */
    private long revision;

    /**
     * 错误消息。
     */
    private String message;

    /**
     * 构造成功的 RANGE 响应。
     */
    public static RangeResponse of(List<KeyValueView> items, int count, long revision) {
        RangeResponse response = new RangeResponse();
        response.success = true;
        if (items != null) {
            response.items = new ArrayList<>(items);
        }
        response.count = count;
        response.revision = revision;
        return response;
    }
}
