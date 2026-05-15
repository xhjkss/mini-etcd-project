package com.xhj.etcd.kernel.etcd.etcdrpc;

import lombok.Data;

import java.io.Serializable;

/**
 * KeyValueView
 *
 * @author XJks
 * @description RANGE 响应中的单条 KV 记录。
 */
@Data
public class KeyValueView implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * key。
     */
    private String key;

    /**
     * value。
     */
    private String value;

    /**
     * 首次创建 revision。
     */
    private long createRevision;

    /**
     * 最近一次修改 revision。
     */
    private long modRevision;

    /**
     * 当前版本号。
     */
    private long version;

    /**
     * 当前 key 绑定的 leaseId。
     */
    private long leaseId;

    /**
     * 构造 KeyValueView 条目。
     */
    public static KeyValueView of(String key, String value, long createRevision, long modRevision, long version) {
        return of(key, value, createRevision, modRevision, version, 0L);
    }

    /**
     * 构造 KeyValueView 条目。
     */
    public static KeyValueView of(String key, String value, long createRevision, long modRevision, long version, long leaseId) {
        KeyValueView view = new KeyValueView();
        view.key = key;
        view.value = value;
        view.createRevision = createRevision;
        view.modRevision = modRevision;
        view.version = version;
        view.leaseId = leaseId;
        return view;
    }
}
