package com.xhj.etcd.kernel.etcd.etcdrpc;

import lombok.Data;

import java.io.Serializable;

/**
 * GetResponse
 *
 * @author XJks
 * @description MVCC KV GET 响应。
 *
 * <p>当前响应同时返回 value 和关键 MVCC 元信息，便于后续扩展 Range/Watch/Compact 相关语义验证。</p>
 */
@Data
public class GetResponse implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * 当前值。
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
     * 当前读取使用的 revision。
     */
    private long revision;

    /**
     * 构造包含完整 MVCC 元信息的 GET 响应。
     */
    public static GetResponse of(String value, long createRevision, long modRevision, long version, long revision) {
        return of(value, createRevision, modRevision, version, revision, 0L);
    }

    /**
     * 构造包含完整 MVCC 元信息的 GET 响应。
     */
    public static GetResponse of(String value, long createRevision, long modRevision, long version, long revision, long leaseId) {
        GetResponse response = new GetResponse();
        response.value = value;
        response.createRevision = createRevision;
        response.modRevision = modRevision;
        response.version = version;
        response.revision = revision;
        response.leaseId = leaseId;
        return response;
    }

    /**
     * 构造空值响应。
     *
     * <p>用于 key 不存在或在目标 revision 已删除的场景。</p>
     */
    public static GetResponse empty(long revision) {
        GetResponse response = new GetResponse();
        response.revision = revision;
        return response;
    }
}
