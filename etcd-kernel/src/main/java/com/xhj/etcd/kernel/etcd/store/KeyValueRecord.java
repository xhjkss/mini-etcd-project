package com.xhj.etcd.kernel.etcd.store;

import lombok.Data;

import java.io.Serializable;

/**
 * KeyValueRecord
 *
 * @author XJks
 * @description MVCC 状态机内部记录。
 *
 * <p>
 * TODO:
 *  createRevision / modRevision / version 是当前 MVCC 语义的核心字段组：
 *  1) createRevision：当前“存活代（generation）”首次创建时的全局 revision；
 *  2) modRevision：当前这条版本记录写入时的全局 revision；
 *  3) version：当前“存活代”内该 key 的第几次变更（包含删除墓碑版本）。
 *  删除（tombstone）后再次 put 会开启新的存活代，因此 version 会从 1 重新开始，createRevision 也会切换为新代首次写入的 revision。
 * </p>
 */
@Data
public class KeyValueRecord implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * 键。
     */
    private String key;

    /**
     * 值。
     */
    private String value;

    /**
     * 首次创建 revision。
     *
     * <p>该字段在同一存活代内保持不变；delete 后新建会重置。</p>
     */
    private long createRevision;

    /**
     * 最近一次修改 revision。
     *
     * <p>每次 put / delete tombstone 都会更新为当前全局 revision。</p>
     */
    private long modRevision;

    /**
     * 当前版本号。
     *
     * <p>该字段是“单 key 计数”，不是全局 revision。</p>
     */
    private long version;

    /**
     * 删除墓碑标记。
     */
    private boolean deleted;
}
