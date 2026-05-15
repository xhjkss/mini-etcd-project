package com.xhj.etcd.kernel.etcd.store.mvcc;

import lombok.Data;

import java.io.Serializable;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;

/**
 * KeyValueStoreSnapshot
 *
 * @author XJks
 * @description MVCC 状态机快照对象。
 *
 * <p>快照结构与 KeyValueStore 运行态结构一致，便于排查恢复问题和后续增量演进。</p>
 */
@Data
public class KeyValueStoreSnapshot implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * 快照时的当前 revision。
     */
    private long revision;

    /**
     * 快照时的 compact revision。
     *
     * <p>恢复后历史读会基于该边界拒绝读取已被压缩的旧 revision。</p>
     */
    private long compactRevision;

    /**
     * 快照保存的历史记录。
     *
     * <p>结构与运行态 KeyValueStore.historyByKey 保持一致，便于阅读和恢复。</p>
     */
    private NavigableMap<String, List<KeyValueRecord>> historyByKey = new TreeMap<>();
}
