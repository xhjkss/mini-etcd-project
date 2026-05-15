package com.xhj.etcd.kernel.etcd.store.mvcc;

import lombok.Data;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * KeyValueDeleteResult
 *
 * @author XJks
 * @description KeyValueStore 删除结果对象。
 */
@Data
public class KeyValueDeleteResult implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * 删除数量。
     */
    private int deletedCount;

    /**
     * 删除对应的 revision。
     */
    private long revision;

    /**
     * 删除前记录。
     */
    private List<KeyValueRecord> previousRecords = new ArrayList<>();
}
