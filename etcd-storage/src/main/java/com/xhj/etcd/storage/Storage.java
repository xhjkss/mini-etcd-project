package com.xhj.etcd.storage;

import java.util.List;

/**
 * Storage
 *
 * @author XJks
 * @description 存储能力接口，定义按 group/key 读写二进制数据的最小持久化边界。
 */
public interface Storage {

    // ==================== Put ====================

    /**
     * 写入指定 key 对应的数据。
     */
    void put(String group, String key, byte[] value);

    // ==================== Get ====================

    /**
     * 读取指定 key 对应的数据。
     */
    byte[] get(String group, String key);

    // ==================== Delete ====================

    /**
     * 删除指定 key 对应的数据。
     */
    void delete(String group, String key);

    // ==================== List ====================

    /**
     * 列出指定 group 下的全部 key。
     */
    List<String> listKeys(String group);
}
