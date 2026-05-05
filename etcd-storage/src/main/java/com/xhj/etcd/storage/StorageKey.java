package com.xhj.etcd.storage;

import lombok.Data;

import java.io.Serializable;

/**
 * StorageKey
 *
 * @author XJks
 * @description 存储键对象，用 group 和 key 共同标识一条底层存储记录。
 */
@Data
public class StorageKey implements Serializable {

    /**
     * 存储分组，用于隔离不同类型的数据。
     * <p>
     * TODO:group 是 Storage 层的命名空间边界。在 FileStorage 中，group 会被映射为根目录下的一级子目录；相同 key 在不同 group 下会落到不同文件中，因此不会互相覆盖。
     */
    private final String group;

    /**
     * 分组内的存储键，用于定位当前 group 下的一条数据。
     *
     * <p>在 FileStorage 中，key 会被映射为 group 目录下的数据文件名。</p>
     */
    private final String key;

    public StorageKey(String group, String key) {
        if (group == null || group.trim().length() == 0) {
            throw new IllegalArgumentException("group must not be empty");
        }
        if (key == null || key.trim().length() == 0) {
            throw new IllegalArgumentException("key must not be empty");
        }
        this.group = group;
        this.key = key;
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (!(object instanceof StorageKey)) {
            return false;
        }
        StorageKey that = (StorageKey) object;
        return group.equals(that.group) && key.equals(that.key);
    }

    @Override
    public int hashCode() {
        int result = group.hashCode();
        result = 31 * result + key.hashCode();
        return result;
    }
}
