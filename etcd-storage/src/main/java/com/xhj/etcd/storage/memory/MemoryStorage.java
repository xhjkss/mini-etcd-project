package com.xhj.etcd.storage.memory;

import com.xhj.etcd.storage.Storage;
import com.xhj.etcd.storage.StorageKey;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * MemoryStorage
 *
 * @author XJks
 * @description 内存存储实现，负责在进程内保存 group/key 到字节数组的映射。
 */
public class MemoryStorage implements Storage {

    private final Map<StorageKey, byte[]> storageMap = new ConcurrentHashMap<>();

    // ==================== Put ====================

    @Override
    public void put(String group, String key, byte[] value) {
        StorageKey storageKey = new StorageKey(group, key);
        if (value == null) {
            storageMap.remove(storageKey);
            return;
        }
        storageMap.put(storageKey, Arrays.copyOf(value, value.length));
    }

    // ==================== Get ====================

    @Override
    public byte[] get(String group, String key) {
        StorageKey storageKey = new StorageKey(group, key);
        byte[] value = storageMap.get(storageKey);
        if (value == null) {
            return null;
        }
        return Arrays.copyOf(value, value.length);
    }

    // ==================== Delete ====================

    @Override
    public void delete(String group, String key) {
        storageMap.remove(new StorageKey(group, key));
    }

    // ==================== List ====================

    @Override
    public List<String> listKeys(String group) {
        if (group == null || group.trim().length() == 0) {
            throw new IllegalArgumentException("group must not be empty");
        }

        List<String> result = new ArrayList<>();
        for (StorageKey storageKey : storageMap.keySet()) {
            if (group.equals(storageKey.getGroup())) {
                result.add(storageKey.getKey());
            }
        }
        Collections.sort(result);
        return result;
    }
}
