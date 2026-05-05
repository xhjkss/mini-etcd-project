package com.xhj.etcd.storage.memory;

import com.xhj.etcd.storage.Storage;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class MemoryStorageTest {

    @Test
    public void shouldPutAndGetValue() {
        Storage storage = new MemoryStorage();
        storage.put("test", "key1", "hello".getBytes());
        Assert.assertEquals("hello", new String(storage.get("test", "key1")));
    }

    @Test
    public void shouldDeleteValue() {
        Storage storage = new MemoryStorage();
        storage.put("test", "key1", "hello".getBytes());
        storage.delete("test", "key1");
        Assert.assertNull(storage.get("test", "key1"));
    }

    @Test
    public void shouldCopyBytesDefensively() {
        Storage storage = new MemoryStorage();
        byte[] value = "hello".getBytes();
        storage.put("test", "key1", value);
        value[0] = 'x';
        Assert.assertEquals("hello", new String(storage.get("test", "key1")));

        byte[] loaded = storage.get("test", "key1");
        loaded[0] = 'y';
        Assert.assertEquals("hello", new String(storage.get("test", "key1")));
    }

    @Test
    public void shouldIsolateGroups() {
        Storage storage = new MemoryStorage();
        storage.put("g1", "same", "v1".getBytes());
        storage.put("g2", "same", "v2".getBytes());
        Assert.assertEquals("v1", new String(storage.get("g1", "same")));
        Assert.assertEquals("v2", new String(storage.get("g2", "same")));
    }

    @Test
    public void shouldListKeysByGroupInOrder() {
        Storage storage = new MemoryStorage();
        storage.put("raft-log", "node-1:2", "entry-2".getBytes());
        storage.put("raft-log", "node-1:1", "entry-1".getBytes());
        storage.put("other", "node-1:3", "entry-3".getBytes());

        List<String> keys = storage.listKeys("raft-log");

        Assert.assertEquals(2, keys.size());
        Assert.assertEquals("node-1:1", keys.get(0));
        Assert.assertEquals("node-1:2", keys.get(1));
    }
}
