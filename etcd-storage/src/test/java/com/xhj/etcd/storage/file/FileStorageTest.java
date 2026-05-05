package com.xhj.etcd.storage.file;

import com.xhj.etcd.storage.Storage;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.List;

public class FileStorageTest {

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void shouldPutAndGetValueFromDisk() throws Exception {
        Storage storage = new FileStorage(temporaryFolder.newFolder("data"));

        storage.put("test", "key1", "hello".getBytes());

        Assert.assertEquals("hello", new String(storage.get("test", "key1")));
    }

    @Test
    public void shouldDeleteValueFromDisk() throws Exception {
        Storage storage = new FileStorage(temporaryFolder.newFolder("data"));

        storage.put("test", "key1", "hello".getBytes());
        storage.delete("test", "key1");

        Assert.assertNull(storage.get("test", "key1"));
    }

    @Test
    public void shouldPersistValueAcrossFileStorageInstances() throws Exception {
        java.io.File rootDir = temporaryFolder.newFolder("data");
        Storage firstStorage = new FileStorage(rootDir);
        firstStorage.put("raft-log", "1", "entry-1".getBytes());

        Storage secondStorage = new FileStorage(rootDir);

        Assert.assertEquals("entry-1", new String(secondStorage.get("raft-log", "1")));
    }

    @Test
    public void shouldIsolateGroupsOnDisk() throws Exception {
        Storage storage = new FileStorage(temporaryFolder.newFolder("data"));

        storage.put("g1", "same", "v1".getBytes());
        storage.put("g2", "same", "v2".getBytes());

        Assert.assertEquals("v1", new String(storage.get("g1", "same")));
        Assert.assertEquals("v2", new String(storage.get("g2", "same")));
    }

    @Test
    public void shouldListKeysByGroupInOrder() throws Exception {
        Storage storage = new FileStorage(temporaryFolder.newFolder("data"));
        storage.put("raft-log", "node-1:2", "entry-2".getBytes());
        storage.put("raft-log", "node-1:1", "entry-1".getBytes());
        storage.put("other", "node-1:3", "entry-3".getBytes());

        List<String> keys = storage.listKeys("raft-log");

        Assert.assertEquals(2, keys.size());
        Assert.assertEquals("node-1:1", keys.get(0));
        Assert.assertEquals("node-1:2", keys.get(1));
    }
}
