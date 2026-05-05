package com.xhj.etcd.storage;

import com.xhj.etcd.storage.file.FileStorage;
import com.xhj.etcd.storage.memory.MemoryStorage;
import org.junit.Assert;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;

public class StorageBoundaryTest {

    @Test
    public void shouldProtectStoredBytesFromCallerMutationInMemoryStorage() {
        Storage storage = new MemoryStorage();
        byte[] value = new byte[] {1, 2, 3};
        storage.put("group", "key", value);
        value[0] = 9;

        byte[] loaded = storage.get("group", "key");
        Assert.assertArrayEquals(new byte[] {1, 2, 3}, loaded);

        loaded[1] = 9;
        Assert.assertArrayEquals(new byte[] {1, 2, 3}, storage.get("group", "key"));
    }

    @Test
    public void shouldProtectStoredBytesFromCallerMutationInFileStorage() throws Exception {
        Path rootDir = Files.createTempDirectory("mini-etcd-storage-boundary");
        Storage storage = new FileStorage(rootDir);
        byte[] value = new byte[] {1, 2, 3};
        storage.put("group", "key/with/slash", value);
        value[0] = 9;

        byte[] loaded = storage.get("group", "key/with/slash");
        Assert.assertArrayEquals(new byte[] {1, 2, 3}, loaded);
        loaded[1] = 9;
        Assert.assertArrayEquals(new byte[] {1, 2, 3}, storage.get("group", "key/with/slash"));
        Assert.assertEquals(Arrays.asList("key/with/slash"), storage.listKeys("group"));
    }

    @Test
    public void shouldTreatNullValueAsDeleteThroughStorageInterface() {
        Storage storage = new MemoryStorage();
        storage.put("group", "key", new byte[] {1});
        storage.put("group", "key", null);

        Assert.assertNull(storage.get("group", "key"));
        Assert.assertTrue(storage.listKeys("group").isEmpty());
    }
}
