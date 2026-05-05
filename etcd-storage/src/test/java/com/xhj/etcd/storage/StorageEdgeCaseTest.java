package com.xhj.etcd.storage;

import com.xhj.etcd.storage.file.FileStorage;
import com.xhj.etcd.storage.memory.MemoryStorage;
import org.junit.Assert;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Path;

public class StorageEdgeCaseTest {

    @Test
    public void shouldTreatNullPutAsDeleteInMemoryStorage() {
        MemoryStorage storage = new MemoryStorage();
        storage.put("group", "key", new byte[] {1});
        Assert.assertNotNull(storage.get("group", "key"));

        storage.put("group", "key", null);
        Assert.assertNull(storage.get("group", "key"));
    }

    @Test
    public void shouldTreatNullPutAsDeleteInFileStorage() throws Exception {
        Path dir = Files.createTempDirectory("file-storage-null-put-");
        FileStorage storage = new FileStorage(dir.toFile());
        storage.put("group", "key", new byte[] {1});
        Assert.assertNotNull(storage.get("group", "key"));

        storage.put("group", "key", null);
        Assert.assertNull(storage.get("group", "key"));
    }

    @Test
    public void shouldProtectMemoryStorageValueCopies() {
        MemoryStorage storage = new MemoryStorage();
        byte[] value = new byte[] {1, 2, 3};
        storage.put("group", "key", value);
        value[0] = 9;

        byte[] loaded = storage.get("group", "key");
        Assert.assertEquals(1, loaded[0]);
        loaded[1] = 9;
        Assert.assertEquals(2, storage.get("group", "key")[1]);
    }

    @Test
    public void shouldSupportSpecialCharactersInFileStorageGroupAndKey() throws Exception {
        Path dir = Files.createTempDirectory("file-storage-special-");
        FileStorage storage = new FileStorage(dir.toFile());

        String group = "raft/node-1:special";
        String key = "hard/state:term/1";
        byte[] value = new byte[] {1, 2, 3};
        storage.put(group, key, value);

        Assert.assertArrayEquals(value, storage.get(group, key));
        Assert.assertEquals(key, storage.listKeys(group).get(0));
    }

    @Test
    public void shouldRejectBlankGroupAndKey() throws Exception {
        assertIllegalArgument(new Runnable() {
            @Override
            public void run() {
                new MemoryStorage().put("", "key", new byte[] {1});
            }
        });
        assertIllegalArgument(new Runnable() {
            @Override
            public void run() {
                new MemoryStorage().put("group", "", new byte[] {1});
            }
        });

        final FileStorage storage = new FileStorage(Files.createTempDirectory("file-storage-blank-").toFile());
        assertIllegalArgument(new Runnable() {
            @Override
            public void run() {
                storage.put("", "key", new byte[] {1});
            }
        });
        assertIllegalArgument(new Runnable() {
            @Override
            public void run() {
                storage.put("group", "", new byte[] {1});
            }
        });
    }

    private void assertIllegalArgument(Runnable runnable) {
        try {
            runnable.run();
            Assert.fail("should throw IllegalArgumentException");
        } catch (IllegalArgumentException expected) {
            Assert.assertTrue(expected.getMessage() != null);
        }
    }
}
