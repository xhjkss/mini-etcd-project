package com.xhj.etcd.kernel.etcd.module.store;

import com.xhj.etcd.kernel.etcd.store.KeyValueStore;
import com.xhj.etcd.kernel.etcd.store.KeyValueRecord;
import com.xhj.etcd.kernel.etcd.store.KeyValueDeleteResult;

import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * KeyValueStoreRangeDeleteRangeTest
 *
 * @author XJks
 * @description KeyValueStore 范围读写语义测试。
 */
public class KeyValueStoreRangeDeleteRangeTest {

    @Test
    public void shouldReadByEndKeyExclusiveAndPrefixMatch() {
        KeyValueStore store = new KeyValueStore();
        store.put("app/a", "v1");
        store.put("app/b", "v2");
        store.put("apq/c", "v3");

        List<KeyValueRecord> intervalRecords = store.range("app/", "app0", false, 0, false, false, 0L);
        assertEquals(2, intervalRecords.size());
        assertEquals("app/a", intervalRecords.get(0).getKey());
        assertEquals("app/b", intervalRecords.get(1).getKey());

        List<KeyValueRecord> prefixRecords = store.range("app/", null, true, 0, false, false, 0L);
        assertEquals(2, prefixRecords.size());
        assertEquals("app/a", prefixRecords.get(0).getKey());
        assertEquals("app/b", prefixRecords.get(1).getKey());
    }

    @Test
    public void shouldApplyKeysOnlyCountOnlyAndLimit() {
        KeyValueStore store = new KeyValueStore();
        store.put("k/1", "v1");
        store.put("k/2", "v2");
        store.put("k/3", "v3");

        List<KeyValueRecord> keysOnlyRecords = store.range("k/", null, true, 0, true, false, 0L);
        assertEquals(3, keysOnlyRecords.size());
        for (KeyValueRecord record : keysOnlyRecords) {
            assertNotNull(record.getKey());
            assertNull(record.getValue());
        }

        List<KeyValueRecord> limitedRecords = store.range("k/", null, true, 2, false, false, 0L);
        assertEquals(2, limitedRecords.size());

        List<KeyValueRecord> countOnlyRecords = store.range("k/", null, true, 0, false, true, 0L);
        assertTrue(countOnlyRecords.isEmpty());
    }

    @Test
    public void shouldDeleteRangeAndReturnPrevKvWhenEnabled() {
        KeyValueStore store = new KeyValueStore();
        KeyValueRecord first = store.put("dr/1", "v1");
        store.put("dr/2", "v2");
        store.put("keep/1", "v3");

        KeyValueDeleteResult deleteResult = store.deleteRange("dr/", null, true, true);
        assertEquals(2, deleteResult.getDeletedCount());
        assertEquals(2, deleteResult.getPreviousRecords().size());
        assertTrue(deleteResult.getRevision() > first.getModRevision());

        List<KeyValueRecord> deletedRecords = store.range("dr/", null, true, 0, false, false, 0L);
        assertTrue(deletedRecords.isEmpty());

        List<KeyValueRecord> keptRecords = store.range("keep/", null, true, 0, false, false, 0L);
        assertEquals(1, keptRecords.size());
        assertEquals("v3", keptRecords.get(0).getValue());
    }

    @Test
    public void shouldNotIncreaseGlobalRevisionWhenDeleteRangeHasNoMatch() {
        KeyValueStore store = new KeyValueStore();
        store.put("x/1", "v1");
        long beforeDeleteRevision = store.currentRevision();

        KeyValueDeleteResult deleteResult = store.deleteRange("missing/", null, true, false);
        assertEquals(0, deleteResult.getDeletedCount());
        assertEquals(beforeDeleteRevision, deleteResult.getRevision());
        assertEquals(beforeDeleteRevision, store.currentRevision());
    }

    @Test
    public void shouldDeleteByExplicitIntervalAndKeepPrevItemsEmptyWhenPrevKvDisabled() {
        KeyValueStore store = new KeyValueStore();
        store.put("a/1", "v1");
        store.put("a/2", "v2");
        store.put("a/3", "v3");
        long beforeDeleteRevision = store.currentRevision();

        KeyValueDeleteResult deleteResult = store.deleteRange("a/1", "a/3", false, false);
        assertEquals(2, deleteResult.getDeletedCount());
        assertTrue(deleteResult.getPreviousRecords().isEmpty());
        assertTrue(deleteResult.getRevision() > beforeDeleteRevision);

        List<KeyValueRecord> deletedRange = store.range("a/1", "a/3", false, 0, false, false, 0L);
        assertTrue(deletedRange.isEmpty());

        List<KeyValueRecord> remainingRecords = store.range("a/3", null, false, 0, false, false, 0L);
        assertEquals(1, remainingRecords.size());
        assertEquals("a/3", remainingRecords.get(0).getKey());
        assertEquals("v3", remainingRecords.get(0).getValue());
    }

    @Test
    public void shouldRejectInvalidRangeRevisionAndInvalidInterval() {
        KeyValueStore store = new KeyValueStore();
        store.put("r/1", "v1");

        try {
            store.range("r/", null, true, 0, false, false, -1L);
            fail("negative revision should throw IllegalArgumentException");
        } catch (IllegalArgumentException expected) {
            // expected
        }

        try {
            store.range("r/", null, true, 0, false, false, 9999L);
            fail("future revision should throw IllegalArgumentException");
        } catch (IllegalArgumentException expected) {
            // expected
        }

        try {
            store.range("r/9", "r/1", false, 0, false, false, 0L);
            fail("invalid interval should throw IllegalArgumentException");
        } catch (IllegalArgumentException expected) {
            // expected
        }
    }

    @Test
    public void shouldSupportPrefixRangeAndDeleteOnHighUnicodeBoundary() {
        KeyValueStore store = new KeyValueStore();
        String highPrefix = "edge-\uFFFF";
        String key1 = highPrefix + "-1";
        String key2 = highPrefix + "/child";
        String otherKey = "edge-z";

        store.put(key1, "v1");
        store.put(key2, "v2");
        store.put(otherKey, "v3");

        List<KeyValueRecord> prefixRangeRecords = store.range(highPrefix, null, true, 0, false, false, 0L);
        assertEquals(2, prefixRangeRecords.size());
        assertEquals(key1, prefixRangeRecords.get(0).getKey());
        assertEquals(key2, prefixRangeRecords.get(1).getKey());

        KeyValueDeleteResult deleteResult = store.deleteRange(highPrefix, null, true, true);
        assertEquals(2, deleteResult.getDeletedCount());
        assertEquals(2, deleteResult.getPreviousRecords().size());

        List<KeyValueRecord> afterDeletePrefixRangeRecords = store.range(highPrefix, null, true, 0, false, false, 0L);
        assertTrue(afterDeletePrefixRangeRecords.isEmpty());

        KeyValueRecord otherRecord = store.get(otherKey, 0L);
        assertNotNull(otherRecord);
        assertEquals("v3", otherRecord.getValue());
    }
}
