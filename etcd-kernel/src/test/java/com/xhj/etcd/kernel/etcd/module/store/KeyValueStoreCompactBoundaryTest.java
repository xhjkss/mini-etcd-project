package com.xhj.etcd.kernel.etcd.module.store;

import com.xhj.etcd.kernel.etcd.store.KeyValueRecord;
import com.xhj.etcd.kernel.etcd.store.KeyValueStore;
import com.xhj.etcd.kernel.etcd.store.KeyValueStoreSnapshot;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * KeyValueStoreCompactBoundaryTest
 *
 * @author XJks
 * @description KeyValueStore compact 历史压缩与边界语义测试。
 */
public class KeyValueStoreCompactBoundaryTest {

    @Test
    public void shouldCompactHistoryBeforeRevisionAndKeepBoundaryVisibleVersion() {
        KeyValueStore store = new KeyValueStore();
        KeyValueRecord firstRecord = store.put("compact/k", "v1");
        KeyValueRecord secondRecord = store.put("compact/k", "v2");
        KeyValueRecord thirdRecord = store.put("compact/k", "v3");

        int removedRecordCount = store.compact(secondRecord.getModRevision());
        assertEquals(1, removedRecordCount);
        assertEquals(secondRecord.getModRevision(), store.compactRevision());
        assertEquals(thirdRecord.getModRevision(), store.currentRevision());

        KeyValueRecord revision2Record = store.get("compact/k", secondRecord.getModRevision());
        assertNotNull(revision2Record);
        assertEquals("v2", revision2Record.getValue());

        try {
            store.get("compact/k", firstRecord.getModRevision());
            fail("historical read before compact revision should fail");
        } catch (IllegalArgumentException expected) {
            assertTrue(expected.getMessage().contains("compacted"));
        }

        KeyValueRecord latestRecord = store.get("compact/k", 0L);
        assertNotNull(latestRecord);
        assertEquals("v3", latestRecord.getValue());
    }

    @Test
    public void shouldDropKeyHistoryWhenBoundaryRecordIsTombstone() {
        KeyValueStore store = new KeyValueStore();
        store.put("compact/deleted", "alive");
        store.delete("compact/deleted");

        int removedRecordCount = store.compact(store.currentRevision());
        assertEquals(2, removedRecordCount);
        assertNull(store.get("compact/deleted", 0L));

        List<KeyValueRecord> records = store.range("compact/deleted", null, false, 0, false, false, 0L);
        assertTrue(records.isEmpty());
    }

    @Test
    public void shouldRejectInvalidCompactRevisionBoundaries() {
        KeyValueStore store = new KeyValueStore();
        store.put("compact/invalid", "v1");

        try {
            store.compact(0L);
            fail("compact revision <= 0 should fail");
        } catch (IllegalArgumentException expected) {
            // expected
        }

        try {
            store.compact(store.currentRevision() + 1L);
            fail("future compact revision should fail");
        } catch (IllegalArgumentException expected) {
            // expected
        }

        long compactRevision = store.currentRevision();
        store.compact(compactRevision);
        try {
            store.compact(compactRevision);
            fail("compact revision <= compactRevision should fail");
        } catch (IllegalArgumentException expected) {
            assertTrue(expected.getMessage().contains("compacted"));
        }
    }

    @Test
    public void shouldRestoreCompactRevisionFromSnapshot() {
        KeyValueStore sourceStore = new KeyValueStore();
        KeyValueRecord firstRecord = sourceStore.put("compact/snapshot", "v1");
        KeyValueRecord secondRecord = sourceStore.put("compact/snapshot", "v2");
        sourceStore.compact(secondRecord.getModRevision());

        KeyValueStoreSnapshot snapshot = sourceStore.createSnapshot();
        KeyValueStore targetStore = new KeyValueStore();
        targetStore.restoreSnapshot(snapshot);

        assertEquals(sourceStore.currentRevision(), targetStore.currentRevision());
        assertEquals(sourceStore.compactRevision(), targetStore.compactRevision());

        try {
            targetStore.get("compact/snapshot", firstRecord.getModRevision());
            fail("restored compact boundary should reject compacted revision");
        } catch (IllegalArgumentException expected) {
            assertTrue(expected.getMessage().contains("compacted"));
        }
    }

    @Test
    public void shouldAllowReadAtCompactRevisionBoundary() {
        KeyValueStore store = new KeyValueStore();
        store.put("compact/boundary/k", "v1");
        KeyValueRecord boundaryRecord = store.put("compact/boundary/k", "v2");
        store.compact(boundaryRecord.getModRevision());

        KeyValueRecord recordAtBoundary = store.get("compact/boundary/k", boundaryRecord.getModRevision());
        assertNotNull(recordAtBoundary);
        assertEquals("v2", recordAtBoundary.getValue());

        List<KeyValueRecord> rangeRecordsAtBoundary = store.range(
                "compact/boundary/",
                null,
                true,
                0,
                false,
                false,
                boundaryRecord.getModRevision());
        assertEquals(1, rangeRecordsAtBoundary.size());
        assertEquals("v2", rangeRecordsAtBoundary.get(0).getValue());
    }

    @Test
    public void shouldKeepDifferentKeysHistoryConsistentAfterCompaction() {
        KeyValueStore store = new KeyValueStore();
        KeyValueRecord k1v1Record = store.put("compact/multi/k1", "v1");
        KeyValueRecord k1v2Record = store.put("compact/multi/k1", "v2");
        KeyValueRecord k2v1Record = store.put("compact/multi/k2", "a1");
        store.put("compact/multi/k1", "v3");

        int removedRecordCount = store.compact(k1v2Record.getModRevision());
        assertEquals(1, removedRecordCount);
        assertEquals(k1v2Record.getModRevision(), store.compactRevision());

        KeyValueRecord k1AtBoundaryRecord = store.get("compact/multi/k1", k1v2Record.getModRevision());
        assertNotNull(k1AtBoundaryRecord);
        assertEquals("v2", k1AtBoundaryRecord.getValue());

        KeyValueRecord k2Record = store.get("compact/multi/k2", k2v1Record.getModRevision());
        assertNotNull(k2Record);
        assertEquals("a1", k2Record.getValue());

        try {
            store.get("compact/multi/k1", k1v1Record.getModRevision());
            fail("compacted historical record should not be readable");
        } catch (IllegalArgumentException expected) {
            assertTrue(expected.getMessage().contains("compacted"));
        }
    }
}
