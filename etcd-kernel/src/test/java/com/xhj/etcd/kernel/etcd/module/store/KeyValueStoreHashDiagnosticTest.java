package com.xhj.etcd.kernel.etcd.module.store;

import com.xhj.etcd.kernel.etcd.store.mvcc.KeyValueStore;
import com.xhj.etcd.kernel.etcd.store.mvcc.KeyValueStoreSnapshot;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * KeyValueStoreHashDiagnosticTest
 *
 * @author XJks
 * @description KeyValueStore KvStateHash 诊断测试。
 */
public class KeyValueStoreHashDiagnosticTest {

    @Test
    public void shouldComputeStableHashForSameVisibleStateAcrossSnapshotRestore() {
        KeyValueStore store = new KeyValueStore();
        store.put("hash/a", "v1");
        store.put("hash/b", "v2");

        long revision = store.currentRevision();
        long hashBeforeSnapshot = store.computeKvStateHash(0L);
        int keyCountBeforeSnapshot = store.countVisibleKeys(0L);

        KeyValueStoreSnapshot snapshot = store.createSnapshot();
        KeyValueStore restoredStore = new KeyValueStore();
        restoredStore.restoreSnapshot(snapshot);

        assertEquals(revision, restoredStore.currentRevision());
        assertEquals(hashBeforeSnapshot, restoredStore.computeKvStateHash(0L));
        assertEquals(keyCountBeforeSnapshot, restoredStore.countVisibleKeys(0L));
    }

    @Test
    public void shouldChangeHashAfterVisibleStateMutationAndRejectCompactedRevision() {
        KeyValueStore store = new KeyValueStore();
        store.put("hash/change", "v1");
        long firstHash = store.computeKvStateHash(0L);

        store.put("hash/change", "v2");
        long secondHash = store.computeKvStateHash(0L);

        assertTrue(firstHash != secondHash);
        assertEquals(1, store.countVisibleKeys(0L));

        store.compact(store.currentRevision());
        try {
            store.computeKvStateHash(1L);
            fail("hash on compacted revision should fail");
        } catch (IllegalArgumentException expected) {
            assertTrue(expected.getMessage().contains("compacted"));
        }
    }

    @Test
    public void shouldKeepHistoricalRevisionHashStableAfterLaterWrites() {
        KeyValueStore store = new KeyValueStore();
        store.put("hash/history/a", "v1");
        long revisionOne = store.currentRevision();
        long revisionOneHashBeforeLaterWrites = store.computeKvStateHash(revisionOne);

        store.put("hash/history/a", "v2");
        store.put("hash/history/b", "v3");
        long latestHash = store.computeKvStateHash(0L);

        long revisionOneHashAfterLaterWrites = store.computeKvStateHash(revisionOne);
        assertEquals(revisionOneHashBeforeLaterWrites, revisionOneHashAfterLaterWrites);
        assertNotEquals(revisionOneHashBeforeLaterWrites, latestHash);
        assertEquals(1, store.countVisibleKeys(revisionOne));
        assertEquals(2, store.countVisibleKeys(0L));
    }

    @Test
    public void shouldRejectHashWhenRequestedRevisionIsGreaterThanCurrentRevision() {
        KeyValueStore store = new KeyValueStore();
        store.put("hash/future/a", "v1");

        long futureRevision = store.currentRevision() + 1L;
        try {
            store.computeKvStateHash(futureRevision);
            fail("hash on future revision should fail");
        } catch (IllegalArgumentException expected) {
            assertTrue(expected.getMessage().contains("revision"));
        }
    }

    @Test
    public void shouldRejectHashWhenRequestedRevisionIsNegative() {
        KeyValueStore store = new KeyValueStore();
        store.put("hash/negative/a", "v1");

        try {
            store.computeKvStateHash(-1L);
            fail("hash on negative revision should fail");
        } catch (IllegalArgumentException expected) {
            assertTrue(expected.getMessage().contains("negative"));
        }
    }

    @Test
    public void shouldReflectDeleteMutationOnHashAndVisibleKeyCount() {
        KeyValueStore store = new KeyValueStore();
        store.put("hash/delete/a", "v1");
        store.put("hash/delete/b", "v2");
        long hashBeforeDelete = store.computeKvStateHash(0L);
        int keyCountBeforeDelete = store.countVisibleKeys(0L);

        store.delete("hash/delete/a");
        long hashAfterDelete = store.computeKvStateHash(0L);
        int keyCountAfterDelete = store.countVisibleKeys(0L);

        assertNotEquals(hashBeforeDelete, hashAfterDelete);
        assertEquals(2, keyCountBeforeDelete);
        assertEquals(1, keyCountAfterDelete);
    }
}
