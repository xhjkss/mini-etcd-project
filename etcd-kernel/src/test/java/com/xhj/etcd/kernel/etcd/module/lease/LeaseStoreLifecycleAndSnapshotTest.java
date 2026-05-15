package com.xhj.etcd.kernel.etcd.module.lease;

import com.xhj.etcd.kernel.etcd.store.lease.LeaseRecord;
import com.xhj.etcd.kernel.etcd.store.lease.LeaseStore;
import com.xhj.etcd.kernel.etcd.store.lease.LeaseStoreSnapshot;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

/**
 * LeaseStoreLifecycleAndSnapshotTest
 *
 * @author XJks
 * @description LeaseStore 单模块测试，覆盖 grant / keepAlive / revoke / expired scan / snapshot restore。
 */
public class LeaseStoreLifecycleAndSnapshotTest {

    @Test
    public void shouldGrantKeepAliveRevokeAndRestoreSnapshot() {
        LeaseStore store = new LeaseStore();

        LeaseRecord firstLease = store.grant(0L, 1L, 1000L);
        LeaseRecord secondLease = store.grant(0L, 3L, 1000L);
        store.attachKey(firstLease.getLeaseId(), "lease/a");
        store.attachKey(secondLease.getLeaseId(), "lease/b");

        LeaseRecord keepAliveLease = store.keepAlive(secondLease.getLeaseId(), 2000L);
        assertEquals(1L, firstLease.getLeaseId());
        assertEquals(2L, secondLease.getLeaseId());
        assertEquals(1L, firstLease.getTtlSeconds());
        assertEquals(3L, secondLease.getTtlSeconds());
        assertEquals(5000L, keepAliveLease.getDeadlineMillis());
        assertEquals(3L, keepAliveLease.remainingSeconds(2500L));

        List<Long> expiredLeaseIds = store.collectExpiredLeaseIds(2500L);
        assertEquals(1, expiredLeaseIds.size());
        assertEquals(firstLease.getLeaseId(), expiredLeaseIds.get(0).longValue());

        LeaseStoreSnapshot snapshot = store.createSnapshot();
        LeaseStore restoredStore = new LeaseStore();
        restoredStore.restoreSnapshot(snapshot);

        assertEquals(store.nextLeaseId(), restoredStore.nextLeaseId());
        assertEquals(2, restoredStore.size());
        assertEquals(1, restoredStore.attachedKeyCount(firstLease.getLeaseId()));
        assertEquals(1, restoredStore.attachedKeyCount(secondLease.getLeaseId()));
        assertEquals(1, restoredStore.collectExpiredLeaseIds(2500L).size());

        LeaseRecord revokedLease = restoredStore.revoke(firstLease.getLeaseId());
        assertNotNull(revokedLease);
        assertEquals(firstLease.getLeaseId(), revokedLease.getLeaseId());
        assertEquals(0, restoredStore.attachedKeyCount(firstLease.getLeaseId()));
        assertNull(restoredStore.ttl(firstLease.getLeaseId(), 3000L));
        assertEquals(1, restoredStore.list(3000L).size());
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldRejectNonPositiveTtlWhenGrantingLease() {
        LeaseStore store = new LeaseStore();
        store.grant(0L, 0L, 1000L);
    }

    @Test
    public void shouldGrantExplicitLeaseIdAndKeepExpiredScanOrderStable() {
        LeaseStore store = new LeaseStore();

        LeaseRecord explicitLease = store.grant(10L, 1L, 1000L);
        LeaseRecord laterLease = store.grant(0L, 3L, 1000L);
        store.attachKey(explicitLease.getLeaseId(), "lease/explicit");
        store.attachKey(laterLease.getLeaseId(), "lease/later");
        store.detachKey(laterLease.getLeaseId(), "lease/not-exist");

        assertEquals(10L, explicitLease.getLeaseId());
        assertEquals(11L, laterLease.getLeaseId());
        assertEquals(2, store.size());
        assertEquals(1, store.attachedKeyCount(explicitLease.getLeaseId()));
        assertEquals(1, store.attachedKeyCount(laterLease.getLeaseId()));

        List<Long> expiredLeaseIds = store.collectExpiredLeaseIds(2500L);
        assertEquals(1, expiredLeaseIds.size());
        assertEquals(explicitLease.getLeaseId(), expiredLeaseIds.get(0).longValue());

        LeaseRecord revokedLease = store.revoke(explicitLease.getLeaseId());
        assertNotNull(revokedLease);
        assertNull(store.revoke(explicitLease.getLeaseId()));
        assertEquals(0, store.attachedKeyCount(explicitLease.getLeaseId()));
    }
}
