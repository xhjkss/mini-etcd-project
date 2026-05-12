package com.xhj.etcd.kernel.etcd.module.store;

import com.xhj.etcd.kernel.etcd.store.KeyValueStore;
import com.xhj.etcd.kernel.etcd.store.KeyValueStoreSnapshot;
import com.xhj.etcd.kernel.etcd.store.KeyValueRecord;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * KeyValueStoreSnapshotTest
 *
 * @author XJks
 * @description KeyValueStore 快照测试。
 */
public class KeyValueStoreSnapshotTest {

    /**
     * 验证快照结构按 historyByKey 保存，并且恢复后 MVCC 读取语义保持一致。
     */
    @Test
    public void testSnapshotHistoryByKeyAndRestore() {
        KeyValueStore sourceStore = new KeyValueStore();

        // 1) 构造包含多版本写入与删除墓碑的历史数据。
        sourceStore.put("k1", "v1");
        sourceStore.put("k1", "v2");
        sourceStore.put("k2", "v3");
        sourceStore.delete("k1");

        // 2) 校验快照按 key 维度保存历史记录。
        KeyValueStoreSnapshot snapshot = sourceStore.createSnapshot();
        assertNotNull(snapshot);
        assertEquals(4L, snapshot.getRevision());
        assertNotNull(snapshot.getHistoryByKey());
        assertTrue(snapshot.getHistoryByKey().containsKey("k1"));
        assertTrue(snapshot.getHistoryByKey().containsKey("k2"));
        assertEquals(3, snapshot.getHistoryByKey().get("k1").size());
        assertEquals(1, snapshot.getHistoryByKey().get("k2").size());

        // 3) 恢复新状态机后，校验 revision、历史读取和墓碑可见性。
        KeyValueStore restoredStore = new KeyValueStore();
        restoredStore.restoreSnapshot(snapshot);

        assertEquals(4L, restoredStore.currentRevision());

        KeyValueRecord k1AtRevision2 = restoredStore.get("k1", 2L);
        assertNotNull(k1AtRevision2);
        assertEquals("v2", k1AtRevision2.getValue());
        assertEquals(2L, k1AtRevision2.getVersion());

        KeyValueRecord k1AtRevision4 = restoredStore.get("k1", 4L);
        assertNull(k1AtRevision4);

        KeyValueRecord k2Latest = restoredStore.get("k2", 0L);
        assertNotNull(k2Latest);
        assertEquals("v3", k2Latest.getValue());
    }
}
