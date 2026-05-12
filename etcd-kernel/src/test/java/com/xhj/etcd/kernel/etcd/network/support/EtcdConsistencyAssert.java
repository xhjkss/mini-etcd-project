package com.xhj.etcd.kernel.etcd.network.support;

import com.xhj.etcd.kernel.etcd.etcdrpc.GetResponse;
import com.xhj.etcd.kernel.etcd.etcdrpc.KeyValueView;
import com.xhj.etcd.kernel.etcd.etcdrpc.RangeResponse;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * EtcdConsistencyAssert
 *
 * @author XJks
 * @description 分布式一致性断言工具，统一 key 可见性和前缀查询语义校验。
 */
public final class EtcdConsistencyAssert {

    private EtcdConsistencyAssert() {
    }

    public static void assertGetValueEquals(String messagePrefix, String expectedValue, GetResponse actualResponse) {
        assertNotNull(messagePrefix + " response is null", actualResponse);
        assertEquals(messagePrefix + " value mismatch", expectedValue, actualResponse.getValue());
    }

    public static void assertPrefixRangeEquals(String messagePrefix,
                                               String prefix,
                                               Map<String, String> expectedValueByKey,
                                               RangeResponse actualRangeResponse) {
        assertNotNull(messagePrefix + " range response is null", actualRangeResponse);
        Set<String> expectedKeys = new HashSet<>();
        for (Map.Entry<String, String> entry : expectedValueByKey.entrySet()) {
            if (entry.getKey().startsWith(prefix)) {
                expectedKeys.add(entry.getKey());
            }
        }

        Set<String> actualKeys = new HashSet<>();
        if (actualRangeResponse.getItems() != null) {
            for (KeyValueView item : actualRangeResponse.getItems()) {
                actualKeys.add(item.getKey());
                assertEquals(messagePrefix + " prefix item value mismatch, key=" + item.getKey(),
                        expectedValueByKey.get(item.getKey()),
                        item.getValue());
            }
        }
        assertEquals(messagePrefix + " prefix keys mismatch", expectedKeys, actualKeys);
        assertEquals(messagePrefix + " prefix count mismatch", expectedKeys.size(), actualRangeResponse.getCount());
    }
}
