package com.xhj.etcd.kernel.etcd.network.support;

import com.xhj.etcd.kernel.etcd.etcdrpc.EtcdRpcResponse;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * EtcdRpcAssert
 *
 * @author XJks
 * @description Netty 分布式场景测试中的 RPC 响应断言工具，统一 success/not-leader/leader-hint 校验逻辑。
 */
public final class EtcdRpcAssert {

    private EtcdRpcAssert() {
    }

    public static void assertSuccess(EtcdRpcResponse<?> response) {
        assertNotNull(response);
        assertNotNull(response.getHeader());
        assertTrue(response.getHeader().isSuccess());
    }

    public static void assertFailed(EtcdRpcResponse<?> response) {
        assertNotNull(response);
        assertNotNull(response.getHeader());
        assertFalse(response.getHeader().isSuccess());
    }

    public static void assertNotLeaderWithLeaderHint(EtcdRpcResponse<?> response) {
        assertNotNull(response);
        assertNotNull(response.getHeader());
        assertFalse(response.getHeader().isSuccess());
        assertTrue(response.getHeader().isNotLeader());
        assertNotNull(response.getHeader().getLeaderId());
    }
}
