package com.xhj.etcd.sdk.client.lease;

import com.xhj.etcd.kernel.etcd.etcdrpc.LeaseKeepAliveRequest;
import com.xhj.etcd.kernel.etcd.etcdrpc.LeaseKeepAliveResponse;
import com.xhj.etcd.kernel.etcd.etcdrpc.LeaseRevokeRequest;
import com.xhj.etcd.kernel.etcd.etcdrpc.LeaseRevokeResponse;
import com.xhj.etcd.kernel.etcd.etcdrpc.LeaseView;
import com.xhj.etcd.rpc.NodeEndpoint;
import com.xhj.etcd.rpc.RpcClient;
import com.xhj.etcd.rpc.RpcMessageHandler;
import com.xhj.etcd.sdk.client.EtcdClient;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * DefaultLeaseHandleTest
 *
 * @author XJks
 * @description LeaseHandle 自动续约调度模型模块测试。
 */
public class DefaultLeaseHandleTest {

    @Test
    public void shouldStartAutoKeepAliveOnlyOnceWhenConcurrentStart() throws Exception {
        FakeEtcdClient fakeEtcdClient = new FakeEtcdClient(buildKeepAliveResponse(1001L, 3L, 3L));
        RecordingScheduledThreadPoolExecutor scheduler = new RecordingScheduledThreadPoolExecutor(2);
        AtomicInteger onClosedCount = new AtomicInteger(0);
        DefaultLeaseHandle leaseHandle = new DefaultLeaseHandle(
                fakeEtcdClient,
                1001L,
                scheduler,
                false,
                incrementCounterCallback(onClosedCount),
                buildKeepAliveResponse(1001L, 3L, 3L));

        int threadCount = 16;
        CountDownLatch readyLatch = new CountDownLatch(threadCount);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch finishLatch = new CountDownLatch(threadCount);
        for (int i = 0; i < threadCount; i++) {
            Thread thread = new Thread(new Runnable() {
                @Override
                public void run() {
                    readyLatch.countDown();
                    awaitLatch(startLatch);
                    leaseHandle.startAutoKeepAlive();
                    finishLatch.countDown();
                }
            }, "lease-start-thread-" + i);
            thread.start();
        }
        Assert.assertTrue("start threads are not ready", readyLatch.await(3L, TimeUnit.SECONDS));
        startLatch.countDown();
        Assert.assertTrue("start threads did not finish", finishLatch.await(3L, TimeUnit.SECONDS));

        Assert.assertEquals("startAutoKeepAlive should only schedule one periodic task", 1, scheduler.getScheduleWithFixedDelayCount());
        leaseHandle.close();
        Assert.assertEquals("close callback should execute once", 1, onClosedCount.get());

        scheduler.shutdownNow();
        fakeEtcdClient.close();
    }

    @Test
    public void shouldRebuildKeepAliveScheduleWhenIntervalDeltaReachesTwentyPercent() throws Exception {
        FakeEtcdClient fakeEtcdClient = new FakeEtcdClient(buildKeepAliveResponse(1002L, 6L, 6L));
        RecordingScheduledThreadPoolExecutor scheduler = new RecordingScheduledThreadPoolExecutor(1);
        DefaultLeaseHandle leaseHandle = new DefaultLeaseHandle(
                fakeEtcdClient,
                1002L,
                scheduler,
                false,
                null,
                buildKeepAliveResponse(1002L, 6L, 6L));

        leaseHandle.startAutoKeepAlive();
        Assert.assertEquals("initial schedule should be created", 1, scheduler.getScheduleWithFixedDelayCount());
        Assert.assertEquals("initial keepAlive interval mismatch", 2000L, scheduler.getLastDelayMillis());

        invokeRefreshKeepAliveIntervalIfNeeded(leaseHandle, buildKeepAliveResponse(1002L, 10L, 10L));
        Assert.assertEquals("interval >=20% should trigger schedule rebuild", 2, scheduler.getScheduleWithFixedDelayCount());
        Assert.assertEquals("rebuild interval should follow TTL/3", 3333L, scheduler.getLastDelayMillis());

        leaseHandle.close();
        scheduler.shutdownNow();
        fakeEtcdClient.close();
    }

    @Test
    public void shouldNotRebuildKeepAliveScheduleWhenIntervalDeltaBelowTwentyPercent() throws Exception {
        FakeEtcdClient fakeEtcdClient = new FakeEtcdClient(buildKeepAliveResponse(1003L, 6L, 6L));
        RecordingScheduledThreadPoolExecutor scheduler = new RecordingScheduledThreadPoolExecutor(1);
        DefaultLeaseHandle leaseHandle = new DefaultLeaseHandle(
                fakeEtcdClient,
                1003L,
                scheduler,
                false,
                null,
                buildKeepAliveResponse(1003L, 6L, 6L));

        leaseHandle.startAutoKeepAlive();
        Assert.assertEquals("initial schedule should be created", 1, scheduler.getScheduleWithFixedDelayCount());
        invokeRefreshKeepAliveIntervalIfNeeded(leaseHandle, buildKeepAliveResponse(1003L, 5L, 5L));
        Assert.assertEquals("interval <20% should not rebuild schedule", 1, scheduler.getScheduleWithFixedDelayCount());

        leaseHandle.close();
        scheduler.shutdownNow();
        fakeEtcdClient.close();
    }

    @Test
    public void shouldCloseHandleAfterThreeConsecutiveKeepAliveFailures() throws Exception {
        FakeEtcdClient fakeEtcdClient = new FakeEtcdClient(buildKeepAliveResponse(1004L, 3L, 3L));
        fakeEtcdClient.enqueueKeepAliveThrowable(new RuntimeException("failure-1"));
        fakeEtcdClient.enqueueKeepAliveThrowable(new RuntimeException("failure-2"));
        fakeEtcdClient.enqueueKeepAliveThrowable(new RuntimeException("failure-3"));
        RecordingScheduledThreadPoolExecutor scheduler = new RecordingScheduledThreadPoolExecutor(1);
        DefaultLeaseHandle leaseHandle = new DefaultLeaseHandle(
                fakeEtcdClient,
                1004L,
                scheduler,
                false,
                null,
                buildKeepAliveResponse(1004L, 3L, 3L));

        invokeRunKeepAliveRound(leaseHandle);
        Assert.assertFalse("first failure should not close handle", leaseHandle.isClosed());
        invokeRunKeepAliveRound(leaseHandle);
        Assert.assertFalse("second failure should not close handle", leaseHandle.isClosed());
        invokeRunKeepAliveRound(leaseHandle);
        Assert.assertTrue("third consecutive failure should close handle", leaseHandle.isClosed());

        scheduler.shutdownNow();
        fakeEtcdClient.close();
    }

    @Test
    public void shouldCloseHandleImmediatelyWhenKeepAliveResponseIsInvalid() throws Exception {
        FakeEtcdClient fakeEtcdClient = new FakeEtcdClient(buildKeepAliveResponse(1005L, 3L, 3L));
        fakeEtcdClient.enqueueKeepAliveResponse(buildKeepAliveResponse(1005L, 3L, 0L));
        RecordingScheduledThreadPoolExecutor scheduler = new RecordingScheduledThreadPoolExecutor(1);
        DefaultLeaseHandle leaseHandle = new DefaultLeaseHandle(
                fakeEtcdClient,
                1005L,
                scheduler,
                false,
                null,
                buildKeepAliveResponse(1005L, 3L, 3L));

        invokeRunKeepAliveRound(leaseHandle);
        Assert.assertTrue("remainingSeconds<=0 should close handle immediately", leaseHandle.isClosed());

        scheduler.shutdownNow();
        fakeEtcdClient.close();
    }

    @Test
    public void shouldBeIdempotentOnConcurrentCloseAndSubmitRevokeOnScheduler() throws Exception {
        FakeEtcdClient fakeEtcdClient = new FakeEtcdClient(buildKeepAliveResponse(1006L, 3L, 3L));
        RecordingScheduledThreadPoolExecutor scheduler = new RecordingScheduledThreadPoolExecutor(2);
        AtomicInteger onClosedCount = new AtomicInteger(0);
        DefaultLeaseHandle leaseHandle = new DefaultLeaseHandle(
                fakeEtcdClient,
                1006L,
                scheduler,
                true,
                incrementCounterCallback(onClosedCount),
                buildKeepAliveResponse(1006L, 3L, 3L));
        leaseHandle.startAutoKeepAlive();

        int threadCount = 16;
        CountDownLatch closeDoneLatch = new CountDownLatch(threadCount);
        for (int i = 0; i < threadCount; i++) {
            Thread thread = new Thread(new Runnable() {
                @Override
                public void run() {
                    leaseHandle.close();
                    closeDoneLatch.countDown();
                }
            }, "lease-close-thread-" + i);
            thread.start();
        }
        Assert.assertTrue("concurrent close did not finish", closeDoneLatch.await(5L, TimeUnit.SECONDS));
        waitUntilTrue(new Condition() {
            @Override
            public boolean test() {
                return fakeEtcdClient.getLeaseRevokeCallCount() == 1;
            }
        }, 3000L);

        Assert.assertTrue("lease handle should be closed", leaseHandle.isClosed());
        Assert.assertEquals("close callback should run once", 1, onClosedCount.get());
        Assert.assertEquals("lease revoke should be submitted once", 1, fakeEtcdClient.getLeaseRevokeCallCount());

        scheduler.shutdownNow();
        fakeEtcdClient.close();
    }

    // ==================== 反射调用工具 ====================

    private static void invokeRunKeepAliveRound(DefaultLeaseHandle leaseHandle) throws Exception {
        Method runKeepAliveRoundMethod = DefaultLeaseHandle.class.getDeclaredMethod("runKeepAliveRound");
        runKeepAliveRoundMethod.setAccessible(true);
        runKeepAliveRoundMethod.invoke(leaseHandle);
    }

    private static void invokeRefreshKeepAliveIntervalIfNeeded(DefaultLeaseHandle leaseHandle,
                                                               LeaseKeepAliveResponse keepAliveResponse) throws Exception {
        Method refreshKeepAliveIntervalMethod = DefaultLeaseHandle.class.getDeclaredMethod(
                "refreshKeepAliveIntervalIfNeeded",
                LeaseKeepAliveResponse.class);
        refreshKeepAliveIntervalMethod.setAccessible(true);
        refreshKeepAliveIntervalMethod.invoke(leaseHandle, keepAliveResponse);
    }

    private static LeaseKeepAliveResponse buildKeepAliveResponse(long leaseId, long ttlSeconds, long remainingSeconds) {
        LeaseView leaseView = new LeaseView();
        leaseView.setLeaseId(leaseId);
        leaseView.setTtlSeconds(ttlSeconds);
        leaseView.setRemainingSeconds(remainingSeconds);
        return LeaseKeepAliveResponse.of(leaseView);
    }

    private static Runnable incrementCounterCallback(AtomicInteger counter) {
        return new Runnable() {
            @Override
            public void run() {
                counter.incrementAndGet();
            }
        };
    }

    private static void awaitLatch(CountDownLatch countDownLatch) {
        try {
            countDownLatch.await();
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(exception);
        }
    }

    private static void waitUntilTrue(Condition condition, long timeoutMillis) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (System.currentTimeMillis() < deadline) {
            if (condition.test()) {
                return;
            }
            Thread.sleep(20L);
        }
        throw new AssertionError("condition is not true within timeout");
    }

    private interface Condition {
        boolean test();
    }

    // ==================== 测试桩实现 ====================

    private static class RecordingScheduledThreadPoolExecutor extends ScheduledThreadPoolExecutor {

        private final AtomicInteger scheduleWithFixedDelayCount = new AtomicInteger(0);

        private final List<Long> scheduledDelayMillisList = new ArrayList<>();

        RecordingScheduledThreadPoolExecutor(int corePoolSize) {
            super(corePoolSize);
            setRemoveOnCancelPolicy(true);
        }

        @Override
        public ScheduledFuture<?> scheduleWithFixedDelay(Runnable command,
                                                         long initialDelay,
                                                         long delay,
                                                         TimeUnit unit) {
            scheduleWithFixedDelayCount.incrementAndGet();
            synchronized (scheduledDelayMillisList) {
                scheduledDelayMillisList.add(unit.toMillis(delay));
            }
            return super.scheduleWithFixedDelay(command, initialDelay, delay, unit);
        }

        int getScheduleWithFixedDelayCount() {
            return scheduleWithFixedDelayCount.get();
        }

        long getLastDelayMillis() {
            synchronized (scheduledDelayMillisList) {
                if (scheduledDelayMillisList.isEmpty()) {
                    return -1L;
                }
                return scheduledDelayMillisList.get(scheduledDelayMillisList.size() - 1);
            }
        }
    }

    private static class FakeEtcdClient extends EtcdClient {

        private final ConcurrentLinkedQueue<Object> keepAliveOutcomeQueue = new ConcurrentLinkedQueue<>();

        private final AtomicInteger leaseRevokeCallCount = new AtomicInteger(0);

        private volatile LeaseKeepAliveResponse defaultKeepAliveResponse;

        FakeEtcdClient(LeaseKeepAliveResponse defaultKeepAliveResponse) {
            super(new NoopRpcClient(), buildSingleNodeEndpointList());
            this.defaultKeepAliveResponse = defaultKeepAliveResponse;
        }

        void enqueueKeepAliveResponse(LeaseKeepAliveResponse keepAliveResponse) {
            keepAliveOutcomeQueue.add(keepAliveResponse);
        }

        void enqueueKeepAliveThrowable(Throwable throwable) {
            keepAliveOutcomeQueue.add(throwable);
        }

        int getLeaseRevokeCallCount() {
            return leaseRevokeCallCount.get();
        }

        @Override
        public LeaseKeepAliveResponse leaseKeepAlive(LeaseKeepAliveRequest request) {
            Object outcome = keepAliveOutcomeQueue.poll();
            if (outcome == null) {
                return defaultKeepAliveResponse;
            }
            if (outcome instanceof RuntimeException) {
                throw (RuntimeException) outcome;
            }
            if (outcome instanceof Error) {
                throw (Error) outcome;
            }
            if (outcome instanceof Throwable) {
                throw new RuntimeException((Throwable) outcome);
            }
            return (LeaseKeepAliveResponse) outcome;
        }

        @Override
        public LeaseRevokeResponse leaseRevoke(LeaseRevokeRequest request) {
            leaseRevokeCallCount.incrementAndGet();
            return LeaseRevokeResponse.of(request.getLeaseId(), 0, 0L);
        }
    }

    private static class NoopRpcClient implements RpcClient {

        @Override
        public <T> T call(NodeEndpoint endpoint, String serviceName, String methodName, Object request, Class<T> responseClass) {
            return null;
        }

        @Override
        public void send(NodeEndpoint endpoint, String serviceName, String methodName, Object request) {
        }

        @Override
        public void sendRequestWithRpcMessageId(NodeEndpoint endpoint,
                                                String serviceName,
                                                String methodName,
                                                Object request,
                                                String rpcMessageId,
                                                RpcMessageHandler handler) {
        }

        @Override
        public void removeRpcMessageHandler(String rpcMessageId) {
        }

        @Override
        public boolean heartbeat(NodeEndpoint endpoint) {
            return false;
        }

        @Override
        public void shutdown() {
        }
    }

    private static List<NodeEndpoint> buildSingleNodeEndpointList() {
        List<NodeEndpoint> endpointList = new ArrayList<>();
        endpointList.add(new NodeEndpoint("n1", "127.0.0.1", 1));
        return endpointList;
    }
}
