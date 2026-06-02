package com.xhj.etcd.sdk.client.lease;

import com.xhj.etcd.kernel.etcd.etcdrpc.LeaseKeepAliveRequest;
import com.xhj.etcd.kernel.etcd.etcdrpc.LeaseKeepAliveResponse;
import com.xhj.etcd.kernel.etcd.etcdrpc.LeaseRevokeRequest;
import com.xhj.etcd.kernel.etcd.etcdrpc.LeaseView;
import com.xhj.etcd.sdk.client.EtcdClient;

import java.util.ArrayList;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * DefaultLeaseHandle
 *
 * @author XJks
 * @description 单个 Lease 自动续约句柄实现。
 */
public class DefaultLeaseHandle implements LeaseHandle {

    /**
     * 最小续约调度间隔，单位：毫秒。
     */
    private static final long MIN_KEEP_ALIVE_INTERVAL_MILLIS = 1000L;

    /**
     * 最大续约调度间隔，单位：毫秒。
     */
    private static final long MAX_KEEP_ALIVE_INTERVAL_MILLIS = 5000L;

    /**
     * 关联客户端。
     */
    private final EtcdClient etcdClient;

    /**
     * LeaseId。
     */
    private final long leaseId;

    /**
     * Lease 生命周期任务调度器（keepAlive/revoke）。
     */
    private final ScheduledExecutorService leaseLifecycleTaskScheduler;

    // ==================== 生命周期状态 ====================

    /**
     * 周期续约任务 future。
     */
    private volatile ScheduledFuture<?> keepAliveFuture;

    /**
     * 句柄是否已关闭。
     */
    private final AtomicBoolean closed = new AtomicBoolean(false);

    /**
     * close 是否默认执行 revoke。
     */
    private final boolean revokeOnClose;

    /**
     * 关闭时回调，用于清理 EtcdClient 内部句柄索引。
     */
    private final Runnable onClosedCallback;

    /**
     * 当前生效的续约调度间隔，单位：毫秒。
     */
    private volatile long currentKeepAliveIntervalMillis;

    /**
     * keepAlive 连续失败次数。
     */
    private final AtomicInteger consecutiveFailureCount = new AtomicInteger(0);

    /**
     * 调度重建互斥锁（保护 keepAliveFuture cancel + rebuild 原子切换）。
     */
    private final Object keepAliveScheduleLock = new Object();

    /**
     * 当前 LeaseView 快照。
     */
    private volatile LeaseView leaseView = new LeaseView();

    public DefaultLeaseHandle(EtcdClient etcdClient,
                              long leaseId,
                              ScheduledExecutorService leaseLifecycleTaskScheduler,
                              boolean revokeOnClose,
                              Runnable onClosedCallback,
                              LeaseKeepAliveResponse bootstrapKeepAliveResponse) {
        if (etcdClient == null) {
            throw new IllegalArgumentException("etcdClient must not be null");
        }
        if (leaseId <= 0L) {
            throw new IllegalArgumentException("leaseId must be positive");
        }
        if (leaseLifecycleTaskScheduler == null) {
            throw new IllegalArgumentException("leaseLifecycleTaskScheduler must not be null");
        }
        if (bootstrapKeepAliveResponse == null || bootstrapKeepAliveResponse.getLease() == null) {
            throw new IllegalArgumentException("bootstrapKeepAliveResponse must not be null");
        }
        this.etcdClient = etcdClient;
        this.leaseId = leaseId;
        this.leaseLifecycleTaskScheduler = leaseLifecycleTaskScheduler;
        this.revokeOnClose = revokeOnClose;
        this.onClosedCallback = onClosedCallback;
        this.currentKeepAliveIntervalMillis = computeKeepAliveIntervalMillis(bootstrapKeepAliveResponse);
        refreshLeaseView(bootstrapKeepAliveResponse.getLease());
    }

    /**
     * 启动自动续约调度。
     */
    public void startAutoKeepAlive() {
        synchronized (keepAliveScheduleLock) {
            /**
             * TODO:
             *  启动流程：
             *  1) closed=true：句柄已关闭，直接返回；
             *  2) keepAliveFuture!=null：周期任务已存在，直接返回；
             *  3) 两个条件都不满足时，按 currentKeepAliveIntervalMillis 创建周期任务。
             */
            if (closed.get() || keepAliveFuture != null) {
                return;
            }
            scheduleWithIntervalInLock(currentKeepAliveIntervalMillis);
        }
    }

    @Override
    public long getLeaseId() {
        return leaseId;
    }

    @Override
    public boolean isClosed() {
        return closed.get();
    }

    @Override
    public LeaseView getLeaseView() {
        LeaseView currentLeaseView = leaseView;
        LeaseView copiedLeaseView = new LeaseView();
        copiedLeaseView.setLeaseId(currentLeaseView.getLeaseId());
        copiedLeaseView.setTtlSeconds(currentLeaseView.getTtlSeconds());
        copiedLeaseView.setRemainingSeconds(currentLeaseView.getRemainingSeconds());
        copiedLeaseView.setKeys(new ArrayList<>(currentLeaseView.getKeys()));
        return copiedLeaseView;
    }

    @Override
    public void refreshLeaseView(LeaseView sourceLeaseView) {
        if (sourceLeaseView == null) {
            return;
        }
        LeaseView copiedLeaseView = new LeaseView();
        copiedLeaseView.setLeaseId(sourceLeaseView.getLeaseId());
        copiedLeaseView.setTtlSeconds(sourceLeaseView.getTtlSeconds());
        copiedLeaseView.setRemainingSeconds(sourceLeaseView.getRemainingSeconds());
        copiedLeaseView.setKeys(sourceLeaseView.getKeys() == null ? new ArrayList<String>() : new ArrayList<>(sourceLeaseView.getKeys()));
        leaseView = copiedLeaseView;
    }

    @Override
    public void close() {
        // 1) closed 切到终态；
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        // 2) 在 keepAliveScheduleLock 下取消周期任务；
        synchronized (keepAliveScheduleLock) {
            cancelKeepAliveFutureInLock();
        }
        // 3) 回调清理 EtcdClient 中的句柄索引；
        if (onClosedCallback != null) {
            onClosedCallback.run();
        }
        // 4) 异步提交 revoke（不阻塞 close 返回）。
        submitRevokeTaskOnClose();
    }

    /**
     * 执行一次 keepAlive 并决定下一次调度。
     */
    private void runKeepAliveRound() {
        if (closed.get()) {
            return;
        }
        try {
            LeaseKeepAliveResponse keepAliveResponse = etcdClient.leaseKeepAlive(new LeaseKeepAliveRequest(leaseId));
            if (shouldStopOnKeepAliveResponse(keepAliveResponse)) {
                close();
                return;
            }
            refreshLeaseView(keepAliveResponse.getLease());
            // TODO: keepAlive 成功后把连续失败计数清零。
            consecutiveFailureCount.set(0);
            refreshKeepAliveIntervalIfNeeded(keepAliveResponse);
        } catch (Throwable throwable) {
            int failureCount = consecutiveFailureCount.incrementAndGet();
            // TODO: 连续失败达到 3 次时关闭句柄。
            if (failureCount >= 3) {
                close();
            }
        }
    }

    /**
     * 根据租约 TTL 计算固定 keepAlive 调度间隔。
     */
    private long computeKeepAliveIntervalMillis(LeaseKeepAliveResponse keepAliveResponse) {
        LeaseView currentLeaseView = keepAliveResponse == null ? null : keepAliveResponse.getLease();
        if (currentLeaseView == null) {
            return MIN_KEEP_ALIVE_INTERVAL_MILLIS;
        }
        long ttlSeconds = currentLeaseView.getRemainingSeconds() > 0L ? currentLeaseView.getRemainingSeconds() : currentLeaseView.getTtlSeconds();
        long candidateDelayMillis = (ttlSeconds * 1000L) / 3L;
        if (candidateDelayMillis <= 0L) {
            return MIN_KEEP_ALIVE_INTERVAL_MILLIS;
        }
        if (candidateDelayMillis > MAX_KEEP_ALIVE_INTERVAL_MILLIS) {
            return MAX_KEEP_ALIVE_INTERVAL_MILLIS;
        }
        return Math.max(MIN_KEEP_ALIVE_INTERVAL_MILLIS, candidateDelayMillis);
    }

    /**
     * keepAlive 响应是否表示租约已失效并需要自动停止句柄。
     */
    private boolean shouldStopOnKeepAliveResponse(LeaseKeepAliveResponse keepAliveResponse) {
        if (keepAliveResponse == null || keepAliveResponse.getLease() == null) {
            return true;
        }
        LeaseView currentLeaseView = keepAliveResponse.getLease();
        // TODO:服务端若返回剩余 TTL <= 0，说明租约已经不可续约（已到期或已失效）。
        return currentLeaseView.getRemainingSeconds() <= 0L;
    }

    /**
     * 更新续约周期（TTL/3），在阈值变化达到 20% 时重建调度任务。
     */
    private void refreshKeepAliveIntervalIfNeeded(LeaseKeepAliveResponse keepAliveResponse) {
        long nextKeepAliveIntervalMillis = computeKeepAliveIntervalMillis(keepAliveResponse);
        long currentIntervalMillis = currentKeepAliveIntervalMillis;
        if (!shouldRebuildKeepAliveSchedule(currentIntervalMillis, nextKeepAliveIntervalMillis)) {
            return;
        }
        synchronized (keepAliveScheduleLock) {
            if (closed.get()) {
                return;
            }
            // TODO:锁内再次读取 currentKeepAliveIntervalMillis 并判断：若其他线程已完成间隔切换，则本轮直接返回。
            long latestIntervalMillis = currentKeepAliveIntervalMillis;
            if (!shouldRebuildKeepAliveSchedule(latestIntervalMillis, nextKeepAliveIntervalMillis)) {
                return;
            }
            scheduleWithIntervalInLock(nextKeepAliveIntervalMillis);
        }
    }

    /**
     * 判断是否需要重建 keepAlive 调度任务。
     */
    private boolean shouldRebuildKeepAliveSchedule(long currentIntervalMillis, long nextIntervalMillis) {
        if (currentIntervalMillis <= 0L) {
            return true;
        }
        long intervalDelta = Math.abs(nextIntervalMillis - currentIntervalMillis);
        /**
         * TODO:
         *  周期变化阈值为 20%：
         *  1) 低于 20%：沿用当前任务；
         *  2) 大于等于 20%：取消旧任务并重建新任务。
         */
        return intervalDelta * 100L >= currentIntervalMillis * 20L;
    }

    /**
     * 取消当前周期任务并按新间隔重建（原子切换）。
     */
    private void scheduleWithIntervalInLock(long keepAliveIntervalMillis) {
        cancelKeepAliveFutureInLock();
        keepAliveFuture = leaseLifecycleTaskScheduler.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                runKeepAliveRound();
            }
        }, keepAliveIntervalMillis, keepAliveIntervalMillis, TimeUnit.MILLISECONDS);
        currentKeepAliveIntervalMillis = keepAliveIntervalMillis;
    }

    /**
     * 在已持有 keepAliveScheduleLock 的前提下取消 keepAlive 任务。
     */
    private void cancelKeepAliveFutureInLock() {
        ScheduledFuture<?> scheduledFuture = keepAliveFuture;
        keepAliveFuture = null;
        if (scheduledFuture != null) {
            scheduledFuture.cancel(false);
        }
    }

    /**
     * close 阶段提交 revoke（同池最佳努力）。
     */
    private void submitRevokeTaskOnClose() {
        if (!revokeOnClose || leaseId <= 0L) {
            return;
        }
        try {
            leaseLifecycleTaskScheduler.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        etcdClient.leaseRevoke(new LeaseRevokeRequest(leaseId));
                    } catch (Throwable ignore) {
                        // close 收敛阶段不抛异常；revoke 失败由上层按业务需要重试。
                    }
                }
            });
        } catch (RejectedExecutionException ignore) {
            // TODO:调度池关闭时，revoke 任务提交失败，close 只做本地收敛。
        }
    }
}
