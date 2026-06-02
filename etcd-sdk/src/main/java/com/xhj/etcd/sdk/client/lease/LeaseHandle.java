package com.xhj.etcd.sdk.client.lease;

import com.xhj.etcd.kernel.etcd.etcdrpc.LeaseView;

/**
 * LeaseHandle
 *
 * @author XJks
 * @description Lease 自动续约句柄。
 */
public interface LeaseHandle {

    /**
     * 获取 leaseId。
     */
    long getLeaseId();

    /**
     * 是否已关闭。
     */
    boolean isClosed();

    /**
     * 获取当前 LeaseView。
     */
    LeaseView getLeaseView();

    /**
     * 用租约视图刷新本地 LeaseView。
     */
    void refreshLeaseView(LeaseView leaseView);

    /**
     * 关闭自动续约。
     *
     * <p>
     * TODO:
     *  close 一定会停止 keepAlive 调度并收敛到 CLOSED。
     *  是否自动 revoke 由句柄创建模式决定：
     *  1) startLeaseKeepAlive(...)：默认不 revoke，避免误删外部持有租约上的业务 key。
     *  2) grantAndStartLeaseKeepAlive(...)：默认 revoke，释放由该句柄创建并托管的租约。
     * </p>
     */
    void close();
}
