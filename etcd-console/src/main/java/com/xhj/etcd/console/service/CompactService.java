package com.xhj.etcd.console.service;

import com.xhj.etcd.kernel.etcd.etcdrpc.CompactRequest;
import com.xhj.etcd.kernel.etcd.etcdrpc.CompactResponse;
import com.xhj.etcd.sdk.client.EtcdClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * CompactService
 *
 * @author XJks
 * @description Compact 控制台服务。
 */
@Service
public class CompactService {

    // ==================== 依赖组件 ====================
    /**
     * 连接服务。
     */
    @Autowired
    private ConnectionService connectionService;

    /**
     * 执行 compact 请求。
     *
     * @param compactRequest compact 请求
     * @return compact 响应
     */
    public CompactResponse compact(CompactRequest compactRequest) {
        if (compactRequest == null) {
            throw new IllegalArgumentException("compactRequest must not be null");
        }
        EtcdClient etcdClient = connectionService.getEtcdClient();
        return etcdClient.compact(compactRequest);
    }
}
