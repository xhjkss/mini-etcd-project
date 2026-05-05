package com.xhj.etcd.rpc;

/**
 * RpcServer
 *
 * @author XJks
 * @description RPC 服务端接口，定义服务注册、启动和关闭的生命周期边界。
 */
public interface RpcServer {

    // ==================== 服务注册 ====================

    /**
     * 注册可被远程调用的 RPC 服务。
     */
    void registerService(String serviceName, Object serviceObject, String... methodNames);

    // ==================== 生命周期 ====================

    /**
     * 启动当前组件。
     */
    void start();

    /**
     * 停止当前组件并释放端口或线程资源。
     */
    void stop();
}
