package com.xhj.etcd.rpc.core;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * RpcServiceRegistry
 *
 * @author XJks
 * @description RPC 服务注册表，负责维护服务名到服务定义的映射。
 */
public class RpcServiceRegistry {

    /**
     * 服务定义映射。
     *
     * <p>key 为 RPC 服务名，value 为该服务对应的服务实例和可调用方法定义。</p>
     *
     * <p>TODO: 服务注册表只负责服务名路由，不负责请求 ID 分发；客户端响应分发使用 rpcMessageId，服务端请求分发使用 serviceName + methodName。</p>
     */
    private final Map<String, RpcServiceDefinition> serviceMap = new ConcurrentHashMap<>();

    // ==================== Register service ====================

    /**
     * 注册 RPC 服务。
     *
     * <p>注册时必须显式指定允许远程调用的方法名集合，框架不会自动暴露 serviceObject 的所有 public 方法。</p>
     *
     * <p>如果重复注册相同 serviceName，新的 RpcServiceDefinition 会覆盖旧定义。</p>
     *
     * @param serviceName   RPC 服务名
     * @param serviceObject 服务实例对象
     * @param methodNames   允许远程调用的方法名列表
     */
    public void registerService(String serviceName, Object serviceObject, String... methodNames) {
        if (serviceName == null || serviceName.trim().length() == 0) {
            throw new IllegalArgumentException("serviceName must not be empty");
        }
        if (serviceObject == null) {
            throw new IllegalArgumentException("serviceObject must not be null");
        }
        Set<String> methodNameSet = toMethodNameSet(methodNames);
        if (methodNameSet.isEmpty()) {
            throw new IllegalArgumentException("methodNames must not be empty");
        }

        // 根据服务实例和显式导出的方法名构建服务定义，后续请求会通过 serviceName 定位到该定义。
        serviceMap.put(serviceName, new RpcServiceDefinition(serviceObject, methodNameSet));
    }

    // ==================== Get service ====================

    /**
     * 根据服务名获取服务定义。
     *
     * <p>RpcRequestExecutor 会先根据 RpcMessage.serviceName 获取服务定义，再从服务定义中根据 RpcMessage.methodName 获取具体方法定义。</p>
     *
     * @param serviceName RPC 服务名
     * @return 服务定义；不存在时返回 null
     */
    public RpcServiceDefinition getServiceDefinition(String serviceName) {
        return serviceMap.get(serviceName);
    }

    /**
     * 将方法名数组转换为去重后的方法名集合。
     *
     * <p>空字符串和 null 会被忽略；有效方法名会先 trim 再加入集合，因此重复方法名只会保留一份。</p>
     *
     * @param methodNames 方法名数组
     * @return 去重后的方法名集合
     */
    private Set<String> toMethodNameSet(String... methodNames) {
        Set<String> methodNameSet = new HashSet<>();
        if (methodNames == null) {
            return methodNameSet;
        }
        for (String methodName : methodNames) {
            if (methodName == null || methodName.trim().length() == 0) {
                continue;
            }
            methodNameSet.add(methodName.trim());
        }
        return methodNameSet;
    }
}