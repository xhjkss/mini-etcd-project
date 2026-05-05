package com.xhj.etcd.rpc.core;

import com.xhj.etcd.rpc.RpcException;

import java.lang.reflect.Method;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * RpcServiceDefinition
 *
 * @author XJks
 * @description RPC 服务定义，描述单个服务实例及其对外暴露的 RPC 方法集合。
 *
 * <p>TODO: 该类只描述一个服务对象内部可被 RPC 调用的方法；服务名到服务定义的映射由 RpcServiceRegistry 维护。</p>
 */
public class RpcServiceDefinition {

    /**
     * 服务实例对象。
     *
     * <p>RpcRequestExecutor 最终会通过 RpcMethodInvoker 在该对象上反射调用目标方法。</p>
     */
    private final Object serviceObject;

    /**
     * 方法定义映射。
     *
     * <p>key 为对外暴露的 RPC 方法名，value 为该方法对应的反射定义。</p>
     *
     * <p>该映射只包含 methodNameSet 指定的方法，不会自动暴露 serviceObject 的全部 public 方法。</p>
     */
    private final Map<String, RpcMethodDefinition> methodDefinitionMap;

    public RpcServiceDefinition(Object serviceObject, Set<String> methodNameSet) {
        this.serviceObject = serviceObject;
        this.methodDefinitionMap = buildMethodDefinitionMap(serviceObject, methodNameSet);
    }

    public Object getServiceObject() {
        return serviceObject;
    }

    /**
     * 根据方法名获取 RPC 方法定义。
     *
     * @param methodName RPC 方法名
     * @return 方法定义；不存在时返回 null
     */
    public RpcMethodDefinition getMethodDefinition(String methodName) {
        return methodDefinitionMap.get(methodName);
    }

    /**
     * 构建当前服务的 RPC 方法定义映射。
     *
     * <p>methodNameSet 是服务注册时显式声明的可导出方法集合。
     * 构建过程中会逐个校验这些方法是否真实存在，且是否符合 RPC 方法签名约定。</p>
     *
     * @param serviceObject 服务实例对象
     * @param methodNameSet 需要导出的 RPC 方法名集合
     * @return 不可变方法定义映射
     */
    private Map<String, RpcMethodDefinition> buildMethodDefinitionMap(Object serviceObject, Set<String> methodNameSet) {
        Map<String, RpcMethodDefinition> methodDefinitionMap = new HashMap<>();
        for (String methodName : methodNameSet) {
            Method method = findExportedMethod(serviceObject.getClass(), methodName);
            methodDefinitionMap.put(methodName, new RpcMethodDefinition(methodName, method));
        }
        return Collections.unmodifiableMap(methodDefinitionMap);
    }

    /**
     * 查找指定名称的可导出 RPC 方法。
     *
     * <p>查找规则：</p>
     * <p>1) 方法名必须与 methodName 完全一致；</p>
     * <p>2) 方法签名必须符合 RpcMethodDefinition.isRpcMethod 的约定；</p>
     * <p>3) 同名且符合签名约定的方法只能有一个，否则视为歧义导出。</p>
     *
     * @param serviceClass 服务类型
     * @param methodName   RPC 方法名
     * @return 匹配到的 Java 反射方法
     */
    private Method findExportedMethod(Class<?> serviceClass, String methodName) {
        Method matchedMethod = null;
        Method[] methods = serviceClass.getMethods();
        for (Method method : methods) {
            if (!method.getName().equals(methodName)) {
                continue;
            }
            if (!RpcMethodDefinition.isRpcMethod(method)) {
                continue;
            }

            // 同名且都符合 RPC 签名约定的方法会导致请求路由无法唯一确定，直接拒绝注册。
            if (matchedMethod != null) {
                throw new RpcException("ambiguous rpc method signature: " + methodName);
            }
            matchedMethod = method;
        }
        if (matchedMethod == null) {
            throw new RpcException("rpc method not found: " + methodName);
        }
        return matchedMethod;
    }
}