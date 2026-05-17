package com.xhj.etcd.rpc.core;

import io.netty.channel.Channel;

import java.lang.reflect.Method;

/**
 * RpcMethodDefinition
 *
 * @author XJks
 * @description RPC 方法定义，描述服务方法的名称、反射方法、请求类型和连接感知能力。
 *
 * <p>
 * TODO:
 *  当前 RPC 框架支持三类服务方法签名：
 *  method(Request)、method(Request, Channel)、method(Request, Channel, String rpcMessageId)。
 *  第一个参数固定为请求对象，第二个参数如果存在必须是 Netty Channel 或其子类型，
 *  第三个参数如果存在必须是 String 类型的 rpcMessageId。
 * </p>
 */
public class RpcMethodDefinition {

    /**
     * RPC 方法名。
     *
     * <p>客户端通过 RpcMessage.methodName 传递该名称，服务端据此定位具体服务方法。</p>
     */
    private final String methodName;

    /**
     * Java 反射方法对象。
     *
     * <p>服务端完成请求反序列化后，会通过该 Method 调用真实服务方法。</p>
     */
    private final Method method;

    /**
     * 请求对象类型。
     *
     * <p>固定取 RPC 方法的第一个参数类型，用于把 RpcMessage.data 反序列化为具体请求对象。</p>
     */
    private final Class<?> requestClass;

    /**
     * 是否需要感知底层连接。
     *
     * <p>TODO: true 表示方法签名至少包含 Channel 参数，服务方法可以直接使用 Channel 发送流式响应、关闭连接或绑定连接维度状态。</p>
     */
    private final boolean channelAware;

    /**
     * 是否需要感知 rpcMessageId。
     *
     * <p>该能力主要给流式或长连接协议使用，用于把服务端推送消息路由回同一条逻辑流。</p>
     */
    private final boolean rpcMessageIdAware;

    public RpcMethodDefinition(String methodName, Method method) {
        this.methodName = methodName;
        this.method = method;
        this.requestClass = method.getParameterTypes()[0];
        this.channelAware = method.getParameterTypes().length >= 2;
        this.rpcMessageIdAware = method.getParameterTypes().length == 3;
    }

    public String getMethodName() {
        return methodName;
    }

    public Method getMethod() {
        return method;
    }

    public Class<?> getRequestClass() {
        return requestClass;
    }

    public boolean isChannelAware() {
        return channelAware;
    }

    public boolean isRpcMessageIdAware() {
        return rpcMessageIdAware;
    }

    /**
     * 判断 Java 方法是否符合 RPC 服务方法签名。
     *
     * <p>当前只允许以下三种形式：</p>
     *
     * <pre>
     * response method(request)
     * response method(request, channel)
     * response method(request, channel, rpcMessageId)
     * </pre>
     *
     * @param method Java 反射方法对象
     * @return true 表示该方法可以注册为 RPC 方法，false 表示方法签名不符合约定
     */
    public static boolean isRpcMethod(Method method) {
        Class<?>[] parameterTypes = method.getParameterTypes();
        if (parameterTypes.length == 1) {
            return true;
        }

        if (parameterTypes.length == 2) {
            return Channel.class.isAssignableFrom(parameterTypes[1]);
        }
        return parameterTypes.length == 3
                && Channel.class.isAssignableFrom(parameterTypes[1])
                && String.class.isAssignableFrom(parameterTypes[2]);
    }
}