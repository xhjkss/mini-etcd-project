package com.xhj.etcd.rpc.core;

import com.xhj.etcd.rpc.RpcException;
import com.xhj.etcd.serializer.Serializer;
import io.netty.channel.Channel;

import java.lang.reflect.Method;

/**
 * RpcMethodInvoker
 *
 * @author XJks
 * @description RPC 方法调用器，负责把协议字节数据反序列化为请求对象，并通过反射调用目标服务方法。
 */
public class RpcMethodInvoker {

    /**
     * 序列化器。
     *
     * <p>用于根据 RpcMethodDefinition 中记录的请求类型，将 RpcMessage.data 还原为服务方法入参。</p>
     */
    private final Serializer serializer;

    public RpcMethodInvoker(Serializer serializer) {
        this.serializer = serializer;
    }

    // ==================== 方法调用入口 ====================

    /**
     * 调用 RPC 服务方法。
     *
     * <p>调用流程：</p>
     * <p>1) 根据方法定义中的 requestClass 反序列化请求对象；</p>
     * <p>2) 如果方法需要感知连接，则按 method(request, channel) 或 method(request, channel, rpcMessageId) 调用；</p>
     * <p>3) 否则按 method(request) 调用。</p>
     *
     * @param service          服务实例
     * @param methodDefinition RPC 方法定义
     * @param data             序列化后的请求数据
     * @param channel          当前请求所在连接
     * @param rpcMessageId     当前请求消息 id
     * @return 服务方法返回值
     */
    public Object invoke(Object service, RpcMethodDefinition methodDefinition, byte[] data, Channel channel, String rpcMessageId) {
        Method method = methodDefinition.getMethod();
        Object request = serializer.deserialize(data, methodDefinition.getRequestClass());
        try {
            // 3 参数方法通常用于长连接流式协议，可以同时感知连接和当前 rpcMessageId。
            if (methodDefinition.isRpcMessageIdAware()) {
                return method.invoke(service, request, channel, rpcMessageId);
            }

            // channelAware 方法需要感知底层连接，常用于流式响应或连接维度状态绑定。
            if (methodDefinition.isChannelAware()) {
                return method.invoke(service, request, channel);
            }

            // 普通 RPC 方法只接收反序列化后的请求对象。
            return method.invoke(service, request);
        } catch (Exception e) {
            throw new RpcException("invoke rpc method failed: " + methodDefinition.getMethodName(), e);
        }
    }

    /**
     * 调用 RPC 服务方法（兼容旧签名）。
     *
     * <p>TODO: 该重载用于兼容现有调用方与测试，内部统一委托到带 rpcMessageId 的新签名。</p>
     */
    public Object invoke(Object service, RpcMethodDefinition methodDefinition, byte[] data, Channel channel) {
        return invoke(service, methodDefinition, data, channel, null);
    }
}