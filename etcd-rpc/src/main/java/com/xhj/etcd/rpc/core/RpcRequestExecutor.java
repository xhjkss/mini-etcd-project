package com.xhj.etcd.rpc.core;

import com.xhj.etcd.rpc.RpcMessage;
import com.xhj.etcd.rpc.RpcMessageType;
import com.xhj.etcd.serializer.Serializer;
import io.netty.channel.Channel;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * RpcRequestExecutor
 *
 * @author XJks
 * @description RPC 请求执行器，负责将服务端收到的请求切换到业务线程执行，并把执行结果封装为响应消息。
 *
 * <p>TODO: 该类是 Netty IO 线程和业务方法调用之间的边界。IO 线程只负责提交任务，具体服务方法调用由 businessExecutor 执行，避免业务逻辑阻塞网络读写线程。</p>
 */
public class RpcRequestExecutor {

    /**
     * 序列化器。
     *
     * <p>用于将服务方法返回值序列化为 RpcMessage.data。</p>
     */
    private final Serializer serializer;

    /**
     * RPC 服务注册表。
     *
     * <p>用于根据 RpcMessage.serviceName 定位服务实例和该服务暴露的方法定义。</p>
     */
    private final RpcServiceRegistry serviceRegistry;

    /**
     * RPC 方法调用器。
     *
     * <p>负责将请求字节数据反序列化为请求对象，并通过反射调用目标服务方法。</p>
     */
    private final RpcMethodInvoker methodInvoker;

    /**
     * 业务线程池。
     *
     * <p>服务端收到 RPC 请求后，会把真正的方法调用提交到该线程池执行，
     * 避免在 Netty IO 线程中直接执行业务逻辑。</p>
     */
    private final ExecutorService businessExecutor;

    public RpcRequestExecutor(Serializer serializer, RpcServiceRegistry serviceRegistry, RpcMethodInvoker methodInvoker) {
        this(serializer, serviceRegistry, methodInvoker, Executors.newCachedThreadPool());
    }

    public RpcRequestExecutor(Serializer serializer, RpcServiceRegistry serviceRegistry, RpcMethodInvoker methodInvoker, ExecutorService businessExecutor) {
        this.serializer = serializer;
        this.serviceRegistry = serviceRegistry;
        this.methodInvoker = methodInvoker;
        this.businessExecutor = businessExecutor;
    }

    /**
     * 提交 RPC 请求执行任务。
     *
     * <p>该方法通常由 Netty IO 线程调用，只负责把请求转交给业务线程池；具体服务查找、方法调用和响应写回都在业务线程中完成。</p>
     *
     * @param channel        当前请求所在连接
     * @param requestMessage RPC 请求消息
     */
    public void submit(final Channel channel, final RpcMessage requestMessage) {
        businessExecutor.execute(new Runnable() {
            @Override
            public void run() {
                execute(channel, requestMessage);
            }
        });
    }

    /**
     * 关闭请求执行器。
     *
     * <p>通常在 RPC 服务端关闭时调用，用于中断正在等待或执行中的业务任务。</p>
     */
    public void shutdown() {
        businessExecutor.shutdownNow();
    }

    /**
     * 执行 RPC 请求。
     *
     * <p>处理流程：</p>
     * <p>1) 根据 serviceName 查找服务定义；</p>
     * <p>2) 根据 methodName 查找方法定义；</p>
     * <p>3) 通过 RpcMethodInvoker 调用目标服务方法；</p>
     * <p>4) 将非空返回值封装为 RESPONSE 消息写回客户端。</p>
     *
     * @param channel        当前请求所在连接
     * @param requestMessage RPC 请求消息
     */
    private void execute(Channel channel, RpcMessage requestMessage) {
        try {
            // 1) 根据服务名定位服务实例。服务不存在属于 RPC 框架层错误，直接返回 ERROR。
            RpcServiceDefinition serviceDefinition = serviceRegistry.getServiceDefinition(requestMessage.getServiceName());
            if (serviceDefinition == null) {
                channel.writeAndFlush(buildError(requestMessage, "service not found: " + requestMessage.getServiceName()));
                return;
            }

            // 2) 根据方法名定位服务方法。方法未导出同样属于 RPC 框架层错误。
            RpcMethodDefinition methodDefinition = serviceDefinition.getMethodDefinition(requestMessage.getMethodName());
            if (methodDefinition == null) {
                channel.writeAndFlush(buildError(requestMessage, "rpc method not exported: " + requestMessage.getMethodName()));
                return;
            }

            // 3) 反序列化请求数据并反射调用服务方法；是否传入 Channel 由 RpcMethodDefinition 决定。
            Object result = methodInvoker.invoke(
                    serviceDefinition.getServiceObject(),
                    methodDefinition,
                    requestMessage.getData(),
                    channel);

            // 4) 返回值为空表示该方法自行处理响应，例如流式方法可能直接通过 Channel 写回多帧数据。
            if (result != null) {
                channel.writeAndFlush(buildResponse(requestMessage, result));
            }
        } catch (Exception e) {
            channel.writeAndFlush(buildError(requestMessage, e.getMessage()));
        }
    }

    /**
     * 构建普通 RPC 响应消息。
     *
     * <p>响应消息会复用请求消息的 rpcMessageId，
     * 客户端据此找到等待中的 RpcMessageHandler 并完成回调。</p>
     *
     * @param requestMessage RPC 请求消息
     * @param responseBody   服务方法返回对象
     * @return RPC 响应消息
     */
    private RpcMessage buildResponse(RpcMessage requestMessage, Object responseBody) {
        RpcMessage response = new RpcMessage();
        response.setType(RpcMessageType.RESPONSE);
        response.setRpcMessageId(requestMessage.getRpcMessageId());
        response.setData(serializer.serialize(responseBody));
        return response;
    }

    /**
     * 构建 RPC 框架层错误消息。
     *
     * <p>该方法只表达服务不存在、方法未导出、反序列化失败、反射调用失败等框架层错误；
     * 业务失败应由具体业务响应对象表达。</p>
     *
     * @param requestMessage RPC 请求消息
     * @param errorMessage   错误信息
     * @return RPC 错误消息
     */
    private RpcMessage buildError(RpcMessage requestMessage, String errorMessage) {
        RpcMessage response = new RpcMessage();
        response.setType(RpcMessageType.ERROR);
        response.setRpcMessageId(requestMessage.getRpcMessageId());
        response.setErrorMessage(errorMessage == null ? "rpc request failed" : errorMessage);
        return response;
    }
}