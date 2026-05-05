package com.xhj.etcd.rpc.core;

import com.xhj.etcd.rpc.RpcException;
import com.xhj.etcd.rpc.RpcMessage;
import com.xhj.etcd.rpc.RpcMessageHandler;
import com.xhj.etcd.rpc.RpcMessageHandlerRegistration;
import com.xhj.etcd.rpc.RpcMessageType;
import com.xhj.etcd.serializer.Serializer;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * UnaryRpcMessageHandler
 *
 * @author XJks
 * @description 一元 RPC 消息处理器，负责等待并完成一次请求对应的一次响应。一元 RPC 即 Unary RPC，表示一次客户端请求只会产生一次最终响应；收到 RESPONSE 或 ERROR 后，本次调用生命周期结束。
 *
 * <p>TODO: 一元 RPC 的生命周期只允许产生一个最终结果；收到 RESPONSE、HEARTBEAT、ERROR 或连接关闭事件后，都会完成 responseFuture 并注销当前处理器。</p>
 */
public class UnaryRpcMessageHandler<T> implements RpcMessageHandler {

    /**
     * RPC 响应结果 Future。
     *
     * <p>请求发送方通过 await 等待该 Future 完成；
     * 网络线程收到响应后，通过 handle 将结果写入该 Future。</p>
     */
    private final CompletableFuture<T> responseFuture = new CompletableFuture<>();

    /**
     * 序列化器。
     *
     * <p>用于将 RpcMessage.data 反序列化为调用方期望的响应对象。</p>
     */
    private final Serializer serializer;

    /**
     * RPC 响应类型。
     *
     * <p>一元 RPC 收到 RESPONSE 或 HEARTBEAT 后，会按照该类型反序列化消息载荷。</p>
     */
    private final Class<T> responseClass;

    public UnaryRpcMessageHandler(Serializer serializer, Class<T> responseClass) {
        this.serializer = serializer;
        this.responseClass = responseClass;
    }

    /**
     * 处理 RPC 响应消息。
     *
     * <p>处理流程：</p>
     * <p>1) RESPONSE / HEARTBEAT 表示调用成功，反序列化响应数据并完成 responseFuture；</p>
     * <p>2) ERROR 表示 RPC 框架层异常，用 RpcException 完成 responseFuture；</p>
     * <p>3) 其他消息类型不属于一元 RPC 结果，按协议异常处理；</p>
     * <p>4) 当前一元调用完成后，注销对应的 handler 注册关系。</p>
     *
     * @param message      RPC 响应消息
     * @param registration 当前 handler 的注册关系
     */
    @Override
    public void handle(RpcMessage message, RpcMessageHandlerRegistration registration) {
        try {
            // 1) 普通响应和心跳响应都表示本次一元调用成功完成。
            if (message.getType() == RpcMessageType.RESPONSE || message.getType() == RpcMessageType.HEARTBEAT) {
                T response = serializer.deserialize(message.getData(), responseClass);
                responseFuture.complete(response);
                registration.remove();
                return;
            }

            // 2) ERROR 表示 RPC 框架层失败，例如服务不存在、方法未导出、调用异常等。
            if (message.getType() == RpcMessageType.ERROR) {
                responseFuture.completeExceptionally(new RpcException(message.getErrorMessage()));
                registration.remove();
                return;
            }

            // 3) 一元 RPC 不接收 STREAM 等多帧消息，收到后直接按协议异常完成。
            responseFuture.completeExceptionally(new RpcException("unsupported unary rpc message type: " + message.getType()));
            registration.remove();
        } catch (Exception e) {
            // 4) 反序列化或回调处理异常也要完成 Future，避免调用方一直阻塞等待。
            responseFuture.completeExceptionally(e);
            registration.remove();
        }
    }

    /**
     * 处理连接关闭事件。
     *
     * <p>连接关闭时，本次一元调用已经无法再收到响应，需要立即异常完成等待中的 Future，
     * 并注销当前 handler 注册关系。</p>
     *
     * @param cause        连接关闭原因
     * @param registration 当前 handler 的注册关系
     */
    @Override
    public void handleConnectionClosed(Throwable cause, RpcMessageHandlerRegistration registration) {
        responseFuture.completeExceptionally(cause == null ? new RpcException("rpc connection closed") : cause);
        registration.remove();
    }

    /**
     * 等待一元 RPC 响应结果。
     *
     * <p>该方法由发起 RPC 调用的业务线程调用；网络线程收到响应后会完成 responseFuture，
     * 从而唤醒当前等待线程。</p>
     *
     * @param timeoutMillis 等待超时时间，单位毫秒
     * @return RPC 响应对象
     */
    public T await(long timeoutMillis) {
        try {
            return responseFuture.get(timeoutMillis, TimeUnit.MILLISECONDS);
        } catch (RpcException e) {
            throw e;
        } catch (Exception e) {
            throw new RpcException("rpc call timeout or failed", e);
        }
    }
}