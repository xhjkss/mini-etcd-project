package com.xhj.etcd.rpc.netty;

import com.xhj.etcd.rpc.NodeEndpoint;
import com.xhj.etcd.rpc.RpcClient;
import com.xhj.etcd.rpc.RpcMessage;
import com.xhj.etcd.rpc.RpcMessageHandler;
import com.xhj.etcd.rpc.RpcMessageHandlerRegistration;
import com.xhj.etcd.rpc.RpcMessageHandlerRegistry;
import com.xhj.etcd.rpc.RpcMessageType;
import com.xhj.etcd.rpc.RpcStream;
import com.xhj.etcd.rpc.core.ClientRpcMessageDispatcher;
import com.xhj.etcd.rpc.core.UnaryRpcMessageHandler;
import com.xhj.etcd.serializer.Serializer;
import com.xhj.etcd.serializer.SerializerRegistry;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

/**
 * NettyRpcClient
 *
 * @author XJks
 * @description Netty RPC 客户端实现，负责创建 outbound 连接、发送请求消息，并把服务端响应分发给等待中的调用方。
 *
 * <p>职责边界：</p>
 * <ul>
 *     <li>负责客户端侧 Netty Bootstrap、Channel 复用和消息写出。</li>
 *     <li>负责为一元调用、流式调用注册 RpcMessageHandler。</li>
 *     <li>负责把写失败转换为 ERROR 消息交给客户端分发器处理。</li>
 *     <li>不负责服务端方法路由，服务端的 serviceName/methodName 分发由 ServerRpcMessageDispatcher 和 RpcRequestExecutor 处理。</li>
 * </ul>
 *
 * <p>TODO: 客户端响应不按 serviceName/methodName 分发，而是按 rpcMessageId 找回请求发送前注册的 handler。因此 call/openStream 发送请求前必须先注册 handler，再写出请求消息。</p>
 */
public class NettyRpcClient implements RpcClient {

    /**
     * 序列化器。
     *
     * <p>用于把请求对象序列化为 RpcMessage.data，也用于一元响应 handler 反序列化响应对象。</p>
     */
    private final Serializer serializer;

    /**
     * RPC 调用超时时间，单位毫秒。
     *
     * <p>一元调用等待响应、心跳等待响应、同步获取连接时都会使用该超时时间。</p>
     */
    private final long timeoutMillis;

    /**
     * 客户端消息处理器注册表。
     *
     * <p>发送请求前按 rpcMessageId 注册 handler；响应返回后，ClientRpcMessageDispatcher
     * 根据 rpcMessageId 找回对应 handler 并完成回调。</p>
     */
    private final RpcMessageHandlerRegistry handlerRegistry = new RpcMessageHandlerRegistry();

    /**
     * 客户端消息分发器。
     *
     * <p>Netty 客户端 handler 收到服务端响应后，会交给该分发器按 rpcMessageId 回填到等待中的调用方。</p>
     */
    private final ClientRpcMessageDispatcher messageDispatcher = new ClientRpcMessageDispatcher(handlerRegistry);

    /**
     * 客户端实例 ID。
     *
     * <p>用于参与生成 rpcMessageId，降低不同客户端实例之间消息 ID 冲突的概率。</p>
     */
    private final String clientId = UUID.randomUUID().toString();

    /**
     * 当前客户端内的消息递增序号。
     *
     * <p>和 clientId 一起组成 rpcMessageId，保证同一客户端内的请求消息 ID 单调递增且尽量唯一。</p>
     */
    private final AtomicLong messageSequence = new AtomicLong(0);

    /**
     * Netty 客户端 IO 线程组。
     *
     * <p>负责处理客户端侧连接建立、读写事件和 pipeline 回调。</p>
     */
    private final EventLoopGroup workerGroup = new NioEventLoopGroup();

    /**
     * Netty 客户端启动器。
     *
     * <p>由 buildBootstrap 初始化 pipeline，并交给 NettyChannelRegistry 用于主动连接远端节点。</p>
     */
    private final Bootstrap bootstrap;

    /**
     * outbound Channel 注册表。
     *
     * <p>负责按 NodeEndpoint 复用或创建客户端主动连接。</p>
     */
    private final NettyChannelRegistry channelRegistry;

    public NettyRpcClient() {
        this(SerializerRegistry.getDefaultSerializer(), 15000L);
    }

    public NettyRpcClient(Serializer serializer, long timeoutMillis) {
        this.serializer = serializer;
        this.timeoutMillis = timeoutMillis;
        this.bootstrap = buildBootstrap();
        this.channelRegistry = new NettyChannelRegistry(bootstrap, 2000L);
    }

    /**
     * 发起一元 RPC 调用。
     *
     * <p>一元 RPC 表示一次请求只对应一次最终响应。该方法会注册 UnaryRpcMessageHandler，
     * 发送请求后阻塞等待 RESPONSE、HEARTBEAT、ERROR 或连接关闭事件完成本次调用。</p>
     *
     * @param endpoint      目标节点地址
     * @param serviceName   RPC 服务名
     * @param methodName    RPC 方法名
     * @param request       请求对象
     * @param responseClass 响应对象类型
     * @param <T>           响应类型
     * @return RPC 响应对象
     */
    @Override
    public <T> T call(NodeEndpoint endpoint, String serviceName, String methodName, Object request, Class<T> responseClass) {
        String rpcMessageId = nextRpcMessageId();
        UnaryRpcMessageHandler<T> handler = new UnaryRpcMessageHandler<>(serializer, responseClass);

        // 1) 先注册 handler，再发送请求，避免响应提前返回时找不到等待方。
        registerHandler(rpcMessageId, handler);

        // 2) 写出请求消息；写失败会被转换为 ERROR 消息回调到当前 handler。
        sendRequest(endpoint, serviceName, methodName, request, rpcMessageId);

        // 3) 业务线程等待一元响应完成。
        return handler.await(timeoutMillis);
    }

    /**
     * 打开流式 RPC 调用。
     *
     * <p>流式 RPC 会复用同一个 rpcMessageId 持续接收多帧 STREAM/ERROR/HEARTBEAT 等消息，
     * 因此 handler 不会像一元调用那样在首个响应后自动结束，而是由返回的 RpcStream 控制生命周期。</p>
     *
     * @param streamId    调用方指定的流 ID；为空时由客户端自动生成
     * @param endpoint    目标节点地址
     * @param serviceName RPC 服务名
     * @param methodName  RPC 方法名
     * @param request     首帧请求对象
     * @param handler     流式消息处理器
     * @return 流式 RPC 句柄
     */
    @Override
    public RpcStream openStream(String streamId, NodeEndpoint endpoint, String serviceName, String methodName, Object request, RpcMessageHandler handler) {
        String rpcMessageId = (streamId == null || streamId.trim().length() == 0)
                ? nextRpcMessageId()
                : streamId.trim();

        // 流式调用在整个 stream 生命周期内复用同一个 handler 注册关系。
        RpcMessageHandlerRegistration registration = registerHandler(rpcMessageId, handler);
        NettyRpcStream stream = new NettyRpcStream(rpcMessageId, endpoint, registration);
        try {
            sendRequest(endpoint, serviceName, methodName, request, rpcMessageId);
            return stream;
        } catch (Exception e) {
            // 首帧请求发送失败时，需要关闭 stream 并注销 handler，避免注册表残留。
            stream.close();
            throw e;
        }
    }

    /**
     * 发送单向 RPC 消息。
     *
     * <p>
     * TODO:
     *  该方法是 best-effort 单向发送，不注册 handler、不等待响应、不保证本次消息一定写出。
     *  如果目标 endpoint 正处于失败退避窗口，NettyChannelRegistry 可能会直接丢弃本次消息。
     * </p>
     *
     * <p>该方法适合 Raft AppendEntries、InstallSnapshot、异步心跳等上层自带重试机制的内部消息。
     * 如果调用方需要可靠送达、错误感知或响应结果，应使用 {@link #call(NodeEndpoint, String, String, Object, Class)}。</p>
     *
     * @param endpoint    目标节点地址
     * @param serviceName RPC 服务名
     * @param methodName  RPC 方法名
     * @param request     请求对象
     */
    @Override
    public void send(NodeEndpoint endpoint, String serviceName, String methodName, Object request) {
        RpcMessage message = buildRequestMessage(nextRpcMessageId(), serviceName, methodName, request);
        channelRegistry.sendBestEffort(endpoint, message);
    }

    /**
     * 发送心跳探测。
     *
     * <p>心跳使用普通的一元等待模型：先注册 handler，再通过 Channel 写出 HEARTBEAT 消息，
     * 等待服务端返回 HEARTBEAT 响应。等待成功返回 true，任意异常返回 false。</p>
     *
     * @param endpoint 目标节点地址
     * @return true 表示心跳成功，false 表示连接、写入或等待响应失败
     */
    @Override
    public boolean heartbeat(NodeEndpoint endpoint) {
        String rpcMessageId = nextRpcMessageId();
        UnaryRpcMessageHandler<Object> handler = new UnaryRpcMessageHandler<>(serializer, Object.class);
        registerHandler(rpcMessageId, handler);
        try {
            Channel channel = channelRegistry.getOrConnect(endpoint, timeoutMillis);
            RpcMessage message = new RpcMessage();
            message.setType(RpcMessageType.HEARTBEAT);
            message.setRpcMessageId(rpcMessageId);

            ChannelFuture writeFuture = channel.writeAndFlush(message);
            attachWriteFailureHandler(rpcMessageId, writeFuture);

            handler.await(timeoutMillis);
            return true;
        } catch (Exception e) {
            return false;
        } finally {
            // 心跳是一次性调用，无论成功失败都要清理注册关系。
            removeHandler(rpcMessageId);
        }
    }

    /**
     * 关闭 RPC 客户端。
     *
     * <p>关闭时会先关闭所有 outbound Channel，再优雅关闭 Netty worker 线程组。</p>
     */
    @Override
    public void shutdown() {
        channelRegistry.closeAll();
        workerGroup.shutdownGracefully();
    }

    // ==================== 内部消息生命周期 ====================

    /**
     * 生成下一个 RPC 消息 ID。
     *
     * <p>消息 ID 用于把客户端发出的请求和服务端返回的响应关联起来。</p>
     *
     * @return RPC 消息 ID
     */
    private String nextRpcMessageId() {
        return "rpc-message-" + clientId + "-" + messageSequence.incrementAndGet();
    }

    /**
     * 注册 RPC 消息处理器。
     *
     * @param rpcMessageId RPC 消息 ID
     * @param handler      消息处理器
     * @return handler 注册关系
     */
    private RpcMessageHandlerRegistration registerHandler(String rpcMessageId, RpcMessageHandler handler) {
        return handlerRegistry.register(rpcMessageId, handler);
    }

    /**
     * 移除 RPC 消息处理器。
     *
     * @param rpcMessageId RPC 消息 ID
     */
    private void removeHandler(String rpcMessageId) {
        handlerRegistry.remove(rpcMessageId);
    }

    /**
     * 发送普通 RPC 请求消息。
     *
     * <p>该方法会同步获取或建立到目标 endpoint 的 Channel，然后写出 REQUEST 消息。
     * 如果写出失败，会通过 attachWriteFailureHandler 转换为 ERROR 消息分发给等待中的 handler。</p>
     *
     * @param endpoint     目标节点地址
     * @param serviceName  RPC 服务名
     * @param methodName   RPC 方法名
     * @param request      请求对象
     * @param rpcMessageId RPC 消息 ID
     */
    private void sendRequest(NodeEndpoint endpoint, String serviceName, String methodName, Object request, String rpcMessageId) {
        Channel channel = channelRegistry.getOrConnect(endpoint, timeoutMillis);
        RpcMessage message = buildRequestMessage(rpcMessageId, serviceName, methodName, request);
        ChannelFuture writeFuture = channel.writeAndFlush(message);
        attachWriteFailureHandler(rpcMessageId, writeFuture);
    }

    /**
     * 构建 Netty 客户端 Bootstrap。
     *
     * <p>客户端 pipeline 采用定长长度字段解决粘包拆包问题，
     * RpcMessageCodec 负责 RpcMessage 编解码，NettyRpcClientHandler 负责把入站消息交给客户端分发器。</p>
     *
     * @return Netty Bootstrap
     */
    private Bootstrap buildBootstrap() {
        Bootstrap bootstrap = new Bootstrap();
        bootstrap.group(workerGroup)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.TCP_NODELAY, true)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 3000)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        // 1) 先按长度字段拆包，避免 TCP 粘包/半包影响 RpcMessageCodec。
                        ch.pipeline().addLast(new LengthFieldBasedFrameDecoder(16 * 1024 * 1024, 0, 4, 0, 4));

                        // 2) 写出时在消息前补 4 字节长度字段。
                        ch.pipeline().addLast(new LengthFieldPrepender(4));

                        // 3) RpcMessage 和 ByteBuf 之间的协议编解码。
                        ch.pipeline().addLast(new RpcMessageCodec(serializer));

                        // 4) 客户端入站消息处理，最终根据 rpcMessageId 分发给 handler。
                        ch.pipeline().addLast(new NettyRpcClientHandler(messageDispatcher));
                    }
                });
        return bootstrap;
    }

    /**
     * 构建 RPC 请求消息。
     *
     * <p>serviceName 和 methodName 用于服务端路由，rpcMessageId 用于客户端响应回填。</p>
     *
     * @param rpcMessageId RPC 消息 ID
     * @param serviceName  RPC 服务名
     * @param methodName   RPC 方法名
     * @param request      请求对象
     * @return RPC 请求消息
     */
    private RpcMessage buildRequestMessage(String rpcMessageId, String serviceName, String methodName, Object request) {
        RpcMessage message = new RpcMessage();
        message.setType(RpcMessageType.REQUEST);
        message.setRpcMessageId(rpcMessageId);
        message.setServiceName(serviceName);
        message.setMethodName(methodName);
        message.setData(serializer.serialize(request));
        return message;
    }

    /**
     * 绑定写失败处理器。
     *
     * <p>TODO: 写失败不是服务端业务失败，但调用方仍然需要被唤醒；因此这里会构造一条框架层 ERROR 消息，并交给 ClientRpcMessageDispatcher 走统一回调路径。</p>
     *
     * @param rpcMessageId RPC 消息 ID
     * @param writeFuture  Netty 写出 Future
     */
    private void attachWriteFailureHandler(final String rpcMessageId, ChannelFuture writeFuture) {
        writeFuture.addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) {
                if (future.isSuccess()) {
                    return;
                }
                RpcMessage error = new RpcMessage();
                error.setType(RpcMessageType.ERROR);
                error.setRpcMessageId(rpcMessageId);
                error.setErrorMessage("netty rpc write failed");
                messageDispatcher.dispatch(error);
            }
        });
    }

    /**
     * NettyRpcStream
     *
     * @author XJks
     * @description Netty 流式 RPC 句柄，维护单个 stream 的 rpcMessageId、目标 endpoint 和 handler 注册关系。
     */
    private class NettyRpcStream implements RpcStream {

        /**
         * 流式 RPC 消息 ID。
         *
         * <p>同一个 stream 后续请求和响应都会复用该 ID，以便客户端分发器把多帧消息路由到同一个 handler。</p>
         */
        private final String rpcMessageId;

        /**
         * 流式 RPC 目标节点。
         */
        private final NodeEndpoint endpoint;

        /**
         * 当前 stream 对应的 handler 注册关系。
         *
         * <p>stream 关闭时通过该 registration 注销 handler。</p>
         */
        private final RpcMessageHandlerRegistration registration;

        /**
         * stream 是否已经关闭。
         */
        private volatile boolean closed;

        private NettyRpcStream(String rpcMessageId, NodeEndpoint endpoint, RpcMessageHandlerRegistration registration) {
            this.rpcMessageId = rpcMessageId;
            this.endpoint = endpoint;
            this.registration = registration;
        }

        @Override
        public String getRpcMessageId() {
            return rpcMessageId;
        }

        /**
         * 在当前 stream 上继续发送请求。
         *
         * <p>和普通一元调用不同，stream 后续请求会继续复用当前 rpcMessageId，
         * 这样服务端返回的多帧消息仍会被分发到同一个 handler。</p>
         *
         * @param serviceName RPC 服务名
         * @param methodName  RPC 方法名
         * @param request     请求对象
         */
        @Override
        public void sendRequest(String serviceName, String methodName, Object request) {
            if (closed) {
                return;
            }
            NettyRpcClient.this.sendRequest(endpoint, serviceName, methodName, request, rpcMessageId);
        }

        @Override
        public boolean isClosed() {
            return closed;
        }

        /**
         * 关闭当前 stream。
         *
         * <p>关闭操作只影响客户端本地 handler 注册关系，不会主动关闭底层 Channel。
         * 底层 Channel 由 NettyChannelRegistry 按 endpoint 维度复用。</p>
         */
        @Override
        public void close() {
            if (closed) {
                return;
            }
            closed = true;
            registration.remove();
        }
    }
}