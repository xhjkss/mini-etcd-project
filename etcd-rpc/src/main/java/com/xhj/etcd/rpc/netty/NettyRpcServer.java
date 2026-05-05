package com.xhj.etcd.rpc.netty;

import com.xhj.etcd.rpc.NodeEndpoint;
import com.xhj.etcd.rpc.RpcException;
import com.xhj.etcd.rpc.RpcServer;
import com.xhj.etcd.rpc.core.RpcMethodInvoker;
import com.xhj.etcd.rpc.core.RpcRequestExecutor;
import com.xhj.etcd.rpc.core.RpcServiceRegistry;
import com.xhj.etcd.rpc.core.ServerRpcMessageDispatcher;
import com.xhj.etcd.serializer.Serializer;
import com.xhj.etcd.serializer.SerializerRegistry;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;

/**
 * NettyRpcServer
 *
 * @author XJks
 * @description Netty RPC 服务端实现，负责监听端口、注册服务方法、接收 RPC 请求并组装服务端处理链路。
 *
 * <p>职责边界：</p>
 * <ul>
 *     <li>负责创建 ServerBootstrap、监听 endpoint 对应端口，并维护服务端 Channel 生命周期。</li>
 *     <li>负责组装服务端 Netty pipeline，包括拆包、长度字段编码、RpcMessage 编解码和服务端消息处理器。</li>
 *     <li>负责维护 RpcServiceRegistry，注册 serviceName 到服务实例和导出方法的映射。</li>
 *     <li>不直接执行业务方法；普通 RPC 请求会经 ServerRpcMessageDispatcher 转交给 RpcRequestExecutor 在线程池中执行。</li>
 * </ul>
 *
 * <p>TODO: NettyRpcServer 只负责服务端网络入口和服务注册，具体请求分发、反序列化、反射调用、响应写回分别由 ServerRpcMessageDispatcher、RpcMethodInvoker 和 RpcRequestExecutor 协作完成。</p>
 */
public class NettyRpcServer implements RpcServer {

    /**
     * 序列化器。
     *
     * <p>用于 RpcMessageCodec 编解码 RPC 消息，也用于 RpcMethodInvoker 反序列化请求对象。</p>
     */
    private final Serializer serializer;

    /**
     * RPC 服务注册表。
     *
     * <p>维护 serviceName 到 RpcServiceDefinition 的映射；
     * 服务端收到 REQUEST 后，会根据 RpcMessage.serviceName 和 methodName 定位具体服务方法。</p>
     */
    private final RpcServiceRegistry serviceRegistry = new RpcServiceRegistry();

    /**
     * RPC 方法调用器。
     *
     * <p>负责把请求字节数据反序列化为方法入参，并通过反射调用目标服务方法。</p>
     */
    private final RpcMethodInvoker methodInvoker;

    /**
     * RPC 请求执行器。
     *
     * <p>负责把普通 RPC 请求切换到业务线程执行，并将返回值或框架异常封装为响应消息写回客户端。</p>
     */
    private final RpcRequestExecutor requestExecutor;

    /**
     * 服务端 RPC 消息分发器。
     *
     * <p>负责校验服务端入站消息类型，心跳消息直接响应，普通请求转交给 requestExecutor。</p>
     */
    private final ServerRpcMessageDispatcher messageDispatcher;

    /**
     * 当前 RPC 服务端监听地址。
     *
     * <p>start 时会绑定该 endpoint 对应的 host 和 port。</p>
     */
    private final NodeEndpoint endpoint;

    /**
     * Netty boss 线程组。
     *
     * <p>负责服务端 accept 新连接。当前服务端只需要一个 boss 线程。</p>
     */
    private EventLoopGroup bossGroup;

    /**
     * Netty worker 线程组。
     *
     * <p>负责已建立连接的读写事件和 pipeline 回调。</p>
     */
    private EventLoopGroup workerGroup;

    /**
     * 服务端监听 Channel。
     *
     * <p>start 成功后保存 bind 返回的 Channel，stop 时通过该 Channel 关闭监听端口。</p>
     */
    private Channel serverChannel;

    public NettyRpcServer(NodeEndpoint endpoint) {
        this(endpoint, SerializerRegistry.getDefaultSerializer());
    }

    public NettyRpcServer(NodeEndpoint endpoint, Serializer serializer) {
        this.endpoint = endpoint;
        this.serializer = serializer;
        this.methodInvoker = new RpcMethodInvoker(serializer);
        this.requestExecutor = new RpcRequestExecutor(serializer, serviceRegistry, methodInvoker);
        this.messageDispatcher = new ServerRpcMessageDispatcher(serializer, requestExecutor);
    }

    /**
     * 注册 RPC 服务。
     *
     * <p>注册时需要显式声明允许远程调用的方法名集合；
     * 框架不会自动暴露 serviceObject 的全部 public 方法。</p>
     *
     * @param serviceName   RPC 服务名
     * @param serviceObject 服务实例对象
     * @param methodNames   允许远程调用的方法名列表
     */
    @Override
    public void registerService(String serviceName, Object serviceObject, String... methodNames) {
        serviceRegistry.registerService(serviceName, serviceObject, methodNames);
    }

    /**
     * 启动 Netty RPC 服务端。
     *
     * <p>启动流程：</p>
     * <p>1) 创建 bossGroup 和 workerGroup；</p>
     * <p>2) 配置 ServerBootstrap 和服务端 Channel 参数；</p>
     * <p>3) 初始化子 Channel pipeline；</p>
     * <p>4) 绑定 endpoint 对应端口并保存 serverChannel。</p>
     */
    @Override
    public void start() {
        bossGroup = new NioEventLoopGroup(1);
        workerGroup = new NioEventLoopGroup();

        try {
            ServerBootstrap bootstrap = new ServerBootstrap();
            bootstrap.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .option(ChannelOption.SO_BACKLOG, 128)
                    .childOption(ChannelOption.TCP_NODELAY, true)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) {
                            // 1) 入站先按 4 字节长度字段拆包，避免 TCP 粘包/半包影响 RpcMessageCodec。
                            ch.pipeline().addLast(new LengthFieldBasedFrameDecoder(16 * 1024 * 1024, 0, 4, 0, 4));

                            // 2) 出站时在 RpcMessage 编码结果前追加 4 字节长度字段。
                            ch.pipeline().addLast(new LengthFieldPrepender(4));

                            // 3) RpcMessage 和 ByteBuf 之间的协议编解码。
                            ch.pipeline().addLast(new RpcMessageCodec(serializer));

                            // 4) 服务端入站消息处理，REQUEST 会被转交给 ServerRpcMessageDispatcher。
                            ch.pipeline().addLast(new NettyRpcServerHandler(messageDispatcher));
                        }
                    });

            // sync 等待端口绑定完成；绑定成功后保存 serverChannel，供 stop 关闭监听端口。
            ChannelFuture future = bootstrap.bind(endpoint.getHost(), endpoint.getPort()).sync();
            serverChannel = future.channel();
        } catch (Exception e) {
            // 启动失败时释放已经创建的线程组和 Channel，避免半启动状态下资源泄漏。
            stop();
            throw new RpcException("start netty rpc server failed, endpoint=" + endpoint.endpointKey(), e);
        }
    }

    /**
     * 停止 Netty RPC 服务端。
     *
     * <p>停止时会关闭监听 Channel、关闭 Netty 线程组，并停止业务请求执行器。</p>
     */
    @Override
    public void stop() {
        if (serverChannel != null) {
            serverChannel.close();
        }
        if (workerGroup != null) {
            workerGroup.shutdownGracefully();
        }
        if (bossGroup != null) {
            bossGroup.shutdownGracefully();
        }
        requestExecutor.shutdown();
    }
}