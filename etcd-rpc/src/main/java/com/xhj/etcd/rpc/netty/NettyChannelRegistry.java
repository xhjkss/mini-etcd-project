package com.xhj.etcd.rpc.netty;

import com.xhj.etcd.rpc.NodeEndpoint;
import com.xhj.etcd.rpc.RpcException;
import com.xhj.etcd.rpc.RpcMessage;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * NettyChannelRegistry
 *
 * @author XJks
 * @description Netty 客户端侧 outbound Channel 注册表，负责按 endpoint 复用、创建和清理主动连接。
 *
 * <p>
 * TODO:
 *  该注册表只维护“本节点主动连接其他 endpoint”产生的 outbound Channel。
 *  例如 Raft 节点 A 主动向节点 B 发送 AppendEntries 时，使用的是 A -> B 的 outbound Channel。
 * </p>
 *
 * <p>服务端接收到的 inbound Channel 不放入这里。用户 watch 的 STREAM 推送会直接复用
 * 用户创建 watch 时连接进来的 inbound Channel，由上层 EtcdNode 按 watchId 自己记录。</p>
 */
public class NettyChannelRegistry {

    /**
     * Netty 客户端启动器。
     *
     * <p>当指定 endpoint 暂无可用 Channel 时，通过 bootstrap 主动发起连接。</p>
     */
    private final Bootstrap bootstrap;

    /**
     * endpoint 到可用 Channel 的映射。
     *
     * <p>key 为 endpoint.endpointKey()，value 为当前 endpoint 对应的 active outbound Channel。</p>
     */
    private final Map<String, Channel> channelMap = new ConcurrentHashMap<>();

    /**
     * endpoint 到正在建立中的连接 Future 的映射。
     *
     * <p>
     * TODO:
     *  该映射用于合并同一个 endpoint 上的并发建连请求。
     *  当多个线程同时向同一个 endpoint 发送消息时，只允许复用同一个 connect future，避免重复创建多条不必要的连接。
     * </p>
     */
    private final Map<String, ChannelFuture> connectingFutureMap = new ConcurrentHashMap<>();

    /**
     * endpoint 维度的建连锁。
     *
     * <p>不同 endpoint 可以并发建连，同一个 endpoint 通过独立锁串行化建连检查和 connect 创建。</p>
     */
    private final Map<String, Object> channelLockMap = new ConcurrentHashMap<>();

    /**
     * 暂不可用 endpoint 的退避截止时间。
     *
     * <p>key 为 endpoint.endpointKey()，value 为该 endpoint 下次允许 best-effort 发送尝试的时间戳，单位毫秒。</p>
     */
    private final Map<String, Long> unavailableEndpointUntilMillisMap = new ConcurrentHashMap<>();

    /**
     * best-effort 发送失败后的退避时间，单位毫秒。
     *
     * <p>sendBestEffort 发送失败后不会阻塞重试，而是在该时间窗口内跳过同 endpoint 的发送尝试，避免对不可达节点持续高频建连。</p>
     */
    private final long bestEffortSendFailureBackoffMillis;

    public NettyChannelRegistry(Bootstrap bootstrap, long bestEffortSendFailureBackoffMillis) {
        if (bootstrap == null) {
            throw new IllegalArgumentException("bootstrap must not be null");
        }
        this.bootstrap = bootstrap;
        this.bestEffortSendFailureBackoffMillis = bestEffortSendFailureBackoffMillis;
    }

    /**
     * 获取或建立到指定 endpoint 的 active Channel。
     *
     * <p>该方法用于需要同步拿到连接的 RPC 调用场景，例如 call 和 heartbeat。
     * 方法会在 timeoutMillis 和内部最大等待时间之间取较小值，在截止时间前循环检查 active Channel、等待正在进行的 connect future，或发起新的连接。</p>
     *
     * @param endpoint      目标节点地址
     * @param timeoutMillis 调用方允许等待的超时时间，单位毫秒
     * @return 可用的 Netty Channel
     */
    public Channel getOrConnect(NodeEndpoint endpoint, long timeoutMillis) {
        long deadlineMillis = System.currentTimeMillis() + Math.min(timeoutMillis, 5000L);
        RpcException lastException = null;

        while (System.currentTimeMillis() < deadlineMillis) {
            // 1) 优先复用已经建立且仍然 active 的 outbound Channel。
            Channel channel = activeChannel(endpoint);
            if (channel != null) {
                return channel;
            }

            // 2) 当前没有可用连接时，发起连接或复用同 endpoint 上正在进行的连接。
            ChannelFuture future = connect(endpoint);
            try {
                long waitMillis = Math.max(1L, Math.min(1000L, deadlineMillis - System.currentTimeMillis()));
                boolean finished = future.await(waitMillis);
                if (!finished) {
                    continue;
                }
                if (future.isSuccess()) {
                    return future.channel();
                }

                // 3) 建连失败后标记 endpoint 暂不可用，并短暂 sleep 后继续在截止时间前重试。
                markEndpointUnavailable(endpoint);
                lastException = new RpcException("open netty rpc connection failed, endpoint=" + endpoint.endpointKey(), future.cause());
                sleepBeforeRetry();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RpcException("open netty rpc connection interrupted, endpoint=" + endpoint.endpointKey(), e);
            }
        }

        if (lastException != null) {
            throw lastException;
        }
        throw new RpcException("open netty rpc connection timeout, endpoint=" + endpoint.endpointKey());
    }

    /**
     * best-effort 发送 RPC 消息。
     *
     * <p>
     * TODO 高亮：该方法是不可靠发送，不保证消息一定写出。
     * 如果 endpoint 正处于失败退避窗口、建连失败、连接尚未建立或写出失败，本次消息可能被直接丢弃；
     * 方法不会阻塞等待连接，也不会向调用方返回发送结果。
     * </p>
     *
     * <p>该方法只适合 Raft AppendEntries、heartbeat 等上层具备重试机制的内部消息。
     * 如果调用方需要可靠送达、错误感知或响应结果，应使用 RpcClient.call 或自行设计带 Future 的发送流程。</p>
     *
     * @param endpoint 目标节点地址
     * @param message  RPC 消息
     */
    public void sendBestEffort(final NodeEndpoint endpoint, final RpcMessage message) {
        if (endpoint == null || message == null) {
            return;
        }

        // endpoint 处于失败退避窗口内时，直接丢弃本次消息，避免频繁重连不可达节点。
        if (isEndpointUnavailable(endpoint)) {
            return;
        }

        // 1) 有可用连接时直接复用；写失败只更新退避状态，不向调用方抛异常。
        Channel channel = activeChannel(endpoint);
        if (channel != null) {
            writeBestEffort(endpoint, channel, message);
            return;
        }

        // 2) 没有可用连接时异步建连，connect 内部会合并同 endpoint 的并发建连。
        ChannelFuture future;
        try {
            future = connect(endpoint);
        } catch (Exception e) {
            markEndpointUnavailable(endpoint);
            return;
        }

        // 3) 连接成功后再写消息；连接失败则标记退避，不在当前调用中阻塞重试。
        future.addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture connectFuture) {
                if (!connectFuture.isSuccess()) {
                    markEndpointUnavailable(endpoint);
                    return;
                }
                unavailableEndpointUntilMillisMap.remove(endpoint.endpointKey());
                writeBestEffort(endpoint, connectFuture.channel(), message);
            }
        });
    }

    /**
     * best-effort 写出消息。
     *
     * <p>写失败不会回调调用方，只会标记 endpoint 暂不可用。
     * 调用方必须依赖上层协议重试，例如 Raft 后续 tick 再次发送 AppendEntries。</p>
     */
    private void writeBestEffort(final NodeEndpoint endpoint, Channel channel, RpcMessage message) {
        channel.writeAndFlush(message).addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture writeFuture) {
                if (!writeFuture.isSuccess()) {
                    markEndpointUnavailable(endpoint);
                }
            }
        });
    }

    /**
     * 关闭全部 outbound Channel。
     *
     * <p>通常在 RPC 客户端关闭时调用，用于释放主动连接和清理连接状态。</p>
     */
    public void closeAll() {
        for (Channel channel : channelMap.values()) {
            if (channel != null) {
                channel.close();
            }
        }
        channelMap.clear();
        connectingFutureMap.clear();
        unavailableEndpointUntilMillisMap.clear();

        // Registry 生命周期通常和 RpcClient 一致；关闭客户端时可以顺带清理 endpoint 锁对象。
        channelLockMap.clear();
    }

    /**
     * 获取指定 endpoint 当前可复用的 active Channel。
     *
     * <p>如果缓存的 Channel 已经失效，会从 channelMap 中移除，避免后续继续复用失效连接。</p>
     *
     * @param endpoint 目标节点地址
     * @return 可用 Channel；不存在或已失效时返回 null
     */
    private Channel activeChannel(NodeEndpoint endpoint) {
        if (endpoint == null) {
            return null;
        }
        Channel channel = channelMap.get(endpoint.endpointKey());
        if (channel != null && channel.isActive()) {
            return channel;
        }
        if (channel != null) {
            channelMap.remove(endpoint.endpointKey(), channel);
        }
        return null;
    }

    /**
     * 获取或创建到指定 endpoint 的连接 Future。
     *
     * <p>处理流程：</p>
     * <p>1) 如果已有未完成的 connect future，则直接复用；</p>
     * <p>2) 进入 endpoint 维度锁后再次检查 active Channel 和 connect future；</p>
     * <p>3) 确认没有可用连接和建连任务后，才真正调用 bootstrap.connect；</p>
     * <p>4) 连接成功后写入 channelMap，连接关闭时再从 channelMap 移除。</p>
     *
     * @param endpoint 目标节点地址
     * @return 当前 endpoint 对应的连接 Future
     */
    private ChannelFuture connect(final NodeEndpoint endpoint) {
        final String endpointKey = endpoint.endpointKey();
        ChannelFuture existingFuture = connectingFutureMap.get(endpointKey);
        if (existingFuture != null && !existingFuture.isDone()) {
            return existingFuture;
        }

        Object lock = channelLock(endpointKey);
        synchronized (lock) {
            // 加锁后再次检查 active Channel，避免加锁前后其他线程已经完成建连。
            Channel channel = activeChannel(endpoint);
            if (channel != null) {
                return channel.newSucceededFuture();
            }

            // 加锁后再次检查正在建连的 Future，避免同 endpoint 重复 bootstrap.connect。
            existingFuture = connectingFutureMap.get(endpointKey);
            if (existingFuture != null && !existingFuture.isDone()) {
                return existingFuture;
            }

            ChannelFuture future = bootstrap.connect(endpoint.getHost(), endpoint.getPort());
            connectingFutureMap.put(endpointKey, future);
            future.addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture connectFuture) {
                    connectingFutureMap.remove(endpointKey, connectFuture);
                    if (!connectFuture.isSuccess()) {
                        markEndpointUnavailable(endpoint);
                        return;
                    }

                    final Channel channel = connectFuture.channel();
                    channelMap.put(endpointKey, channel);
                    unavailableEndpointUntilMillisMap.remove(endpointKey);

                    // Channel 关闭后只移除当前 Channel，避免误删同 endpoint 后续新建的 Channel。
                    channel.closeFuture().addListener(new ChannelFutureListener() {
                        @Override
                        public void operationComplete(ChannelFuture closeFuture) {
                            channelMap.remove(endpointKey, channel);
                        }
                    });
                }
            });
            return future;
        }
    }

    /**
     * 获取 endpoint 维度的建连锁。
     *
     * <p>不同 endpoint 使用不同锁，减少无关节点之间的建连竞争；
     * 同一个 endpoint 复用同一个锁，保证 connect 创建过程不会并发重复执行。</p>
     *
     * @param endpointKey endpoint 唯一标识
     * @return endpoint 对应的锁对象
     */
    private Object channelLock(String endpointKey) {
        Object lock = channelLockMap.get(endpointKey);
        if (lock != null) {
            return lock;
        }
        Object newLock = new Object();
        Object previousLock = channelLockMap.putIfAbsent(endpointKey, newLock);
        return previousLock == null ? newLock : previousLock;
    }

    /**
     * 建连失败后的短暂重试间隔。
     *
     * <p>该方法只服务于 getOrConnect 的同步等待路径，避免失败后在 while 循环中空转。</p>
     */
    private void sleepBeforeRetry() {
        try {
            Thread.sleep(50L);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * 判断 endpoint 当前是否处于不可用退避窗口内。
     *
     * @param endpoint 目标节点地址
     * @return true 表示暂时跳过 best-effort 发送，false 表示允许尝试发送或建连
     */
    private boolean isEndpointUnavailable(NodeEndpoint endpoint) {
        Long unavailableUntilMillis = unavailableEndpointUntilMillisMap.get(endpoint.endpointKey());
        return unavailableUntilMillis != null && unavailableUntilMillis.longValue() > System.currentTimeMillis();
    }

    /**
     * 标记 endpoint 暂不可用。
     *
     * <p>该标记主要影响 sendBestEffort：在退避窗口内，best-effort 发送会直接跳过，
     * 避免不可达 endpoint 被频繁异步建连。</p>
     *
     * @param endpoint 目标节点地址
     */
    private void markEndpointUnavailable(NodeEndpoint endpoint) {
        if (endpoint == null) {
            return;
        }
        unavailableEndpointUntilMillisMap.put(
                endpoint.endpointKey(),
                System.currentTimeMillis() + bestEffortSendFailureBackoffMillis
        );
    }
}