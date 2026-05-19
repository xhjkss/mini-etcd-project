package com.xhj.etcd.rpc;

import io.netty.channel.ChannelId;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * RpcMessageHandlerRegistry
 *
 * @author XJks
 * @description RPC 消息处理器注册表，负责维护 rpcMessageId 到消息处理器注册关系的映射。
 */
public class RpcMessageHandlerRegistry {

    /**
     * 消息处理器注册关系映射。
     *
     * <p>key 为 rpcMessageId，value 为当前 rpcMessageId 对应的处理器注册关系。</p>
     *
     * <p>TODO: 注册表使用 ConcurrentHashMap 支持网络线程、业务线程、超时清理线程并发访问；注销时应优先使用 removeIfMatch，避免误删同一个 rpcMessageId 下重新注册的新处理器。</p>
     */
    private final Map<String, RpcMessageHandlerRegistration> registrationMap = new ConcurrentHashMap<>();

    /**
     * 注册消息处理器（带连接 ID）。
     *
     * @param rpcMessageId RPC 消息 ID
     * @param handler      消息处理器
     * @param channelId    注册时绑定的连接 ID，可为空
     * @return 处理器注册关系
     */
    public RpcMessageHandlerRegistration register(String rpcMessageId, RpcMessageHandler handler, ChannelId channelId) {
        if (rpcMessageId == null || rpcMessageId.trim().length() == 0) {
            throw new IllegalArgumentException("rpcMessageId must not be empty");
        }
        if (handler == null) {
            throw new IllegalArgumentException("handler must not be null");
        }
        RpcMessageHandlerRegistration registration = new RpcMessageHandlerRegistration(rpcMessageId, handler, channelId, this);
        registrationMap.put(rpcMessageId, registration);
        return registration;
    }

    /**
     * 根据 RPC 消息 ID 查询处理器注册关系。
     *
     * @param rpcMessageId RPC 消息 ID
     * @return 处理器注册关系；不存在时返回 null
     */
    public RpcMessageHandlerRegistration get(String rpcMessageId) {
        if (rpcMessageId == null) {
            return null;
        }
        return registrationMap.get(rpcMessageId);
    }

    /**
     * 根据 RPC 消息 ID 删除注册关系。
     *
     * <p>该方法只按 rpcMessageId 删除，不校验处理器实例。
     * 如果调用方持有的是某一次 register 返回的 registration，优先使用 registration.remove()。</p>
     *
     * @param rpcMessageId RPC 消息 ID
     */
    public void remove(String rpcMessageId) {
        if (rpcMessageId != null) {
            registrationMap.remove(rpcMessageId);
        }
    }

    /**
     * 在注册关系仍然匹配时删除。
     *
     * <p>TODO: 该方法用于支持 Registration 作为注销句柄的语义：只有注册表中当前保存的 registration 与 expectedRegistration 是同一个对象时，才允许删除。</p>
     *
     * @param rpcMessageId         RPC 消息 ID
     * @param expectedRegistration 期望删除的注册关系
     * @return true 表示删除成功，false 表示注册关系不存在或已经被覆盖
     */
    public boolean removeIfMatch(String rpcMessageId, RpcMessageHandlerRegistration expectedRegistration) {
        if (rpcMessageId == null || expectedRegistration == null) {
            return false;
        }
        return registrationMap.remove(rpcMessageId, expectedRegistration);
    }

    /**
     * 列出当前所有处理器注册关系。
     *
     * <p>返回的是注册关系快照，后续注册表变化不会影响该列表。</p>
     *
     * @return 当前注册关系列表
     */
    public List<RpcMessageHandlerRegistration> listRegistrations() {
        return new ArrayList<>(registrationMap.values());
    }

    /**
     * 清空全部处理器注册关系。
     *
     * <p>通常用于 RPC 客户端或服务端关闭时释放等待响应、流式通道等处理器。</p>
     */
    public void clear() {
        registrationMap.clear();
    }
}