package com.xhj.etcd.kernel.etcd.command;

import com.xhj.etcd.serializer.Serializer;

/**
 * EtcdCommandCodec
 *
 * @author XJks
 * @description Etcd 命令辅助器，负责 EtcdCommand 的日志字节编解码，以及命令 data 的类型安全读取。
 *
 * <p>设计边界：</p>
 * <ul>
 *     <li>EtcdCommand.data 在内存中使用 Object，避免为每种请求再创建一层中间命令对象。</li>
 *     <li>EtcdCommand 进入 Raft 日志前必须整体序列化为 byte[]，保证日志复制、持久化和重启恢复有稳定表达。</li>
 *     <li>apply 阶段从 RaftApplyMessage.commandData 反序列化 EtcdCommand 后，通过 decodeCommandData 按 type 读取 XxxRequest。</li>
 * </ul>
 */
public class EtcdCommandCodec {

    private final Serializer serializer;

    public EtcdCommandCodec(Serializer serializer) {
        if (serializer == null) {
            throw new IllegalArgumentException("serializer must not be null");
        }
        this.serializer = serializer;
    }

    // ==================== Log boundary encode / decode ====================

    /**
     * 将 EtcdCommand 编码为 Raft 日志命令字节。
     *
     * @param command Etcd 命令信封
     * @return 可写入 RaftLogEntry.commandData 的字节数组
     */
    public byte[] encodeEtcdCommand(EtcdCommand command) {
        return serializer.serialize(command);
    }

    /**
     * 从 Raft 日志命令字节还原 EtcdCommand。
     *
     * @param data RaftApplyMessage.commandData
     * @return Etcd 命令信封
     */
    public EtcdCommand decodeEtcdCommand(byte[] data) {
        return serializer.deserialize(data, EtcdCommand.class);
    }

    // ==================== Command data access ====================

    /**
     * 按命令类型读取 data 对象。
     *
     * <p>Object data 可以精简模型，但必须集中做类型校验，避免 apply 阶段把 GET 命令误当成 PUT 请求处理。</p>
     *
     * @param command      Etcd 命令信封
     * @param expectedType 期望命令类型
     * @param dataClass    期望 data 类型
     * @param <T>          data 类型
     * @return 类型安全的 data 对象
     */
    public <T> T decodeCommandData(EtcdCommand command, EtcdCommandType expectedType, Class<T> dataClass) {
        assertCommandType(command, expectedType);
        Object data = command.getData();
        if (data == null) {
            return null;
        }
        if (!dataClass.isInstance(data)) {
            throw new IllegalArgumentException("unexpected etcd command data type, expected="
                    + dataClass.getName() + ", actual=" + data.getClass().getName());
        }
        return dataClass.cast(data);
    }

    private void assertCommandType(EtcdCommand command, EtcdCommandType expectedType) {
        if (command == null) {
            throw new IllegalArgumentException("etcd command must not be null");
        }
        if (command.getType() != expectedType) {
            throw new IllegalArgumentException("unexpected command type, expected=" + expectedType + ", actual=" + command.getType());
        }
    }
}
