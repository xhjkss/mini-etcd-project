package com.xhj.etcd.serializer;

/**
 * Serializer
 *
 * @author XJks
 * @description 序列化能力接口，定义对象和字节数组之间的统一转换边界。
 */
public interface Serializer {

    // ==================== Serialize ====================

    /**
     * 把对象转换为字节数组。
     */
    byte[] serialize(Object object);

    // ==================== Deserialize ====================

    /**
     * 把字节数组还原为目标对象。
     */
    <T> T deserialize(byte[] data, Class<T> targetClass);

    // ==================== Serializer name ====================

    /**
     * 返回当前序列化器名称。
     */
    String name();
}
