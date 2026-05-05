package com.xhj.etcd.serializer.impl;

import com.xhj.etcd.serializer.Serializer;
import com.xhj.etcd.serializer.SerializerException;
import com.xhj.etcd.serializer.SerializerType;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * JsonSerializer
 *
 * @author XJks
 * @description JSON 序列化实现，适合需要可读文本格式的调试场景。
 */
public class JsonSerializer implements Serializer {

    private final ObjectMapper objectMapper = new ObjectMapper();

    // ==================== Serializer name ====================

    @Override
    public String name() {
        return SerializerType.JSON.getName();
    }


    // ==================== Serialize ====================

    @Override
    public byte[] serialize(Object object) {
        if (object == null) {
            return new byte[0];
        }

        try {
            return objectMapper.writeValueAsBytes(object);
        } catch (Exception e) {
            throw new SerializerException("jackson json serialize failed, objectClass=" + object.getClass().getName(), e);
        }
    }


    // ==================== Deserialize ====================

    @Override
    public <T> T deserialize(byte[] data, Class<T> targetClass) {
        if (data == null || data.length == 0) {
            return null;
        }

        try {
            return objectMapper.readValue(data, targetClass);
        } catch (Exception e) {
            throw new SerializerException("jackson json deserialize failed, targetClass=" + targetClass.getName(), e);
        }
    }
}
