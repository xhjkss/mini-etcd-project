package com.xhj.etcd.serializer.impl;

import com.xhj.etcd.serializer.Serializer;
import com.xhj.etcd.serializer.SerializerException;
import com.xhj.etcd.serializer.SerializerType;
import com.google.protobuf.MessageLite;

import java.lang.reflect.Method;

/**
 * ProtobufSerializer
 *
 * @author XJks
 * @description Protobuf 序列化预留实现，用于后续扩展更紧凑的二进制协议。
 */
public class ProtobufSerializer implements Serializer {

    // ==================== Serializer name ====================

    @Override
    public String name() {
        return SerializerType.PROTOBUF.getName();
    }


    // ==================== Serialize ====================

    @Override
    public byte[] serialize(Object object) {
        if (object == null) {
            return new byte[0];
        }

        if (!(object instanceof MessageLite)) {
            throw new SerializerException("protobuf serializer only supports com.google.protobuf.MessageLite, objectClass=" + object.getClass().getName());
        }

        return ((MessageLite) object).toByteArray();
    }


    // ==================== Deserialize ====================

    @Override
    public <T> T deserialize(byte[] data, Class<T> targetClass) {
        if (data == null || data.length == 0) {
            return null;
        }

        if (!MessageLite.class.isAssignableFrom(targetClass)) {
            throw new SerializerException("protobuf deserializer only supports com.google.protobuf.MessageLite, targetClass=" + targetClass.getName());
        }

        try {
            Method parseFromMethod = targetClass.getMethod("parseFrom", byte[].class);
            Object object = parseFromMethod.invoke(null, data);
            return targetClass.cast(object);
        } catch (Exception e) {
            throw new SerializerException("protobuf deserialize failed, targetClass=" + targetClass.getName(), e);
        }
    }
}
