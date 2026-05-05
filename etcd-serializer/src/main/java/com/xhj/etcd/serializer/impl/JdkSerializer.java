package com.xhj.etcd.serializer.impl;

import com.xhj.etcd.serializer.Serializer;
import com.xhj.etcd.serializer.SerializerException;
import com.xhj.etcd.serializer.SerializerType;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

/**
 * JdkSerializer
 *
 * @author XJks
 * @description JDK 原生序列化实现，适合学习和兜底场景使用。
 */
public class JdkSerializer implements Serializer {

    // ==================== Serializer name ====================

    @Override
    public String name() {
        return SerializerType.JDK.getName();
    }

    // ==================== Serialize ====================

    @Override
    public byte[] serialize(Object object) {
        if (object == null) {
            return new byte[0];
        }

        try {
            ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
            ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);
            objectOutputStream.writeObject(object);
            objectOutputStream.flush();
            return byteArrayOutputStream.toByteArray();
        } catch (Exception e) {
            throw new SerializerException("jdk serialize failed, objectClass=" + object.getClass().getName(), e);
        }
    }

    // ==================== Deserialize ====================

    @Override
    public <T> T deserialize(byte[] data, Class<T> targetClass) {
        if (data == null || data.length == 0) {
            return null;
        }

        try {
            ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(data);
            ObjectInputStream objectInputStream = new ObjectInputStream(byteArrayInputStream);
            Object object = objectInputStream.readObject();
            return targetClass.cast(object);
        } catch (Exception e) {
            throw new SerializerException("jdk deserialize failed, targetClass=" + targetClass.getName(), e);
        }
    }
}
