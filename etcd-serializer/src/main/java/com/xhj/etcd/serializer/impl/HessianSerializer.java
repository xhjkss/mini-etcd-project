package com.xhj.etcd.serializer.impl;

import com.xhj.etcd.serializer.Serializer;
import com.xhj.etcd.serializer.SerializerException;
import com.xhj.etcd.serializer.SerializerType;
import com.caucho.hessian.io.HessianInput;
import com.caucho.hessian.io.HessianOutput;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

/**
 * HessianSerializer
 *
 * @author XJks
 * @description Hessian 序列化实现，适合二进制跨语言友好场景。
 */
public class HessianSerializer implements Serializer {

    // ==================== Serializer name ====================

    @Override
    public String name() {
        return SerializerType.HESSIAN.getName();
    }


    // ==================== Serialize ====================

    @Override
    public byte[] serialize(Object object) {
        if (object == null) {
            return new byte[0];
        }

        try {
            ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
            HessianOutput hessianOutput = new HessianOutput(byteArrayOutputStream);
            hessianOutput.writeObject(object);
            hessianOutput.flush();
            return byteArrayOutputStream.toByteArray();
        } catch (Exception e) {
            throw new SerializerException("hessian serialize failed, objectClass=" + object.getClass().getName(), e);
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
            HessianInput hessianInput = new HessianInput(byteArrayInputStream);
            Object object = hessianInput.readObject(targetClass);
            return targetClass.cast(object);
        } catch (Exception e) {
            throw new SerializerException("hessian deserialize failed, targetClass=" + targetClass.getName(), e);
        }
    }
}
