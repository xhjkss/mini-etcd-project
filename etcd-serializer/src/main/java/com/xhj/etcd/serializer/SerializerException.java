package com.xhj.etcd.serializer;

/**
 * SerializerException
 *
 * @author XJks
 * @description 序列化异常，统一包装序列化和反序列化过程中的运行时错误。
 */
public class SerializerException extends RuntimeException {

    public SerializerException(String message) {
        super(message);
    }

    public SerializerException(String message, Throwable cause) {
        super(message, cause);
    }
}
