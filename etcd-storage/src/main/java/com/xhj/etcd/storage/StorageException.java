package com.xhj.etcd.storage;

/**
 * StorageException
 *
 * @author XJks
 * @description 存储异常，统一包装文件或内存存储访问中的运行时错误。
 */
public class StorageException extends RuntimeException {

    public StorageException(String message) {
        super(message);
    }

    public StorageException(String message, Throwable cause) {
        super(message, cause);
    }
}
