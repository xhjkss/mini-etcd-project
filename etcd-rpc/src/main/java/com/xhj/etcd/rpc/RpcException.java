package com.xhj.etcd.rpc;

/**
 * RpcException
 *
 * @author XJks
 * @description RPC 异常，统一包装网络调用和消息分发过程中的运行时错误。
 */
public class RpcException extends RuntimeException {
    public RpcException(String message) {
        super(message);
    }

    public RpcException(String message, Throwable cause) {
        super(message, cause);
    }
}
