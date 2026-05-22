package com.xhj.etcd.console.model.response.common;

/**
 * ErrorCode
 *
 * @author XJks
 * @description HTTP 错误码接口，统一约束状态码与提示信息。
 */
public interface ErrorCode {

    /**
     * 获取状态码。
     */
    Integer getCode();

    /**
     * 获取提示信息。
     */
    String getMessage();
}
