package com.xhj.etcd.console.model.response.common;

/**
 * CommonErrorEnum
 *
 * @author XJks
 * @description 控制台通用错误码。
 */
public enum CommonErrorEnum implements ErrorCode {
    /**
     * 请求成功。
     */
    SUCCESS(0, "success"),

    /**
     * 请求参数错误。
     */
    PARAM_ERROR(400, "请求参数错误"),

    /**
     * 资源不存在。
     */
    NOT_FOUND(404, "资源不存在"),

    /**
     * 服务端内部错误。
     */
    INTERNAL_ERROR(500, "服务端内部错误");

    /**
     * 错误码。
     */
    private final Integer code;

    /**
     * 错误信息。
     */
    private final String message;

    CommonErrorEnum(Integer code, String message) {
        this.code = code;
        this.message = message;
    }

    @Override
    public Integer getCode() {
        return code;
    }

    @Override
    public String getMessage() {
        return message;
    }
}
