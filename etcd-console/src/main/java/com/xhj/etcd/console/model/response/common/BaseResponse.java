package com.xhj.etcd.console.model.response.common;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Data;

/**
 * BaseResponse
 *
 * @author XJks
 * @description HTTP 统一返回模型，封装状态码、提示信息与业务数据。
 */
@Data
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class BaseResponse<T> {

    /**
     * 状态码。
     */
    private Integer code;

    /**
     * 消息内容。
     */
    private String message;

    /**
     * 数据载荷。
     */
    private T data;

    /**
     * 构建成功响应（无数据）。
     *
     * @param <T> 数据泛型
     * @return 成功响应
     */
    public static <T> BaseResponse<T> success() {
        return success(null);
    }

    /**
     * 构建成功响应（携带数据）。
     *
     * @param data 业务数据
     * @param <T>  数据泛型
     * @return 成功响应
     */
    public static <T> BaseResponse<T> success(T data) {
        return build(CommonErrorEnum.SUCCESS.getCode(), CommonErrorEnum.SUCCESS.getMessage(), data);
    }

    /**
     * 构建失败响应（无数据）。
     *
     * @param code    错误码
     * @param message 错误信息
     * @param <T>     数据泛型
     * @return 失败响应
     */
    public static <T> BaseResponse<T> fail(Integer code, String message) {
        return build(code, message, null);
    }

    /**
     * 构建失败响应（携带数据）。
     *
     * @param code    错误码
     * @param message 错误信息
     * @param data    业务数据
     * @param <T>     数据泛型
     * @return 失败响应
     */
    public static <T> BaseResponse<T> fail(Integer code, String message, T data) {
        return build(code, message, data);
    }

    /**
     * 按统一错误码构建失败响应。
     *
     * @param errorCode 错误码对象
     * @param <T>       数据泛型
     * @return 失败响应
     */
    public static <T> BaseResponse<T> fail(ErrorCode errorCode) {
        return build(errorCode.getCode(), errorCode.getMessage(), null);
    }

    /**
     * 构建统一响应对象。
     *
     * @param code    状态码
     * @param message 提示信息
     * @param data    业务数据
     * @param <T>     数据泛型
     * @return 统一响应对象
     */
    private static <T> BaseResponse<T> build(Integer code, String message, T data) {
        BaseResponse<T> response = new BaseResponse<>();
        response.setCode(code);
        response.setMessage(message);
        response.setData(data);
        return response;
    }

    /**
     * 判断是否成功。
     *
     * @return true 表示成功
     */
    public boolean isSuccess() {
        return CommonErrorEnum.SUCCESS.getCode().equals(this.code);
    }
}
