package com.xhj.etcd.console.exception;

import com.xhj.etcd.console.model.response.common.BaseResponse;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

/**
 * ConsoleExceptionHandler
 *
 * @author XJks
 * @description 控制台统一异常处理器。
 */
@RestControllerAdvice
public class ConsoleExceptionHandler {

    /**
     * 捕获并转换未处理异常为统一 BaseResponse。
     *
     * @param exception 异常对象
     * @return 失败响应
     */
    @ExceptionHandler(Exception.class)
    public BaseResponse<Void> handleException(Exception exception) {
        return BaseResponse.fail(500, exception.getMessage());
    }
}
