package com.xhj.etcd.console.controller;

import com.xhj.etcd.console.model.response.common.BaseResponse;
import com.xhj.etcd.console.service.CompactService;
import com.xhj.etcd.kernel.etcd.etcdrpc.CompactRequest;
import com.xhj.etcd.kernel.etcd.etcdrpc.CompactResponse;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import org.springframework.beans.factory.annotation.Autowired;

/**
 * CompactController
 *
 * @author XJks
 * @description Compact 接口。
 */
@RestController
@RequestMapping("/api/compact")
public class CompactController {

    // ==================== 依赖组件 ====================
    /**
     * Compact 服务。
     */
    @Autowired
    private CompactService compactService;

    /**
     * 执行 compact 请求。
     */
    @PostMapping
    public BaseResponse<CompactResponse> compact(@RequestBody CompactRequest compactRequest) {
        return BaseResponse.success(compactService.compact(compactRequest));
    }
}
