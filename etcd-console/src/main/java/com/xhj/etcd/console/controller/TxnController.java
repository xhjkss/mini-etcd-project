package com.xhj.etcd.console.controller;

import com.xhj.etcd.console.model.response.common.BaseResponse;
import com.xhj.etcd.console.service.TxnService;
import com.xhj.etcd.kernel.etcd.etcdrpc.TxnRequest;
import com.xhj.etcd.kernel.etcd.etcdrpc.TxnResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * TxnController
 *
 * @author XJks
 * @description Console 事务（Txn）接口。
 */
@RestController
@RequestMapping("/api/txn")
public class TxnController {

    /**
     * Txn 服务。
     */
    @Autowired
    private TxnService txnService;

    /**
     * 执行 Txn。
     */
    @PostMapping("/execute")
    public BaseResponse<TxnResponse> execute(@RequestBody TxnRequest txnRequest) {
        return BaseResponse.success(txnService.execute(txnRequest));
    }
}
