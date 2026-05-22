package com.xhj.etcd.console.controller;

import com.xhj.etcd.console.model.response.common.BaseResponse;
import com.xhj.etcd.console.service.KeyValueService;
import com.xhj.etcd.kernel.etcd.etcdrpc.DeleteRangeRequest;
import com.xhj.etcd.kernel.etcd.etcdrpc.DeleteRangeResponse;
import com.xhj.etcd.kernel.etcd.etcdrpc.DeleteRequest;
import com.xhj.etcd.kernel.etcd.etcdrpc.DeleteResponse;
import com.xhj.etcd.kernel.etcd.etcdrpc.GetRequest;
import com.xhj.etcd.kernel.etcd.etcdrpc.GetResponse;
import com.xhj.etcd.kernel.etcd.etcdrpc.PutRequest;
import com.xhj.etcd.kernel.etcd.etcdrpc.PutResponse;
import com.xhj.etcd.kernel.etcd.etcdrpc.RangeRequest;
import com.xhj.etcd.kernel.etcd.etcdrpc.RangeResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * KeyValueController
 *
 * @author XJks
 * @description MVCC 查询与写入接口。
 */
@RestController
@RequestMapping("/api/mvcc")
public class KeyValueController {

    // ==================== 依赖组件 ====================
    /**
     * KV 服务。
     */
    @Autowired
    private KeyValueService keyValueService;

    /**
     * GET 查询（支持线性一致读与历史 revision 读取）。
     */
    @PostMapping("/get")
    public BaseResponse<GetResponse> getOnEndpoint(@RequestParam String host,
                                                   @RequestParam int port,
                                                   @RequestBody GetRequest getRequest) {
        if (getRequest == null) {
            throw new IllegalArgumentException("getRequest must not be null");
        }
        return BaseResponse.success(keyValueService.getOnEndpoint(host, port, getRequest));
    }

    /**
     * RANGE 查询（由请求体参数决定普通区间/前缀/全量语义）。
     */
    @PostMapping("/range")
    public BaseResponse<RangeResponse> rangeOnEndpoint(@RequestParam String host,
                                                       @RequestParam int port,
                                                       @RequestBody RangeRequest rangeRequest) {
        if (rangeRequest == null) {
            throw new IllegalArgumentException("rangeRequest must not be null");
        }
        return BaseResponse.success(keyValueService.rangeOnEndpoint(host, port, rangeRequest));
    }

    /**
     * PUT 写入。
     */
    @PostMapping("/put")
    public BaseResponse<PutResponse> put(@RequestBody PutRequest putRequest) {
        return BaseResponse.success(keyValueService.put(putRequest));
    }

    /**
     * DELETE 删除。
     */
    @PostMapping("/delete")
    public BaseResponse<DeleteResponse> delete(@RequestBody DeleteRequest deleteRequest) {
        return BaseResponse.success(keyValueService.delete(deleteRequest));
    }

    /**
     * DELETE_RANGE 删除（由请求体参数决定区间/前缀语义）。
     */
    @PostMapping("/delete-range")
    public BaseResponse<DeleteRangeResponse> deleteRange(@RequestBody DeleteRangeRequest deleteRangeRequest) {
        if (deleteRangeRequest == null) {
            throw new IllegalArgumentException("deleteRangeRequest must not be null");
        }
        return BaseResponse.success(keyValueService.deleteRange(deleteRangeRequest));
    }
}
