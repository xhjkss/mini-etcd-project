package com.xhj.etcd.console.controller;

import com.xhj.etcd.console.model.response.common.BaseResponse;
import com.xhj.etcd.console.model.response.watch.WatchSessionResponse;
import com.xhj.etcd.console.service.WatchService;
import com.xhj.etcd.kernel.etcd.etcdrpc.WatchSubscribeRequest;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import org.springframework.beans.factory.annotation.Autowired;

import java.util.List;

/**
 * WatchController
 *
 * @author XJks
 * @description Watch 订阅管理接口。
 */
@RestController
@RequestMapping("/api/watch")
public class WatchController {

    // ==================== 依赖组件 ====================
    /**
     * Watch 服务。
     */
    @Autowired
    private WatchService watchService;

    /**
     * 查询 watch 会话列表。
     */
    @GetMapping
    public BaseResponse<List<WatchSessionResponse>> listWatchSessions() {
        return BaseResponse.success(watchService.listWatchSessions());
    }

    /**
     * 启动 watch 会话。
     */
    @PostMapping("/start")
    public BaseResponse<WatchSessionResponse> startWatch(@RequestParam String host,
                                                         @RequestParam int port,
                                                         @RequestBody WatchSubscribeRequest watchSubscribeRequest) {
        if (watchSubscribeRequest == null) {
            throw new IllegalArgumentException("watchSubscribeRequest must not be null");
        }
        // 控制台 watch 默认允许落到任意已连接节点（包含 follower 观察视图）。
        watchSubscribeRequest.setLeaderOnly(false);
        return BaseResponse.success(watchService.startWatchOnEndpoint(host, port, watchSubscribeRequest));
    }

    /**
     * 取消 watch 会话。
     */
    @DeleteMapping
    public BaseResponse<Void> cancelWatch(@RequestParam long watchId) {
        watchService.cancelWatchByWatchId(watchId);
        return BaseResponse.success();
    }
}
