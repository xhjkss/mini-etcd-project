package com.xhj.etcd.kernel.etcd.etcdrpc;

import com.xhj.etcd.kernel.etcd.command.EtcdCommandApplyResult;
import com.xhj.etcd.kernel.etcd.command.EtcdCommandApplyResultType;
import lombok.Data;

import java.io.Serializable;

/**
 * EtcdResponseHeader
 *
 * @author XJks
 * @description mini etcd RPC 响应头，用于承载所有响应共有的执行状态、错误信息和 Leader 路由信息。
 *
 * <p>阶段边界：</p>
 * <p>当前项目是 mini etcd，不实现完整 etcd 响应头中的 memberId、revision、raftTerm、clusterId 等元信息。
 * 这些字段如果没有真实维护逻辑，不应该提前出现在响应模型中，否则会让调用方误以为当前阶段已经支持对应语义。</p>
 *
 * <p>职责边界：</p>
 * <ul>
 *     <li>通用成功 / 失败状态放在 header 中。</li>
 *     <li>notLeader 和 leaderId 用于客户端进行 Leader 路由重试。</li>
 *     <li>业务响应体只保存业务字段，例如 value、deletedCount、items 等。</li>
 * </ul>
 */
@Data
public class EtcdResponseHeader implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * RPC 请求是否处理成功。
     *
     * <p>true 表示命令已经在当前阶段语义下成功处理；
     * false 表示发生 notLeader、timeout、conflict 或普通错误。</p>
     */
    private boolean success = true;

    /**
     * 当前节点是否不是 Leader。
     *
     * <p>当前 mini etcd 的 PUT、DELETE、GET、LIST_KEYS 都会进入 Raft 顺序流，
     * 因此请求必须由 Leader 处理。Follower 收到请求时会通过该字段提示客户端重试 Leader。</p>
     */
    private boolean notLeader;

    /**
     * 当前已知 Leader 节点 ID。
     *
     * <p>当 notLeader 为 true 时，客户端会使用该字段在本地 endpointMap 中查找 Leader 地址并重试。</p>
     */
    private String leaderId;

    /**
     * 通用响应消息。
     *
     * <p>失败时保存错误原因；成功时可以为空。
     * 业务数据不放在该字段中，而是放在 EtcdRpcResponse.body 中。</p>
     */
    private String message;

    public EtcdResponseHeader() {
    }

    /**
     * 构造成功响应头。
     *
     * @return 成功响应头
     */
    public static EtcdResponseHeader success() {
        EtcdResponseHeader header = new EtcdResponseHeader();
        header.success = true;
        return header;
    }

    /**
     * 构造错误响应头。
     *
     * @param message 错误消息
     * @return 错误响应头
     */
    public static EtcdResponseHeader error(String message) {
        EtcdResponseHeader header = new EtcdResponseHeader();
        header.success = false;
        header.message = message;
        return header;
    }

    /**
     * 根据命令 apply 结果构造响应头。
     *
     * <p>处理流程：</p>
     * <p>1) apply 结果为空时，返回通用错误头；</p>
     * <p>2) SUCCESS 表示命令已经完成 apply，响应头标记为成功；</p>
     * <p>3) NOT_LEADER 表示当前节点无法处理该请求，需要把 leaderId 写入响应头；</p>
     * <p>4) TIMEOUT、CONFLICT、ERROR 等其他失败类型统一写入 message，由客户端或测试用例感知失败原因。</p>
     *
     * @param result 命令 apply 结果
     * @return RPC 响应头
     */
    public static EtcdResponseHeader fromApplyResult(EtcdCommandApplyResult result) {
        if (result == null) {
            return error("command apply result must not be null");
        }

        EtcdResponseHeader header = new EtcdResponseHeader();
        header.success = result.getType() == EtcdCommandApplyResultType.SUCCESS;
        header.message = result.getMessage();

        if (result.getType() == EtcdCommandApplyResultType.NOT_LEADER) {
            // 当前 EtcdCommandApplyResult.notLeader 使用 message 字段承载 leaderId。
            header.notLeader = true;
            header.leaderId = result.getMessage();
        }

        return header;
    }

    /**
     * 当前响应是否可以进行 Leader 路由重试。
     *
     * <p>只有同时满足 notLeader=true 且 leaderId 非空时，客户端才具备自动跳转 Leader 的必要信息。</p>
     *
     * @return true 表示客户端可以根据 leaderId 重试 Leader
     */
    public boolean shouldRetryLeader() {
        return notLeader && leaderId != null && leaderId.trim().length() > 0;
    }
}
