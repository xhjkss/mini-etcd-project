package com.xhj.etcd.kernel.server.etcdrpc;

import com.xhj.etcd.kernel.server.command.EtcdCommandApplyResult;
import lombok.Data;

import java.io.Serializable;

/**
 * EtcdRpcResponse
 *
 * @param <T> 业务响应体类型
 * @author XJks
 * @description mini etcd RPC 统一响应信封，用于封装通用响应头和具体业务响应体。
 *
 * <p>阶段边界：</p>
 * <p>当前项目是 mini etcd，不实现完整 etcd 响应模型中的 member、cluster、revision 等元信息。
 * 通用成功状态、错误信息和 Leader 路由信息统一放在 header 中；
 * PUT、DELETE、GET、LIST_KEYS 等具体操作的返回数据放在 body 中。</p>
 *
 * <p>职责边界：</p>
 * <ul>
 *     <li>header：保存 success、notLeader、leaderId、message 等通用元信息。</li>
 *     <li>body：保存具体业务响应，例如 PutResponse、DeleteResponse、GetResponse、ListKeysResponse。</li>
 *     <li>不保存完整 etcd server 的 memberId、clusterId、revision、raftTerm 等未来扩展字段。</li>
 * </ul>
 */
@Data
public class EtcdRpcResponse<T> implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * RPC 通用响应头。
     *
     * <p>用于表达请求是否成功、失败原因，以及 notLeader 时客户端是否可以跳转 Leader。</p>
     */
    private EtcdResponseHeader header;

    /**
     * 具体业务响应体。
     *
     * <p>该字段只保存具体 RPC 方法的业务返回数据。
     * 例如 GET 返回 GetResponse，DELETE 返回 DeleteResponse。</p>
     */
    private T body;

    public EtcdRpcResponse() {
    }

    public EtcdRpcResponse(EtcdResponseHeader header, T body) {
        this.header = header;
        this.body = body;
    }

    /**
     * 构造成功响应信封。
     *
     * <p>成功状态写入 header，业务数据写入 body。</p>
     *
     * @param body 业务响应体
     * @param <T>  业务响应体类型
     * @return RPC 响应信封
     */
    public static <T> EtcdRpcResponse<T> success(T body) {
        return of(EtcdResponseHeader.success(), body);
    }

    /**
     * 根据命令 apply 结果构造 RPC 响应信封。
     *
     * <p>当前阶段 PUT、DELETE、GET、LIST_KEYS 都会经过 Raft propose 和 apply。
     * EtcdNode apply 完成后会得到 EtcdCommandApplyResult，再通过该方法转换为 RPC 响应。</p>
     *
     * <p>处理规则：</p>
     * <p>1) apply 成功时，header.success=true，body 保存具体业务响应；</p>
     * <p>2) notLeader、timeout、conflict、error 等失败信息写入 header；</p>
     * <p>3) body 是否为空由调用方决定，通常失败场景可以为 null。</p>
     *
     * @param result 命令 apply 结果
     * @param body   业务响应体
     * @param <T>    业务响应体类型
     * @return RPC 响应信封
     */
    public static <T> EtcdRpcResponse<T> fromApplyResult(EtcdCommandApplyResult result, T body) {
        return of(EtcdResponseHeader.fromApplyResult(result), body);
    }

    /**
     * 构造 RPC 响应信封。
     *
     * <p>如果调用方没有传入 header，则默认构造成功响应头。
     * 这样可以避免响应信封 header 为空导致客户端判断 Leader 路由时出现空指针。</p>
     *
     * @param header 响应头；为 null 时默认成功
     * @param body   业务响应体
     * @param <T>    业务响应体类型
     * @return RPC 响应信封
     */
    public static <T> EtcdRpcResponse<T> of(EtcdResponseHeader header, T body) {
        EtcdRpcResponse<T> response = new EtcdRpcResponse<T>();
        response.header = header == null ? EtcdResponseHeader.success() : header;
        response.body = body;
        return response;
    }

    /**
     * 当前响应是否需要客户端重试 Leader。
     *
     * <p>该方法委托给 header 判断。
     * 只有 notLeader=true 且 leaderId 非空时，客户端才具备自动跳转 Leader 的必要信息。</p>
     *
     * @return true 表示客户端可以根据 leaderId 重试 Leader
     */
    public boolean shouldRetryLeader() {
        return header != null && header.shouldRetryLeader();
    }

    /**
     * 获取当前已知 Leader 节点 ID。
     *
     * <p>该方法主要供 EtcdClient 在 notLeader 响应时查找 Leader endpoint 使用。</p>
     *
     * @return Leader 节点 ID；未知时返回 null
     */
    public String getLeaderId() {
        return header != null ? header.getLeaderId() : null;
    }
}