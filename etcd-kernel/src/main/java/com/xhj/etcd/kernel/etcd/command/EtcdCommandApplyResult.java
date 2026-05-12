package com.xhj.etcd.kernel.etcd.command;

import lombok.Data;

import java.io.Serializable;

/**
 * EtcdCommandApplyResult
 *
 * @author XJks
 * @description 命令应用结果骨架。
 *
 * <p>该对象只承载与命令类型无关的统一结果字段，以及可选的业务响应体。</p>
 */
@Data
public class EtcdCommandApplyResult implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * 命令执行结果类型。
     */
    private EtcdCommandApplyResultType type;

    /**
     * 命令唯一标识。
     */
    private String commandId;

    /**
     * 错误信息。
     */
    private String message;

    /**
     * 业务响应体。
     *
     * <p>仅在 SUCCESS 时可能有值，失败结果该字段必须为 null。</p>
     */
    private Object body;

    /**
     * 是否成功。
     */
    public boolean isSuccess() {
        return type == EtcdCommandApplyResultType.SUCCESS;
    }

    /**
     * 是否失败。
     */
    public boolean isFailed() {
        return !isSuccess();
    }

    /**
     * 读取业务响应体并按指定类型转换。
     *
     * <p>转换规则：</p>
     * <ol>
     *     <li>失败结果直接返回 null，不执行类型转换。</li>
     *     <li>body 为空时返回 null。</li>
     *     <li>body 类型与期望类型不一致时抛出 IllegalStateException。</li>
     * </ol>
     *
     * @param expectedType 期望响应体类型
     * @param <T>          响应体类型
     * @return 转换后的响应体
     */
    public <T> T getBodyAs(Class<T> expectedType) {
        if (isFailed() || body == null) {
            return null;
        }
        if (!expectedType.isInstance(body)) {
            throw new IllegalStateException("response body type mismatch, expected=" + expectedType.getName() + ", actual=" + body.getClass().getName());
        }
        return expectedType.cast(body);
    }

    // ==================== 成功结果 ====================

    /**
     * 创建成功结果。
     *
     * @param commandId 命令 ID
     * @param body      业务响应体
     * @return 应用结果
     */
    public static EtcdCommandApplyResult success(String commandId, Object body) {
        EtcdCommandApplyResult result = new EtcdCommandApplyResult();
        result.type = EtcdCommandApplyResultType.SUCCESS;
        result.commandId = commandId;
        result.body = body;
        return result;
    }

    // ==================== 异常结果 ====================

    /**
     * 创建非 Leader 结果。
     *
     * @param leaderId leader 节点 ID
     * @return 应用结果
     */
    public static EtcdCommandApplyResult notLeader(String leaderId) {
        EtcdCommandApplyResult result = new EtcdCommandApplyResult();
        result.type = EtcdCommandApplyResultType.NOT_LEADER;
        result.message = leaderId;
        result.body = null;
        return result;
    }

    /**
     * 创建超时结果。
     *
     * @param commandId 命令 ID
     * @return 应用结果
     */
    public static EtcdCommandApplyResult timeout(String commandId) {
        EtcdCommandApplyResult result = new EtcdCommandApplyResult();
        result.type = EtcdCommandApplyResultType.TIMEOUT;
        result.commandId = commandId;
        result.message = "wait command apply timeout";
        result.body = null;
        return result;
    }

    /**
     * 创建冲突结果。
     *
     * @param commandId 命令 ID
     * @return 应用结果
     */
    public static EtcdCommandApplyResult conflict(String commandId) {
        EtcdCommandApplyResult result = new EtcdCommandApplyResult();
        result.type = EtcdCommandApplyResultType.CONFLICT;
        result.commandId = commandId;
        result.message = "command conflict at same logIndex";
        result.body = null;
        return result;
    }

    /**
     * 创建错误结果。
     *
     * @param commandId 命令 ID
     * @param message   错误消息
     * @return 应用结果
     */
    public static EtcdCommandApplyResult error(String commandId, String message) {
        EtcdCommandApplyResult result = new EtcdCommandApplyResult();
        result.type = EtcdCommandApplyResultType.ERROR;
        result.commandId = commandId;
        result.message = message;
        result.body = null;
        return result;
    }
}