package com.xhj.etcd.kernel.etcd.command;

/**
 * EtcdCommandApplyResultType
 *
 * @author XJks
 * @description 命令应用结果类型枚举。
 */
public enum EtcdCommandApplyResultType {

    /**
     * 成功。
     */
    SUCCESS,

    /**
     * 非 Leader。
     */
    NOT_LEADER,

    /**
     * 超时。
     */
    TIMEOUT,

    /**
     * 冲突。
     */
    CONFLICT,

    /**
     * 错误。
     */
    ERROR
}