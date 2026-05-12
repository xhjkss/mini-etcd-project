package com.xhj.etcd.kernel.etcd.command;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * EtcdCommandRegistry
 *
 * @author XJks
 * @description Etcd 命令等待注册表，用于关联客户端请求、Raft propose 结果和最终 apply 结果。
 *
 * <p>TODO 高亮：该类是 EtcdNode 中 propose 阶段和 apply 阶段之间的桥接对象。
 * 客户端请求进入 EtcdNode 后会先生成 commandId 并注册等待 future；
 * RaftNode 接收 propose 后才会返回 logIndex；
 * 日志 committed 并 apply 后，再根据 logIndex + commandId 找回等待中的请求并完成 future。</p>
 *
 * <p>为什么不能只用 logIndex：</p>
 * <ul>
 *     <li>Leader 本地 append 的日志不一定最终 committed。</li>
 *     <li>旧 Leader 失去领导权后，同一个 logIndex 可能被新 Leader 写入不同命令。</li>
 *     <li>如果只按 logIndex 唤醒等待请求，可能把旧请求错误唤醒为新命令的 apply 结果。</li>
 *     <li>因此 apply 阶段必须同时校验 commandId，发现不一致时返回 conflict。</li>
 * </ul>
 *
 * <p>为什么还需要 commandId 映射：</p>
 * <ul>
 *     <li>命令刚提交时还没有 logIndex，只能先按 commandId 注册。</li>
 *     <li>如果 propose 超时、失败或被中断，需要按 commandId 清理等待项。</li>
 *     <li>propose 成功拿到 logIndex 后，再把 commandId 等待项绑定到 logIndex。</li>
 * </ul>
 */
public class EtcdCommandRegistry {

    /**
     * Raft 日志索引到等待命令的映射。
     *
     * <p>该映射在 Leader propose 成功、拿到 logIndex 后建立。
     * apply 阶段优先根据 logIndex 查找等待命令，因为 RaftApplyMessage 会携带已提交日志的 logIndex。</p>
     */
    private final Map<Long, PendingCommand> pendingCommandByLogIndexMap = new ConcurrentHashMap<>();

    /**
     * 命令 ID 到等待命令的映射。
     *
     * <p>该映射在客户端请求刚进入 EtcdNode 时建立。
     * 此时命令还没有被 RaftNode 接收，因此还不知道最终 logIndex，只能先通过 commandId 追踪。</p>
     */
    private final Map<String, PendingCommand> pendingCommandByCommandIdMap = new ConcurrentHashMap<>();

    /**
     * 注册等待中的命令。
     *
     * <p>调用时机：EtcdNode 构造 EtcdCommand 后、提交 Raft propose 前。</p>
     *
     * <p>处理流程：</p>
     * <p>1) 为当前 commandId 创建 CompletableFuture；</p>
     * <p>2) 构造 PendingCommand 保存 commandId 和 future；</p>
     * <p>3) 先放入 commandId 映射，等待后续 propose 成功后再绑定 logIndex。</p>
     *
     * @param commandId 命令 ID，由 EtcdNode 内部生成
     * @return 等待 apply 结果的 Future
     */
    public CompletableFuture<EtcdCommandApplyResult> register(String commandId) {
        CompletableFuture<EtcdCommandApplyResult> future = new CompletableFuture<>();

        PendingCommand pendingCommand = new PendingCommand();
        pendingCommand.commandId = commandId;
        pendingCommand.future = future;

        // 先按 commandId 注册，因为此时命令还没有被 RaftNode 分配 logIndex。
        pendingCommandByCommandIdMap.put(commandId, pendingCommand);
        return future;
    }

    /**
     * 注册等待中的命令，并立即绑定 Raft 日志索引。
     *
     * <p>该方法适用于调用方已经同时知道 commandId 和 logIndex 的场景。
     * 当前 EtcdNode 主流程一般会先 register(commandId)，再在 propose 成功后调用 bindLogIndex。</p>
     *
     * @param logIndex  Raft 日志索引
     * @param commandId 命令 ID
     * @return 等待 apply 结果的 Future
     */
    public CompletableFuture<EtcdCommandApplyResult> register(long logIndex, String commandId) {
        CompletableFuture<EtcdCommandApplyResult> future = register(commandId);
        bindLogIndex(logIndex, commandId);
        return future;
    }

    /**
     * 绑定 Raft 日志索引。
     *
     * <p>调用时机：RaftNode 接收 propose 并返回 accepted 后。
     * 此时 EtcdNode 才知道该命令被 Leader 追加到了哪个 logIndex。</p>
     *
     * <p>为什么绑定 logIndex：</p>
     * <p>Raft apply 阶段回来的 RaftApplyMessage 会携带 logIndex。
     * 绑定后，apply 线程可以优先通过 logIndex 找到等待中的请求，再校验 commandId 是否匹配。</p>
     *
     * @param logIndex  Raft 日志索引
     * @param commandId 命令 ID
     */
    public void bindLogIndex(long logIndex, String commandId) {
        PendingCommand pendingCommand = pendingCommandByCommandIdMap.get(commandId);
        if (pendingCommand == null) {
            return;
        }

        pendingCommand.logIndex = logIndex;

        // 建立 logIndex -> pendingCommand 映射，供 apply 阶段根据 RaftApplyMessage.logIndex 快速查找。
        pendingCommandByLogIndexMap.put(logIndex, pendingCommand);
    }

    /**
     * 完成已 apply 的命令。
     *
     * <p>调用时机：EtcdNode apply RaftApplyMessage 后。
     * apply 阶段会从日志中反序列化出 EtcdCommand，因此同时可以拿到 logIndex 和 commandId。</p>
     *
     * <p>处理流程：</p>
     * <p>1) 优先按 logIndex 找等待项，因为 apply 消息天然携带 logIndex；</p>
     * <p>2) 如果 logIndex 没找到，再按 commandId 尝试兜底查找；</p>
     * <p>3) 找到等待项后，从两张映射表中都移除，避免重复完成或内存泄漏；</p>
     * <p>4) 如果等待项 commandId 与 apply 出来的 commandId 不一致，说明该 logIndex 已被其他命令占用，旧请求返回 conflict；</p>
     * <p>5) 如果一致，则用真正的 apply 结果完成 future。</p>
     *
     * @param logIndex  已 apply 的 Raft 日志索引
     * @param commandId 从已 apply 日志中反序列化出的命令 ID
     * @param result    状态机应用结果
     */
    public void complete(long logIndex, String commandId, EtcdCommandApplyResult result) {
        // 1) 优先按 logIndex 找等待项，处理正常路径：propose accepted -> bind logIndex -> committed -> apply。
        PendingCommand pendingCommand = pendingCommandByLogIndexMap.remove(logIndex);

        if (pendingCommand == null) {
            // 2) 如果 logIndex 没找到，尝试按 commandId 兜底。
            //    这可以覆盖部分未成功绑定 logIndex 但 apply 阶段命令已经出现的情况。
            pendingCommand = pendingCommandByCommandIdMap.remove(commandId);
        } else {
            // 3) logIndex 命中后，也要清理 commandId 映射，避免同一个等待项残留。
            pendingCommandByCommandIdMap.remove(pendingCommand.commandId);
        }

        if (pendingCommand == null) {
            return;
        }

        // 4) commandId 不一致表示同一个 logIndex 对应的命令不是当前等待的命令。
        //    典型场景：旧 Leader 追加但未提交的日志被新 Leader 覆盖，旧请求不能被错误唤醒为成功。
        if (!pendingCommand.commandId.equals(commandId)) {
            pendingCommand.future.complete(EtcdCommandApplyResult.conflict(commandId));
            return;
        }

        // 5) logIndex 和 commandId 都匹配，说明当前 apply 结果属于该等待请求。
        pendingCommand.future.complete(result);
    }

    /**
     * 按 Raft 日志索引清理等待中的命令。
     *
     * <p>调用场景：命令已经绑定 logIndex，但等待 apply 超时、被中断或发生异常。
     * 清理时需要同时删除 commandId 映射，避免残留 future。</p>
     *
     * @param logIndex Raft 日志索引
     */
    public void remove(long logIndex) {
        PendingCommand pendingCommand = pendingCommandByLogIndexMap.remove(logIndex);
        if (pendingCommand != null) {
            pendingCommandByCommandIdMap.remove(pendingCommand.commandId);
        }
    }

    /**
     * 按命令 ID 清理等待中的命令。
     *
     * <p>调用场景：命令还没有拿到 logIndex 时，propose 超时、失败或被中断。
     * 如果该命令已经绑定过 logIndex，也要同步删除 logIndex 映射。</p>
     *
     * @param commandId 命令 ID
     */
    public void remove(String commandId) {
        PendingCommand pendingCommand = pendingCommandByCommandIdMap.remove(commandId);
        if (pendingCommand != null && pendingCommand.logIndex != null) {
            pendingCommandByLogIndexMap.remove(pendingCommand.logIndex);
        }
    }

    /**
     * 等待中的命令。
     *
     * <p>该对象保存一次客户端请求在 EtcdNode 内部的等待状态：
     * commandId 在请求创建时就已确定；logIndex 在 Raft propose accepted 后才会绑定；
     * future 在 apply 阶段完成。</p>
     */
    private static class PendingCommand {

        /**
         * Raft 日志索引。
         *
         * <p>命令刚注册时为空，Raft propose accepted 后才会填充。</p>
         */
        private Long logIndex;

        /**
         * 命令 ID。
         *
         * <p>由 EtcdNode 内部生成，用于避免同一个 logIndex 被不同 Leader 重写时错误唤醒旧请求。</p>
         */
        private String commandId;

        /**
         * 等待 apply 结果的 Future。
         */
        private CompletableFuture<EtcdCommandApplyResult> future;
    }
}