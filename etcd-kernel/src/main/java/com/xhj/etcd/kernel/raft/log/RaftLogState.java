package com.xhj.etcd.kernel.raft.log;

import java.util.ArrayList;
import java.util.List;

/**
 * RaftLogState
 *
 * @author XJks
 * @description Raft 日志状态，负责维护当前节点仍然保留的日志条目以及 snapshot 压缩后的日志边界。
 *
 * <p>TODO: 该类不是线程安全对象，只能由 RaftNode 的 raft-event-loop 线程串行访问。</p>
 *
 * <p>职责边界：</p>
 * <ul>
 *     <li>负责追加本地新日志和 Leader 复制过来的日志。</li>
 *     <li>负责根据 index/term 完成日志匹配和冲突截断。</li>
 *     <li>负责维护 snapshot 压缩后的 lastIncludedIndex / lastIncludedTerm 边界。</li>
 *     <li>不保存完整快照数据，完整快照内容由 RaftNode.latestSnapshot 持有。</li>
 * </ul>
 */
public class RaftLogState {

    /**
     * 当前节点仍然保留的 Raft 日志条目。
     *
     * <p>该列表只保存 snapshot 边界之后的日志，即 index 大于 lastIncludedIndex 的日志。
     * 经过 compact 后，lastIncludedIndex 及其之前的日志实体会被删除，只保留边界 index/term。</p>
     *
     * <p>TODO: entries 中第一个元素的实际 Raft log index 不一定是 1，
     * 而是 lastIncludedIndex + 1；因此所有 index 访问都必须通过 lastIncludedIndex 计算偏移。</p>
     */
    private final List<RaftLogEntry> entries = new ArrayList<>();

    /**
     * snapshot 已覆盖的最后一条日志 index。
     *
     * <p>compact 后，该 index 及其之前的真实 RaftLogEntry 已经从 entries 中删除。
     * 但 AppendEntries 仍可能使用该 index 作为 prevLogIndex 做日志匹配，
     * 因此需要单独保留这个边界值。</p>
     */
    private long lastIncludedIndex;

    /**
     * lastIncludedIndex 对应日志的 term。
     *
     * <p>Raft 日志匹配必须同时比较 index 和 term。
     * compact 后边界日志实体已经不存在，所以必须额外保留 lastIncludedTerm，
     * 用于后续 AppendEntries 的 prevLogIndex / prevLogTerm 校验。</p>
     */
    private long lastIncludedTerm;

    // ==================== 日志追加 ====================

    /**
     * 追加本节点新生成的日志条目。
     *
     * <p>该方法用于 Leader 处理本地 propose 时追加新日志。
     * 方法会防御性复制入参，并为内部日志副本分配下一个连续的 Raft log index。</p>
     *
     * @param entry 待追加的日志条目
     * @return 新日志条目分配到的 index
     */
    public long appendNewLocalLogEntry(RaftLogEntry entry) {
        long index = getLastLogIndex() + 1L;
        RaftLogEntry copiedEntry = copyLogEntry(entry);
        copiedEntry.setIndex(index);
        entries.add(copiedEntry);
        return index;
    }

    /**
     * 追加 Leader 复制过来的日志条目。
     *
     * <p>处理流程：</p>
     * <p>1) 从 prevLogIndex 后一个位置开始逐条比较；</p>
     * <p>2) 如果待追加位置已经被 snapshot 覆盖，则跳过该条复制日志；</p>
     * <p>3) 如果本地同 index 日志 term 相同，说明该位置已经一致，继续比较下一条；</p>
     * <p>4) 如果本地同 index 日志 term 不同，先截断本地冲突日志，再追加 Leader 日志。</p>
     *
     * @param prevLogIndex Leader 发送的前置日志 index
     * @param newEntries   Leader 复制过来的日志条目
     * @return 本次真正追加到本地的日志条目副本
     */
    public List<RaftLogEntry> appendLeaderReplicatedLogEntries(long prevLogIndex, List<RaftLogEntry> newEntries) {
        List<RaftLogEntry> appendedEntries = new ArrayList<>();
        if (newEntries == null || newEntries.isEmpty()) {
            return appendedEntries;
        }

        long nextIndex = prevLogIndex + 1L;
        for (RaftLogEntry newEntry : newEntries) {
            // 已经被 snapshot 覆盖的位置不再追加实体日志，只继续推进待比较 index。
            if (nextIndex <= lastIncludedIndex) {
                nextIndex++;
                continue;
            }

            if (nextIndex <= getLastLogIndex()) {
                RaftLogEntry oldEntry = getLogEntryByIndex(nextIndex);
                if (oldEntry != null && oldEntry.getTerm() == newEntry.getTerm()) {
                    nextIndex++;
                    continue;
                }

                // 同 index 但 term 不同，说明从该位置开始出现冲突，需要删除本地冲突后缀。
                truncateLogEntriesFromIndex(nextIndex);
            }

            RaftLogEntry entry = copyLogEntry(newEntry);
            entry.setIndex(nextIndex);
            entries.add(entry);
            appendedEntries.add(copyLogEntry(entry));
            nextIndex++;
        }

        return appendedEntries;
    }

    // ==================== 日志查询 ====================

    /**
     * 根据 index 获取日志条目。
     *
     * <p>只允许查询 snapshot 边界之后、且仍然保留在 entries 中的日志。
     * index 小于等于 lastIncludedIndex 时，说明日志实体已经被 snapshot 覆盖，返回 null。</p>
     *
     * @param index 日志 index
     * @return 日志条目副本；不存在时返回 null
     */
    public RaftLogEntry getLogEntryByIndex(long index) {
        if (index <= lastIncludedIndex || index > getLastLogIndex()) {
            return null;
        }
        int offset = (int) (index - lastIncludedIndex - 1L);
        return copyLogEntry(entries.get(offset));
    }

    /**
     * 根据 index 获取日志 term。
     *
     * <p>返回值语义：</p>
     * <ul>
     *     <li>index 为 0 时返回 0，表示空日志边界。</li>
     *     <li>index 等于 lastIncludedIndex 时返回 lastIncludedTerm。</li>
     *     <li>index 小于 snapshot 边界或不存在时返回 -1。</li>
     * </ul>
     *
     * @param index 日志 index
     * @return 日志 term；不存在时返回 -1
     */
    public long getLogTermByIndex(long index) {
        if (index == 0L) {
            return 0L;
        }
        if (index == lastIncludedIndex) {
            return lastIncludedTerm;
        }
        if (index < lastIncludedIndex) {
            return -1L;
        }
        RaftLogEntry entry = getLogEntryByIndex(index);
        if (entry == null) {
            return -1L;
        }
        return entry.getTerm();
    }

    /**
     * 获取最后一条日志的 index。
     *
     * <p>如果 entries 为空，最后日志 index 就是 snapshot 边界 index。</p>
     *
     * @return 当前日志最后位置
     */
    public long getLastLogIndex() {
        return lastIncludedIndex + entries.size();
    }

    /**
     * 获取最后一条日志的 term。
     *
     * <p>如果 entries 为空，最后日志 term 就是 snapshot 边界 term。</p>
     *
     * @return 当前日志最后位置对应的 term
     */
    public long getLastLogTerm() {
        if (entries.isEmpty()) {
            return lastIncludedTerm;
        }
        return entries.get(entries.size() - 1).getTerm();
    }

    /**
     * 获取第一个仍然可以通过 entries 查询到的日志 index。
     *
     * @return snapshot 边界之后的第一个日志 index
     */
    public long getFirstAvailableLogIndex() {
        return lastIncludedIndex + 1L;
    }

    /**
     * 获取 snapshot 边界 index。
     *
     * @return 已被 snapshot 覆盖的最后日志 index
     */
    public long getLastIncludedIndex() {
        return lastIncludedIndex;
    }

    /**
     * 获取 snapshot 边界 term。
     *
     * @return lastIncludedIndex 对应的 term
     */
    public long getLastIncludedTerm() {
        return lastIncludedTerm;
    }

    /**
     * 检查指定 index 和 term 是否与本地日志状态匹配。
     *
     * <p>该方法主要用于 AppendEntries 的 prevLogIndex / prevLogTerm 校验。
     * 如果 index 正好等于 lastIncludedIndex，则使用 snapshot 边界 term 进行匹配。</p>
     *
     * @param index 日志 index
     * @param term  日志 term
     * @return true 表示本地存在该日志位置且 term 匹配
     */
    public boolean hasLogPositionWithIndexAndTerm(long index, long term) {
        if (index == 0L) {
            return term == 0L;
        }
        if (index == lastIncludedIndex) {
            return term == lastIncludedTerm;
        }
        if (index < lastIncludedIndex) {
            return false;
        }
        RaftLogEntry entry = getLogEntryByIndex(index);
        return entry != null && entry.getTerm() == term;
    }

    /**
     * 从指定 index 开始获取所有仍然保留的日志条目。
     *
     * <p>如果 nextIndex 已经落在 snapshot 覆盖范围内，会自动调整到 lastIncludedIndex + 1，
     * 避免返回已经被 compact 删除的日志实体。</p>
     *
     * @param nextIndex 起始日志 index
     * @return 从 nextIndex 开始的日志条目副本列表
     */
    public List<RaftLogEntry> getLogEntriesFromIndex(long nextIndex) {
        List<RaftLogEntry> result = new ArrayList<>();
        if (nextIndex <= lastIncludedIndex) {
            nextIndex = lastIncludedIndex + 1L;
        }
        for (long index = nextIndex; index <= getLastLogIndex(); index++) {
            result.add(getLogEntryByIndex(index));
        }
        return result;
    }

    /**
     * 获取当前仍然保留的全部日志条目。
     *
     * @return snapshot 边界之后的全部日志条目副本
     */
    public List<RaftLogEntry> getAllLogEntries() {
        return getLogEntriesFromIndex(lastIncludedIndex + 1L);
    }

    // ==================== Snapshot / Compact ====================

    /**
     * 获取指定 index 作为 snapshot 边界时对应的日志 term。
     *
     * <p>创建 snapshot 前需要先根据目标 lastIncludedIndex 找到对应 term，
     * 后续 compact 时会把该 index/term 作为新的日志边界保存下来。</p>
     *
     * @param targetLastIncludedIndex 目标 snapshot 边界 index
     * @return 目标边界 index 对应的 term
     */
    public long getSnapshotBoundaryLogTerm(long targetLastIncludedIndex) {
        if (targetLastIncludedIndex <= lastIncludedIndex) {
            throw new IllegalArgumentException("snapshot index is already compacted: " + targetLastIncludedIndex);
        }
        if (targetLastIncludedIndex > getLastLogIndex()) {
            throw new IllegalArgumentException("snapshot index is greater than last log index: " + targetLastIncludedIndex);
        }

        long targetLastIncludedTerm = getLogTermByIndex(targetLastIncludedIndex);
        if (targetLastIncludedTerm < 0L) {
            throw new IllegalArgumentException("snapshot term not found, index=" + targetLastIncludedIndex);
        }
        return targetLastIncludedTerm;
    }

    /**
     * 将日志压缩到指定 snapshot 边界。
     *
     * <p>压缩后，targetLastIncludedIndex 及其之前的日志实体会从 entries 中删除，
     * 但 targetLastIncludedIndex / targetLastIncludedTerm 会作为新的日志匹配边界保留下来。</p>
     *
     * @param targetLastIncludedIndex snapshot 覆盖的最后日志 index
     * @param targetLastIncludedTerm  snapshot 覆盖的最后日志 term
     */
    public void compactLogEntriesToSnapshotBoundary(long targetLastIncludedIndex, long targetLastIncludedTerm) {
        if (targetLastIncludedIndex <= lastIncludedIndex) {
            return;
        }
        if (targetLastIncludedIndex > getLastLogIndex()) {
            throw new IllegalArgumentException(
                    "compact index is greater than last log index: " + targetLastIncludedIndex);
        }

        int removeCount = (int) Math.min(entries.size(), targetLastIncludedIndex - lastIncludedIndex);
        if (removeCount > 0) {
            entries.subList(0, removeCount).clear();
        }
        lastIncludedIndex = targetLastIncludedIndex;
        lastIncludedTerm = targetLastIncludedTerm;
    }

    /**
     * 根据 snapshot 边界恢复日志状态。
     *
     * <p>该方法通常用于安装 snapshot 或本地快照恢复。
     * 如果当前日志中仍然存在 snapshot 边界之后且与边界 term 连续匹配的日志，
     * 可以保留这些后续日志；否则清空所有日志，只保留新的 snapshot 边界。</p>
     *
     * @param targetLastIncludedIndex snapshot 覆盖的最后日志 index
     * @param targetLastIncludedTerm  snapshot 覆盖的最后日志 term
     */
    public void restoreLogStateBySnapshotBoundary(long targetLastIncludedIndex, long targetLastIncludedTerm) {
        if (targetLastIncludedIndex <= lastIncludedIndex) {
            return;
        }

        List<RaftLogEntry> remainingEntries = new ArrayList<>();
        boolean hasSnapshotBoundaryLog = targetLastIncludedIndex <= getLastLogIndex()
                && getLogTermByIndex(targetLastIncludedIndex) == targetLastIncludedTerm;
        if (hasSnapshotBoundaryLog) {
            for (long index = targetLastIncludedIndex + 1L; index <= getLastLogIndex(); index++) {
                remainingEntries.add(getLogEntryByIndex(index));
            }
        }

        entries.clear();
        for (RaftLogEntry entry : remainingEntries) {
            entries.add(copyLogEntry(entry));
        }
        lastIncludedIndex = targetLastIncludedIndex;
        lastIncludedTerm = targetLastIncludedTerm;
    }


    /**
     * 根据持久化日志恢复 snapshot 边界之后的日志条目。
     *
     * <p>该方法用于节点启动恢复。调用方应先通过 restoreLogStateBySnapshotBoundary 恢复 snapshot 边界，
     * 再把持久化的剩余日志交给该方法恢复到 entries 中。</p>
     *
     * <p>恢复要求：</p>
     * <ul>
     *     <li>只接收 index 大于 lastIncludedIndex 的日志。</li>
     *     <li>日志必须从 lastIncludedIndex + 1 开始连续排列。</li>
     *     <li>方法内部会复制日志条目，避免外部引用污染 RaftLogState。</li>
     * </ul>
     *
     * @param restoredEntries 持久化恢复出的日志条目
     */
    public void restoreLogEntriesAfterSnapshot(List<RaftLogEntry> restoredEntries) {
        entries.clear();
        if (restoredEntries == null || restoredEntries.isEmpty()) {
            return;
        }

        List<RaftLogEntry> sortedEntries = new ArrayList<>(restoredEntries);
        sortedEntries.sort((left, right) -> Long.compare(left.getIndex(), right.getIndex()));

        long expectedIndex = lastIncludedIndex + 1L;
        for (RaftLogEntry entry : sortedEntries) {
            if (entry == null || entry.getIndex() <= lastIncludedIndex) {
                continue;
            }
            if (entry.getIndex() != expectedIndex) {
                throw new IllegalStateException("restored raft log entries must be continuous, expected="
                        + expectedIndex + ", actual=" + entry.getIndex());
            }
            entries.add(copyLogEntry(entry));
            expectedIndex++;
        }
    }

    // ==================== 日志截断 ====================

    /**
     * 从指定 index 开始截断本地日志。
     *
     * <p>该方法用于处理 Leader 复制日志时发现的冲突后缀。
     * 如果截断位置已经落在 snapshot 边界内，则当前保留的日志全部失效，直接清空 entries。</p>
     *
     * @param index 起始截断 index
     */
    private void truncateLogEntriesFromIndex(long index) {
        if (index <= lastIncludedIndex) {
            entries.clear();
            return;
        }
        while (getLastLogIndex() >= index) {
            entries.remove(entries.size() - 1);
        }
    }

    /**
     * 复制日志条目。
     *
     * <p>返回副本可以避免调用方持有内部日志对象引用后修改 RaftLogState 的内部状态。</p>
     *
     * @param source 原始日志条目
     * @return 日志条目副本；source 为 null 时返回 null
     */
    private RaftLogEntry copyLogEntry(RaftLogEntry source) {
        if (source == null) {
            return null;
        }
        RaftLogEntry target = new RaftLogEntry();
        target.setIndex(source.getIndex());
        target.setTerm(source.getTerm());
        if (source.getCommandData() != null) {
            target.setCommandData(source.getCommandData().clone());
        }
        return target;
    }
}
