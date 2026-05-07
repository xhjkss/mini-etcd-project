package com.xhj.etcd.kernel.raft.core;

import com.xhj.etcd.kernel.raft.apply.RaftApplyMessage;
import com.xhj.etcd.kernel.raft.event.RaftCreateSnapshotEventData;
import com.xhj.etcd.kernel.raft.event.RaftEvent;
import com.xhj.etcd.kernel.raft.event.RaftEventCodec;
import com.xhj.etcd.kernel.raft.event.RaftEventType;
import com.xhj.etcd.kernel.raft.log.RaftLogEntry;
import com.xhj.etcd.kernel.raft.log.RaftLogState;
import com.xhj.etcd.kernel.raft.raftrpc.AppendEntriesRequest;
import com.xhj.etcd.kernel.raft.raftrpc.AppendEntriesResponse;
import com.xhj.etcd.kernel.raft.raftrpc.InstallSnapshotRequest;
import com.xhj.etcd.kernel.raft.raftrpc.InstallSnapshotResponse;
import com.xhj.etcd.kernel.raft.raftrpc.RaftRpcMessage;
import com.xhj.etcd.kernel.raft.raftrpc.RaftRpcMessageType;
import com.xhj.etcd.kernel.raft.raftrpc.RequestVoteRequest;
import com.xhj.etcd.kernel.raft.raftrpc.RequestVoteResponse;
import com.xhj.etcd.kernel.raft.snapshot.RaftSnapshot;
import com.xhj.etcd.serializer.Serializer;
import com.xhj.etcd.serializer.SerializerRegistry;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * RaftNode
 *
 * @author XJks
 * @description Raft 共识核心节点，负责维护节点角色、任期、选举、日志复制、提交推进、快照安装和 Ready 输出。
 *
 * <p>职责边界：</p>
 * <ul>
 *     <li>RaftNode 负责推进 Raft 协议状态：选举、投票、日志复制、commitIndex 推进、snapshot 边界处理。</li>
 *     <li>RaftNode 不直接落盘、不直接发网络、不直接 apply 状态机。</li>
 *     <li>需要上层执行的副作用会被封装成 RaftReady，由 EtcdNode 统一处理。</li>
 *     <li>EtcdNode 处理完成当前 RaftReady 后，必须通过 Advance 事件通知 RaftNode 清理 pending 状态。</li>
 * </ul>
 *
 * <p>Raft 内核与 Etcd 层交互主流程：</p>
 * <ul>
 *     <li>写请求入口：EtcdNode 将上层命令序列化为 commandData，并调用 submitRaftProposeEvent。</li>
 *     <li>RPC 入口：EtcdNode 收到远端 Raft RPC 后，调用 submitRequestVote / submitAppendEntries / submitInstallSnapshot 系列方法。</li>
 *     <li>Ready 输出：RaftNode 处理事件后，把持久化、发送、apply 等副作用放入 RaftReady。</li>
 *     <li>Ready 处理：EtcdNode 持久化 HardState / Log / Snapshot，发送 RaftRpcMessage，apply committed entries。</li>
 *     <li>Advance 回执：EtcdNode 处理完 Ready 后调用 submitRaftAdvanceEvent，RaftNode 再清理对应 pending 状态。</li>
 * </ul>
 *
 * <p>TODO: RaftNode 是 Raft 层唯一修改核心状态的入口。
 * EtcdNode、RPC 线程、测试辅助线程都不应该直接并发修改 role、term、log、commitIndex 等状态；
 * 外部输入必须先进入 raftEventQueue，再由 raft-event-loop 串行执行 propose、step、tick、advance 和 createSnapshot。</p>
 */
public class RaftNode {

    // ==================== 基础状态 ====================

    /**
     * 当前 Raft 节点 ID。
     */
    private final String nodeId;

    /**
     * Raft 节点配置。
     */
    private final RaftConfig config;

    /**
     * Raft 日志状态，维护仍保留的日志条目和 snapshot 压缩后的日志边界。
     */
    private final RaftLogState raftLogState;

    /**
     * 序列化器，用于编码 Raft 事件数据和 Raft RPC 消息体。
     */
    private final Serializer serializer;

    /**
     * Raft 事件编解码器。
     */
    private final RaftEventCodec raftEventCodec;

    /**
     * 当前节点角色。
     */
    private RaftRoleType role = RaftRoleType.FOLLOWER;

    /**
     * 当前任期编号。
     */
    private long currentTerm;

    /**
     * 当前任期内已投票的 Candidate ID。
     */
    private String votedFor;

    /**
     * 当前已知 Leader ID。
     */
    private String leaderId;

    /**
     * 已提交日志 index。
     */
    private long commitIndex;

    /**
     * 已 apply 到状态机的最高日志 index。
     */
    private long lastApplied;

    /**
     * 当前节点持有的最新快照。
     *
     * <p>TODO: latestSnapshot 是 RaftNode 的节点级状态，不属于 RaftLogState。
     * RaftLogState 只维护日志序列和 lastIncludedIndex / lastIncludedTerm 边界；
     * 完整快照数据由 latestSnapshot 保存，并通过 RaftReady.snapshotToPersist /
     * RaftReady.snapshotToApply 交给 EtcdNode 持久化或恢复状态机。</p>
     */
    private RaftSnapshot latestSnapshot;

    // ==================== Leader 复制状态 ====================

    /**
     * Leader 记录的 Follower 下一次复制起始 index。
     */
    private final Map<String, Long> nextIndexMap = new HashMap<>();

    /**
     * Leader 记录的 Follower 已匹配日志 index。
     */
    private final Map<String, Long> matchIndexMap = new HashMap<>();

    // ==================== Candidate 投票状态 ====================

    /**
     * 当前选举轮次中已经同意投票的节点集合。
     */
    private final Set<String> grantedVoteNodeIds = new HashSet<>();

    /**
     * 当前选举轮次中已经拒绝投票的节点集合。
     */
    private final Set<String> rejectedVoteNodeIds = new HashSet<>();

    // ==================== Tick 状态 ====================

    /**
     * 距离上次收到 Leader 消息或发起选举已经经过的 tick 数。
     */
    private int electionElapsed;

    /**
     * Leader 距离上次发送心跳已经经过的 tick 数。
     */
    private int heartbeatElapsed;

    /**
     * 当前节点实际使用的选举超时 tick 数。
     */
    private final int electionTimeoutTicks;

    // ==================== RaftReady 暂存 ====================

    /**
     * 等待 EtcdNode 持久化的日志条目。
     *
     * <p>这些日志已经进入 RaftLogState，但还没有被上层持久化。
     * Leader 本地 propose 生成的日志、Follower 接收 Leader 复制过来的日志，都会先进入该列表。</p>
     *
     * <p>交互流程：RaftNode 写入 pendingEntries -> ready().entriesToPersist ->
     * EtcdNode 持久化日志 -> submitRaftAdvanceEvent -> advance 清理对应条目。</p>
     */
    private final List<RaftLogEntry> pendingEntries = new ArrayList<>();

    /**
     * 等待 EtcdNode apply 的 committed 日志消息。
     *
     * <p>这些消息会通过 RaftReady.committedEntries 暴露给 EtcdNode。
     * EtcdNode 需要按 index 顺序将 commandData 反序列化为上层命令，并 apply 到状态机。</p>
     */
    private final List<RaftApplyMessage> pendingCommittedEntries = new ArrayList<>();

    /**
     * 自上次快照边界以来新增的 committed 日志条目数。
     */
    private long committedLogCountSinceSnapshot;

    /**
     * 当前是否等待上层创建并回传快照。
     */
    private final AtomicBoolean snapshotCreateRequestedPending = new AtomicBoolean(false);

    /**
     * 等待 EtcdNode 发送的 Raft RPC 消息。
     *
     * <p>RaftNode 只负责决定“发什么消息、发给哪个 Raft 节点”；
     * EtcdNode 负责根据 targetNodeId 找到目标节点地址，并通过 RPC 客户端实际发送。</p>
     */
    private final List<RaftRpcMessage> pendingRaftRpcMessages = new ArrayList<>();

    /**
     * 等待 EtcdNode 持久化的 HardState。
     */
    private RaftHardState pendingHardStateToPersist;

    /**
     * 等待 EtcdNode 持久化的 Snapshot。
     */
    private RaftSnapshot pendingSnapshotToPersist;

    /**
     * 等待 EtcdNode apply 到状态机的 Snapshot。
     */
    private RaftSnapshot pendingSnapshotToApply;

    // ==================== Raft EventLoop ====================

    /**
     * Raft 内部事件队列。
     *
     * <p>TODO: 该队列用于把 EtcdNode、RPC 入口和测试辅助线程提交的外部输入串行化。
     * 所有会改变 Raft 状态的输入都先进入该队列，再由 raft-event-loop 线程统一消费，
     * 避免多个线程直接并发修改 role、term、log、commitIndex 等核心状态。</p>
     */
    private final BlockingQueue<RaftEvent> raftEventQueue = new LinkedBlockingQueue<>();

    /**
     * 等待 Raft 事件循环返回结果的 propose future 映射。
     */
    private final Map<String, CompletableFuture<RaftProposeResult>> pendingRaftProposeFutureMap = new ConcurrentHashMap<>();

    /**
     * Raft 事件序号生成器。
     */
    private final AtomicLong raftEventSequence = new AtomicLong(0L);

    /**
     * RaftReady 输出队列。
     *
     * <p>该队列是 Raft 内核向 Etcd 层输出副作用的唯一通道。
     * RaftNode 不直接写磁盘、不直接发 RPC、不直接操作状态机，而是把这些动作封装到 RaftReady。</p>
     *
     * <p>TODO: EtcdNode 应传入空的、容量为 1 的单槽队列，例如 new ArrayBlockingQueue&lt;RaftReady(1)。
     * 队列只负责跨线程交接 Ready；Ready 是否已经被处理完成，仍由 waitingReadyAdvance 控制。</p>
     */
    private BlockingQueue<RaftReady> raftReadyEventQueue;

    /**
     * 当前是否存在一个已经发布、但尚未被 EtcdNode Advance 的 RaftReady。
     *
     * <p>该字段只服务于运行期 Ready 背压控制，不属于 Raft 持久化状态。
     * RaftNode 发布 Ready 成功后置为 true；收到 ADVANCE 事件并清理 pending 后置为 false。</p>
     *
     * <p>注意：即使 EtcdNode 已经从队列中取走 Ready，只要还没有提交 Advance，
     * 该字段仍然保持 true，RaftNode 也不会发布下一轮 Ready。</p>
     */
    private boolean waitingReadyAdvance;

    /**
     * Raft tick 间隔，单位毫秒。
     */
    private final long raftTickIntervalMillis = 100L;

    /**
     * Raft 事件队列轮询间隔，单位毫秒。
     */
    private final long raftEventPollMillis = 20L;

    /**
     * raft-event-loop 是否正在运行。
     */
    private volatile boolean raftEventLoopRunning;

    /**
     * Raft 事件循环线程。
     */
    private Thread raftEventLoopThread;

    public RaftNode(String nodeId, RaftConfig config, RaftLogState raftLogState) {
        this(nodeId, config, raftLogState, SerializerRegistry.getDefaultSerializer());
    }

    public RaftNode(String nodeId, RaftConfig config, RaftLogState raftLogState, Serializer serializer) {
        if (nodeId == null || nodeId.trim().length() == 0) {
            throw new IllegalArgumentException("nodeId must not be empty");
        }
        if (config == null) {
            throw new IllegalArgumentException("config must not be null");
        }
        if (raftLogState == null) {
            throw new IllegalArgumentException("raftLogState must not be null");
        }
        if (serializer == null) {
            throw new IllegalArgumentException("serializer must not be null");
        }

        this.nodeId = nodeId;
        this.config = config;
        this.raftLogState = raftLogState;
        this.serializer = serializer;
        this.raftEventCodec = new RaftEventCodec();
        this.electionTimeoutTicks = buildElectionTimeoutTicks(nodeId, config);
    }

    // ==================== 测试辅助 ====================

    /**
     * 测试用：强制该节点成为 Leader。
     */
    public void becomeLeaderForTest() {
        currentTerm = Math.max(currentTerm, 1L);
        becomeLeader();
    }

    /**
     * 恢复 HardState。
     */
    public void restoreHardState(RaftHardState hardState) {
        if (hardState == null) {
            return;
        }
        currentTerm = Math.max(0L, hardState.getCurrentTerm());
        votedFor = hardState.getVotedFor();
        pendingHardStateToPersist = null;
    }

    /**
     * 启动恢复入口：根据已持久化快照恢复 Raft 运行边界。
     */
    public void restoreFromSnapshot(RaftSnapshot snapshot) {
        if (snapshot == null) {
            return;
        }

        raftLogState.restoreLogStateBySnapshotBoundary(snapshot.getLastIncludedIndex(), snapshot.getLastIncludedTerm());
        latestSnapshot = copySnapshot(snapshot);

        commitIndex = Math.max(commitIndex, snapshot.getLastIncludedIndex());
        lastApplied = Math.max(lastApplied, snapshot.getLastIncludedIndex());

        pendingEntries.clear();
        pendingCommittedEntries.clear();
        pendingSnapshotToPersist = null;
        pendingSnapshotToApply = null;

        committedLogCountSinceSnapshot = Math.max(0L, commitIndex - snapshot.getLastIncludedIndex());
        snapshotCreateRequestedPending.set(false);
    }


    /**
     * 启动恢复入口：恢复 snapshot 边界之后仍保留的日志条目。
     *
     * <p>该方法只用于节点启动恢复。EtcdNode 会先恢复 HardState 和 Snapshot，
     * 再把持久化的剩余日志交回 RaftNode，使 RaftLogState 可以从完整的本地日志边界继续工作。</p>
     *
     * @param entries snapshot 边界之后仍保留的日志条目
     */
    public void restoreLogEntries(List<RaftLogEntry> entries) {
        raftLogState.restoreLogEntriesAfterSnapshot(entries);
        pendingEntries.clear();
        pendingCommittedEntries.clear();
    }

    /**
     * 测试用：直接创建快照。
     */
    public void createSnapshotForTest(long lastIncludedIndex, byte[] stateMachineData) {
        RaftCreateSnapshotEventData eventData = new RaftCreateSnapshotEventData();
        eventData.setLastIncludedIndex(lastIncludedIndex);
        if (stateMachineData != null) {
            eventData.setStateMachineData(stateMachineData.clone());
        }
        createSnapshot(eventData);
    }

    // ==================== Raft EventLoop 生命周期 ====================

    /**
     * 启动 Raft 事件循环。
     *
     * <p>该方法由 EtcdNode 在节点启动时调用，并传入 RaftReady 输出队列。
     * 启动后，RaftNode 会通过 raft-event-loop 串行消费 RaftEvent，并在产生副作用时向该队列投递 RaftReady。</p>
     *
     * <p>交互关系：</p>
     * <ul>
     *     <li>EtcdNode -> RaftNode：通过 submitRaftProposeEvent / submitXXXEvent 投递输入。</li>
     *     <li>RaftNode -> EtcdNode：通过 raftReadyEventQueue 输出 RaftReady。</li>
     *     <li>EtcdNode -> RaftNode：通过 submitRaftAdvanceEvent 确认 Ready 已处理完成。</li>
     * </ul>
     *
     * <p>队列约束：</p>
     * <p>该队列应为空的、容量为 1 的单槽队列，推荐由 EtcdNode 创建 new ArrayBlockingQueue&lt;RaftReady&gt;(1)。
     * 单槽队列表达“最多只有一个 Ready 等待被 EtcdNode 接收”的语义；
     * waitingReadyAdvance 进一步表达“Ready 已经交给 EtcdNode，但还没有处理完成”的语义。</p>
     *
     * @param raftReadyEventQueue RaftReady 输出队列
     */
    public void startRaftEventLoop(BlockingQueue<RaftReady> raftReadyEventQueue) {
        if (raftReadyEventQueue == null) {
            throw new IllegalArgumentException("raftReadyEventQueue must not be null");
        }
        if (!raftReadyEventQueue.isEmpty() || raftReadyEventQueue.remainingCapacity() != 1) {
            throw new IllegalArgumentException("raftReadyEventQueue must be empty and capacity must be 1");
        }
        if (raftEventLoopRunning) {
            return;
        }

        this.raftReadyEventQueue = raftReadyEventQueue;
        this.waitingReadyAdvance = false;
        this.raftEventLoopRunning = true;

        raftEventLoopThread = new Thread(new Runnable() {
            @Override
            public void run() {
                runRaftEventLoop();
            }
        }, "raft-event-loop-" + nodeId);
        raftEventLoopThread.setDaemon(true);
        raftEventLoopThread.start();
    }

    /**
     * 停止 Raft 事件循环。
     */
    public void stopRaftEventLoop() {
        raftEventLoopRunning = false;
        if (raftEventLoopThread != null) {
            raftEventLoopThread.interrupt();
        }
    }

    /**
     * Raft 事件循环主流程。
     *
     * <p>该循环负责串行处理 Raft 的所有输入事件，并周期性触发 tick。
     * 事件处理过程中产生的持久化、RPC 发送、状态机 apply 等副作用，不会在 RaftNode 内部直接执行，
     * 而是统一通过 RaftReady 投递给上层 EtcdNode。</p>
     *
     * <p>Ready / Advance 生命周期：</p>
     * <p>1) RaftNode 处理 propose、RPC、tick、snapshot 等事件后，可能产生 pending 状态；</p>
     * <p>2) publishRaftReadyIfNeeded 检查当前没有等待 Advance 时，发布一个 RaftReady；</p>
     * <p>3) 发布 Ready 后，waitingReadyAdvance 置为 true，表示正在等待上层确认；</p>
     * <p>4) EtcdNode 处理完 Ready 中的持久化、消息发送和状态机 apply 后，提交 ADVANCE 事件；</p>
     * <p>5) RaftNode 收到 ADVANCE 后调用 advance 清理 pending，并将 waitingReadyAdvance 置为 false；</p>
     * <p>6) 后续循环如果仍有 pending 内容，才允许发布下一轮 Ready。</p>
     */
    private void runRaftEventLoop() {
        long nextTickTime = System.currentTimeMillis() + raftTickIntervalMillis;

        while (raftEventLoopRunning) {
            try {
                long waitMillis = Math.max(1L, nextTickTime - System.currentTimeMillis());
                RaftEvent event = raftEventQueue.poll(Math.min(waitMillis, raftEventPollMillis), TimeUnit.MILLISECONDS);
                if (event != null) {
                    // 1) 普通事件可能产生 pending；ADVANCE 事件会解除 Ready 等待状态。
                    processRaftEvent(event);
                }

                long now = System.currentTimeMillis();
                if (now >= nextTickTime) {
                    // 2) tick 也是 Raft 输入，可能触发选举、心跳或日志复制消息。
                    tick();
                    nextTickTime = now + raftTickIntervalMillis;
                }

                // 3) 循环末尾统一尝试发布 Ready，避免事件处理中直接穿插上层副作用。
                publishRaftReadyIfNeeded();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (Throwable ignore) {
                // 学习版 Raft EventLoop 不因为单次事件失败而退出。
            }
        }
    }

    /**
     * 处理单个 Raft 事件。
     *
     * <p>事件来源和处理链路：</p>
     * <ul>
     *     <li>PROPOSE：来自 EtcdNode 上层写请求，最终调用 propose 追加日志。</li>
     *     <li>REQUEST_VOTE / APPEND_ENTRIES / INSTALL_SNAPSHOT：来自远端 Raft 节点，经 EtcdNode RPC 层转入本地 RaftNode。</li>
     *     <li>CREATE_SNAPSHOT：来自 EtcdNode 状态机快照创建结果，用于压缩 RaftLogState。</li>
     *     <li>ADVANCE：来自 EtcdNode 对上一个 Ready 的处理完成通知，用于清理 pending 状态。</li>
     * </ul>
     *
     * @param event Raft 事件
     */
    private void processRaftEvent(RaftEvent event) {
        RaftEventType type = event.getType();
        switch (type) {
            case PROPOSE:
                completeRaftProposeEvent(event, raftEventCodec.decodeProposeCommandData(event));
                return;
            case REQUEST_VOTE:
                step(raftEventCodec.decodeRequestVoteRequest(event));
                return;
            case REQUEST_VOTE_RESPONSE:
                step(raftEventCodec.decodeRequestVoteResponse(event));
                return;
            case APPEND_ENTRIES:
                step(raftEventCodec.decodeAppendEntriesRequest(event));
                return;
            case APPEND_ENTRIES_RESPONSE:
                step(raftEventCodec.decodeAppendEntriesResponse(event));
                return;
            case INSTALL_SNAPSHOT:
                step(raftEventCodec.decodeInstallSnapshotRequest(event));
                return;
            case INSTALL_SNAPSHOT_RESPONSE:
                step(raftEventCodec.decodeInstallSnapshotResponse(event));
                return;
            case CREATE_SNAPSHOT:
                createSnapshot(raftEventCodec.decodeRaftCreateSnapshotEventData(event));
                return;
            case ADVANCE:
                handleAdvanceEvent(event);
                return;
            default:
                return;
        }
    }

    /**
     * 处理 Ready 推进事件。
     *
     * <p>ADVANCE 表示 EtcdNode 已经处理完上一轮 Ready。
     * RaftNode 收到后先清理 Ready 对应的 pending 状态，再解除 waitingReadyAdvance，
     * 允许后续循环发布下一轮 Ready。</p>
     *
     * @param event Advance 事件
     */
    private void handleAdvanceEvent(RaftEvent event) {
        RaftReady ready = raftEventCodec.decodeRaftReady(event);
        advance(ready);
        waitingReadyAdvance = false;
    }

    /**
     * 完成 Propose 事件。
     *
     * <p>该方法只是完成 propose 请求本身的受理结果：
     * 如果当前节点是 Leader，accepted 表示日志已经被 Leader 接收并追加到本地；
     * 该日志真正 committed 并 apply 到状态机，还需要等待后续 RaftReady / Advance 流程。</p>
     *
     * @param event       Raft 事件
     * @param commandData 上层命令字节
     */
    private void completeRaftProposeEvent(RaftEvent event, byte[] commandData) {
        CompletableFuture<RaftProposeResult> proposeFuture = pendingRaftProposeFutureMap.remove(event.getEventId());
        if (proposeFuture == null) {
            return;
        }

        try {
            RaftProposeResult proposeResult = propose(commandData);
            proposeFuture.complete(proposeResult);
        } catch (Exception e) {
            proposeFuture.completeExceptionally(e);
        }
    }

    /**
     * 在需要时发布 RaftReady。
     *
     * <p>TODO: 同一时间只允许存在一个未 Advance 的 Ready。
     * EtcdNode 必须先处理完当前 Ready 中的持久化、消息发送和 apply，再提交 Advance 事件；
     * RaftNode 收到 Advance 后才会清理 pending 状态，并继续发布下一轮 Ready。</p>
     */
    private void publishRaftReadyIfNeeded() {
        if (waitingReadyAdvance || !hasReady()) {
            return;
        }
        if (raftReadyEventQueue == null) {
            return;
        }

        boolean offered = raftReadyEventQueue.offer(ready());
        if (offered) {
            waitingReadyAdvance = true;
        }
    }

    // ==================== Raft Event 投递入口：Etcd 层到 Raft 层 ====================

    /**
     * 提交上层 propose 事件。
     *
     * <p>调用方通常是 EtcdNode 的写请求处理逻辑。
     * EtcdNode 会先把 Put/Delete/Txn 等上层命令序列化为 commandData，
     * 再调用该方法把命令提交给 RaftNode。</p>
     *
     * <p>该方法不会直接修改 Raft 状态，只负责：</p>
     * <p>1) 生成事件 ID；</p>
     * <p>2) 注册等待 propose 受理结果的 future；</p>
     * <p>3) 将 PROPOSE 事件投递到 raftEventQueue。</p>
     *
     * @param commandData 上层命令字节
     * @return 等待 Raft 事件循环处理结果的 future
     */
    public CompletableFuture<RaftProposeResult> submitRaftProposeEvent(byte[] commandData) {
        String eventId = nextRaftEventId();
        CompletableFuture<RaftProposeResult> future = new CompletableFuture<>();
        pendingRaftProposeFutureMap.put(eventId, future);

        // PROPOSE 是 RaftNode 内部事件，直接携带上层命令字节即可，不再额外包装事件数据对象。
        offerRaftEvent(raftEventCodec.encodeRaftEvent(RaftEventType.PROPOSE, eventId, commandData));
        return future;
    }

    /**
     * 提交 RequestVote 请求事件。
     */
    public void submitRequestVoteRequestEvent(RequestVoteRequest request) {
        offerRaftEvent(raftEventCodec.encodeRaftEvent(RaftEventType.REQUEST_VOTE, request));
    }

    /**
     * 提交 RequestVote 响应事件。
     */
    public void submitRequestVoteResponseEvent(RequestVoteResponse response) {
        offerRaftEvent(raftEventCodec.encodeRaftEvent(RaftEventType.REQUEST_VOTE_RESPONSE, response));
    }

    /**
     * 提交 AppendEntries 请求事件。
     *
     * <p>调用方通常是 EtcdNode 暴露给 RPC 层的 Raft 服务方法。
     * 当远端 Leader 通过网络发送 AppendEntriesRequest 后，EtcdNode 不直接修改 RaftNode，
     * 而是调用该方法把请求放入 raftEventQueue，由 raft-event-loop 串行处理。</p>
     *
     * @param request AppendEntries 请求
     */
    public void submitAppendEntriesRequestEvent(AppendEntriesRequest request) {
        offerRaftEvent(raftEventCodec.encodeRaftEvent(RaftEventType.APPEND_ENTRIES, request));
    }

    /**
     * 提交 AppendEntries 响应事件。
     *
     * <p>调用方通常是 EtcdNode 暴露给 RPC 层的 Raft 服务方法。
     * 当远端 Follower 返回复制结果后，本地 Leader 通过该事件更新 nextIndex / matchIndex，
     * 并尝试推进 commitIndex。</p>
     *
     * @param response AppendEntries 响应
     */
    public void submitAppendEntriesResponseEvent(AppendEntriesResponse response) {
        offerRaftEvent(raftEventCodec.encodeRaftEvent(RaftEventType.APPEND_ENTRIES_RESPONSE, response));
    }

    /**
     * 提交 InstallSnapshot 请求事件。
     */
    public void submitInstallSnapshotRequestEvent(InstallSnapshotRequest request) {
        offerRaftEvent(raftEventCodec.encodeRaftEvent(RaftEventType.INSTALL_SNAPSHOT, request));
    }

    /**
     * 提交 InstallSnapshot 响应事件。
     */
    public void submitInstallSnapshotResponseEvent(InstallSnapshotResponse response) {
        offerRaftEvent(raftEventCodec.encodeRaftEvent(RaftEventType.INSTALL_SNAPSHOT_RESPONSE, response));
    }

    /**
     * 提交创建快照事件。
     *
     * <p>调用方通常是 EtcdNode 状态机层。
     * 当状态机根据已经 apply 的日志生成快照后，会把快照覆盖的 lastIncludedIndex
     * 和状态机快照字节提交给 RaftNode。</p>
     *
     * @param lastIncludedIndex 快照覆盖的最后日志 index
     * @param stateMachineData  状态机快照数据
     */
    public void submitRaftCreateSnapshotEvent(long lastIncludedIndex, byte[] stateMachineData) {
        RaftCreateSnapshotEventData eventData = new RaftCreateSnapshotEventData();
        eventData.setLastIncludedIndex(lastIncludedIndex);
        if (stateMachineData != null) {
            eventData.setStateMachineData(stateMachineData.clone());
        }
        offerRaftEvent(raftEventCodec.encodeRaftEvent(RaftEventType.CREATE_SNAPSHOT, eventData));
    }

    /**
     * 提交 Ready 推进事件。
     *
     * <p>调用方是 EtcdNode 的 Ready 处理逻辑。
     * EtcdNode 完成当前 RaftReady 中的持久化、RPC 发送、状态机 apply 后，
     * 必须调用该方法通知 RaftNode 清理 pending 状态。</p>
     *
     * @param ready 已处理完成的 Ready
     */
    public void submitRaftAdvanceEvent(RaftReady ready) {
        // ADVANCE 事件只在 RaftNode 内部队列流转，直接携带已处理完成的 Ready。
        offerRaftEvent(raftEventCodec.encodeRaftEvent(RaftEventType.ADVANCE, ready));
    }

    private void offerRaftEvent(RaftEvent event) {
        raftEventQueue.offer(event);
    }

    // ==================== 上层命令提交入口：Propose 到日志复制 ====================

    /**
     * 提交上层命令到 Raft。
     *
     * <p>该方法只在 raft-event-loop 线程内执行，通常由 PROPOSE 事件触发。
     * EtcdNode 外部线程不应该直接调用该方法修改 Raft 状态，而应该调用 submitRaftProposeEvent。</p>
     *
     * <p>处理流程：</p>
     * <p>1) 如果当前节点不是 Leader，返回 notLeader，交给 EtcdNode 做重定向或重试；</p>
     * <p>2) Leader 将 commandData 包装为 RaftLogEntry，并追加到本地 RaftLogState；</p>
     * <p>3) 新日志加入 pendingEntries，等待 EtcdNode 通过 Ready 持久化；</p>
     * <p>4) 更新 Leader 自身 matchIndex；</p>
     * <p>5) 生成 AppendEntries 消息，等待 EtcdNode 从 Ready.messagesToSend 中发送给 Follower；</p>
     * <p>6) 尝试推进 commitIndex。</p>
     *
     * @param commandData 上层命令字节
     * @return 提案受理结果
     */
    public RaftProposeResult propose(byte[] commandData) {
        if (role != RaftRoleType.LEADER) {
            return RaftProposeResult.notLeader(leaderId);
        }

        RaftLogEntry entry = new RaftLogEntry();
        entry.setTerm(currentTerm);
        entry.setCommandData(commandData);

        long logIndex = raftLogState.appendNewLocalLogEntry(entry);
        entry.setIndex(logIndex);

        // 1) 日志已经进入 RaftLogState，但还需要通过 Ready 交给 EtcdNode 持久化。
        pendingEntries.add(entry);

        // 2) Leader 自己天然拥有该日志，先更新自身 matchIndex。
        matchIndexMap.put(nodeId, logIndex);

        // 3) 生成复制消息。真正的网络发送由 EtcdNode 处理 Ready.messagesToSend 时完成。
        broadcastAppendEntries();

        // 4) 单节点集群或多数派已满足时，可能立即推进 commitIndex。
        maybeAdvanceCommitIndex();

        return RaftProposeResult.accepted(logIndex, currentTerm);
    }

    // ==================== Raft RPC 消息入口：网络 RPC 到本地事件处理 ====================

    public void step(RequestVoteRequest request) {
        handleRequestVote(request);
    }

    public void step(RequestVoteResponse response) {
        handleRequestVoteResponse(response);
    }

    public void step(AppendEntriesRequest request) {
        handleAppendEntries(request);
    }

    public void step(AppendEntriesResponse response) {
        handleAppendEntriesResponse(response);
    }

    public void step(InstallSnapshotRequest request) {
        handleInstallSnapshot(request);
    }

    public void step(InstallSnapshotResponse response) {
        handleInstallSnapshotResponse(response);
    }

    // ==================== 时间驱动入口：tick 驱动选举和心跳 ====================

    public void tick() {
        if (role == RaftRoleType.LEADER) {
            tickHeartbeat();
            return;
        }
        tickElection();
    }

    // ==================== RaftReady 模型：Raft 层到 Etcd 层 ====================

    /**
     * 判断当前是否存在需要 EtcdNode 处理的 Ready 内容。
     */
    public boolean hasReady() {
        return pendingHardStateToPersist != null
                || !pendingEntries.isEmpty()
                || pendingSnapshotToPersist != null
                || pendingSnapshotToApply != null
                || !pendingCommittedEntries.isEmpty()
                || !pendingRaftRpcMessages.isEmpty()
                || snapshotCreateRequestedPending.get();
    }

    /**
     * 获取当前 Ready。
     *
     * <p>Ready 是 RaftNode 暴露给 EtcdNode 的副作用批次。</p>
     *
     * <p>Ready 中各字段的上层处理方式：</p>
     * <ul>
     *     <li>hardStateToPersist：EtcdNode 写入稳定存储。</li>
     *     <li>entriesToPersist：EtcdNode 写入 Raft 日志存储。</li>
     *     <li>snapshotToPersist：EtcdNode 写入快照存储。</li>
     *     <li>snapshotToApply：EtcdNode 用快照恢复上层状态机。</li>
     *     <li>messagesToSend：EtcdNode 根据 targetNodeId 发送 Raft RPC。</li>
     *     <li>committedEntries：EtcdNode 按顺序 apply 到状态机。</li>
     * </ul>
     *
     * @return 当前 Ready 快照
     */
    public RaftReady ready() {
        RaftReady ready = new RaftReady();
        ready.setHardStateToPersist(copyHardState(pendingHardStateToPersist));
        ready.setEntriesToPersist(new ArrayList<>(pendingEntries));
        ready.setSnapshotToPersist(copySnapshot(pendingSnapshotToPersist));
        ready.setSnapshotToApply(copySnapshot(pendingSnapshotToApply));
        ready.setCommittedEntries(new ArrayList<>(pendingCommittedEntries));
        ready.setMessagesToSend(new ArrayList<>(pendingRaftRpcMessages));
        ready.setSnapshotCreateRequested(snapshotCreateRequestedPending.get());
        return ready;
    }

    /**
     * 推进 Ready 生命周期。
     *
     * <p>EtcdNode 完成 Ready 中的持久化、消息发送和状态机 apply 后，
     * 会通过 Advance 事件调用该方法，RaftNode 再清理已经被上层消费的 pending 项。</p>
     *
     * <p>清理规则：</p>
     * <p>1) HardState 只有在和当前 pendingHardStateToPersist 匹配时才清理，避免误清理后续新状态；</p>
     * <p>2) Snapshot 按是否出现在已处理 Ready 中清理；</p>
     * <p>3) 日志、committed entries、RPC 消息按 Ready 中的数量从 pending 队首移除。</p>
     *
     * @param ready 已经处理完成的 Ready
     */
    public void advance(RaftReady ready) {
        if (ready.getHardStateToPersist() != null
                && isSameHardState(pendingHardStateToPersist, ready.getHardStateToPersist())) {
            pendingHardStateToPersist = null;
        }
        if (ready.getSnapshotToPersist() != null) {
            pendingSnapshotToPersist = null;
        }
        if (ready.getSnapshotToApply() != null) {
            pendingSnapshotToApply = null;
        }

        // EtcdNode 按 Ready 快照处理这些列表，因此这里按数量移除对应的队首 pending 项。
        removeReadyItems(pendingEntries, ready.getEntriesToPersist().size());
        removeReadyItems(pendingCommittedEntries, ready.getCommittedEntries().size());
        removeReadyItems(pendingRaftRpcMessages, ready.getMessagesToSend().size());

        if (ready.isSnapshotCreateRequested()) {
            snapshotCreateRequestedPending.set(false);
        }

        // TODO: 重置等待标志，允许发布下一轮 Ready。
        waitingReadyAdvance = false;
    }

    // ==================== 选举逻辑 ====================

    private void tickElection() {
        electionElapsed++;
        if (electionElapsed < electionTimeoutTicks) {
            return;
        }
        startElection();
    }

    /**
     * 发起新一轮选举。
     *
     * <p>节点切换为 Candidate，递增 currentTerm，投票给自己，
     * 然后向其他 peer 生成 RequestVote 消息。</p>
     */
    private void startElection() {
        role = RaftRoleType.CANDIDATE;
        currentTerm++;
        votedFor = nodeId;
        markHardStateDirty();
        leaderId = null;
        electionElapsed = 0;

        grantedVoteNodeIds.clear();
        rejectedVoteNodeIds.clear();
        grantedVoteNodeIds.add(nodeId);

        if (hasMajority(grantedVoteNodeIds.size())) {
            becomeLeader();
            return;
        }

        for (String peerNodeId : config.getPeerNodeIds()) {
            RequestVoteRequest request = new RequestVoteRequest();
            request.setTerm(currentTerm);
            request.setCandidateId(nodeId);
            request.setLastLogIndex(raftLogState.getLastLogIndex());
            request.setLastLogTerm(raftLogState.getLastLogTerm());

            addRaftRpcMessage(peerNodeId, RaftRpcMessageType.REQUEST_VOTE, request);
        }
    }

    /**
     * 处理 RequestVote 请求。
     *
     * <p>核心判断：</p>
     * <p>1) 请求 term 过期则拒绝；</p>
     * <p>2) 请求 term 更新则先回退为 Follower；</p>
     * <p>3) 同一任期内只能投票给一个 Candidate；</p>
     * <p>4) Candidate 日志必须至少和本地日志一样新。</p>
     */
    private void handleRequestVote(RequestVoteRequest request) {
        if (request == null || !isKnownPeerNodeId(request.getCandidateId())) {
            return;
        }
        if (request.getTerm() < currentTerm) {
            sendRequestVoteResponse(request.getCandidateId(), false);
            return;
        }

        if (request.getTerm() > currentTerm) {
            becomeFollower(request.getTerm(), null);
        }

        boolean canVote = votedFor == null || votedFor.equals(request.getCandidateId());
        boolean logUpToDate = isCandidateLogUpToDate(request);
        boolean voteGranted = canVote && logUpToDate;

        if (voteGranted) {
            votedFor = request.getCandidateId();
            markHardStateDirty();
            electionElapsed = 0;
        }

        sendRequestVoteResponse(request.getCandidateId(), voteGranted);
    }

    private void handleRequestVoteResponse(RequestVoteResponse response) {
        if (response == null || !isKnownPeerNodeId(response.getVoterId())) {
            return;
        }
        if (response.getTerm() > currentTerm) {
            becomeFollower(response.getTerm(), null);
            return;
        }
        if (role != RaftRoleType.CANDIDATE || response.getTerm() != currentTerm) {
            return;
        }

        if (response.isVoteGranted()) {
            grantedVoteNodeIds.add(response.getVoterId());
            if (hasMajority(grantedVoteNodeIds.size())) {
                becomeLeader();
            }
            return;
        }

        rejectedVoteNodeIds.add(response.getVoterId());
        if (hasMajority(rejectedVoteNodeIds.size())) {
            becomeFollower(currentTerm, null);
        }
    }

    private void sendRequestVoteResponse(String targetNodeId, boolean voteGranted) {
        RequestVoteResponse response = new RequestVoteResponse();
        response.setTerm(currentTerm);
        response.setVoterId(nodeId);
        response.setVoteGranted(voteGranted);

        addRaftRpcMessage(targetNodeId, RaftRpcMessageType.REQUEST_VOTE_RESPONSE, response);
    }

    private boolean isCandidateLogUpToDate(RequestVoteRequest request) {
        long localLastTerm = raftLogState.getLastLogTerm();
        if (request.getLastLogTerm() != localLastTerm) {
            return request.getLastLogTerm() > localLastTerm;
        }
        return request.getLastLogIndex() >= raftLogState.getLastLogIndex();
    }

    // ==================== 日志复制逻辑 ====================

    private void tickHeartbeat() {
        heartbeatElapsed++;
        if (heartbeatElapsed < config.getHeartbeatTimeoutTicks()) {
            return;
        }
        heartbeatElapsed = 0;
        broadcastAppendEntries();
    }

    private void broadcastAppendEntries() {
        if (role != RaftRoleType.LEADER) {
            return;
        }
        for (String peerNodeId : config.getPeerNodeIds()) {
            sendAppendEntriesRequest(peerNodeId);
        }
    }

    /**
     * 向指定 Follower 发送 AppendEntries 请求。
     *
     * <p>如果 Follower 的 nextIndex 已经落在 Leader 本地 snapshot 边界内，
     * 说明 Leader 已经没有该 Follower 缺失的日志实体，只能改为发送 InstallSnapshot。</p>
     */
    private void sendAppendEntriesRequest(String peerNodeId) {
        long nextIndex = getNextIndex(peerNodeId);
        if (nextIndex <= raftLogState.getLastIncludedIndex()) {
            sendInstallSnapshotRequest(peerNodeId);
            return;
        }

        long prevLogIndex = nextIndex - 1L;
        long prevLogTerm = raftLogState.getLogTermByIndex(prevLogIndex);

        AppendEntriesRequest request = new AppendEntriesRequest();
        request.setTerm(currentTerm);
        request.setLeaderId(nodeId);
        request.setPrevLogIndex(prevLogIndex);
        request.setPrevLogTerm(prevLogTerm);
        request.setEntries(raftLogState.getLogEntriesFromIndex(nextIndex));
        request.setLeaderCommit(commitIndex);

        addRaftRpcMessage(peerNodeId, RaftRpcMessageType.APPEND_ENTRIES, request);
    }

    /**
     * 处理 AppendEntries 请求。
     *
     * <p>AppendEntries 同时承担心跳和日志复制职责。
     * Follower 会先校验 prevLogIndex / prevLogTerm 是否匹配，
     * 匹配成功后追加 Leader 日志，并根据 leaderCommit 推进本地 commitIndex。</p>
     */
    private void handleAppendEntries(AppendEntriesRequest request) {
        if (request == null || !isKnownPeerNodeId(request.getLeaderId())) {
            return;
        }
        if (request.getTerm() < currentTerm) {
            sendAppendEntriesResponse(request.getLeaderId(), false, raftLogState.getLastLogIndex(), raftLogState.getLastLogIndex() + 1L);
            return;
        }

        if (request.getTerm() > currentTerm || role != RaftRoleType.FOLLOWER) {
            becomeFollower(request.getTerm(), request.getLeaderId());
        }

        leaderId = request.getLeaderId();
        electionElapsed = 0;

        if (!raftLogState.hasLogPositionWithIndexAndTerm(request.getPrevLogIndex(), request.getPrevLogTerm())) {
            sendAppendEntriesResponse(request.getLeaderId(), false, raftLogState.getLastLogIndex(), buildRejectHint(request));
            return;
        }

        List<RaftLogEntry> appendedEntries = raftLogState.appendLeaderReplicatedLogEntries(request.getPrevLogIndex(), request.getEntries());
        pendingEntries.addAll(appendedEntries);

        if (request.getLeaderCommit() > commitIndex) {
            commitIndex = Math.min(request.getLeaderCommit(), raftLogState.getLastLogIndex());
            collectCommittedEntries();
        }

        long matchIndex = request.getPrevLogIndex();
        if (request.getEntries() != null && !request.getEntries().isEmpty()) {
            matchIndex = request.getPrevLogIndex() + request.getEntries().size();
        }
        sendAppendEntriesResponse(request.getLeaderId(), true, matchIndex, 0L);
    }

    /**
     * 处理 AppendEntries 响应。
     *
     * <p>Leader 根据响应结果更新对应 Follower 的 nextIndex / matchIndex。
     * 成功时推进复制进度并尝试提交日志，失败时根据 rejectHint 回退 nextIndex 后重新复制。</p>
     */
    private void handleAppendEntriesResponse(AppendEntriesResponse response) {
        if (response == null || !isKnownPeerNodeId(response.getFollowerId())) {
            return;
        }
        if (response.getTerm() > currentTerm) {
            becomeFollower(response.getTerm(), null);
            return;
        }
        if (role != RaftRoleType.LEADER || response.getTerm() != currentTerm) {
            return;
        }

        String followerId = response.getFollowerId();
        if (response.isSuccess()) {
            long oldMatchIndex = 0L;
            if (matchIndexMap.get(followerId) != null) {
                oldMatchIndex = matchIndexMap.get(followerId);
            }
            long newMatchIndex = Math.max(oldMatchIndex, response.getMatchIndex());
            matchIndexMap.put(followerId, newMatchIndex);
            nextIndexMap.put(followerId, newMatchIndex + 1L);
            maybeAdvanceCommitIndex();
            return;
        }

        long nextIndex = response.getRejectHint() > 0L ? response.getRejectHint() : getNextIndex(followerId) - 1L;
        if (nextIndex < 1L) {
            nextIndex = 1L;
        }
        nextIndexMap.put(followerId, nextIndex);
        sendAppendEntriesRequest(followerId);
    }

    private void sendAppendEntriesResponse(String targetNodeId, boolean success, long matchIndex, long rejectHint) {
        AppendEntriesResponse response = new AppendEntriesResponse();
        response.setTerm(currentTerm);
        response.setFollowerId(nodeId);
        response.setSuccess(success);
        response.setMatchIndex(matchIndex);
        response.setRejectHint(rejectHint);

        addRaftRpcMessage(targetNodeId, RaftRpcMessageType.APPEND_ENTRIES_RESPONSE, response);
    }

    private long buildRejectHint(AppendEntriesRequest request) {
        long lastLogIndex = raftLogState.getLastLogIndex();
        if (request.getPrevLogIndex() > lastLogIndex) {
            return lastLogIndex + 1L;
        }
        if (request.getPrevLogIndex() < raftLogState.getLastIncludedIndex()) {
            return raftLogState.getLastIncludedIndex() + 1L;
        }
        return Math.max(1L, request.getPrevLogIndex());
    }

    // ==================== Snapshot 复制逻辑 ====================

    /**
     * 向指定 Follower 发送 InstallSnapshot 请求。
     *
     * <p>当 Follower 的 nextIndex 已经落在 Leader 本地 snapshot 边界内时，
     * Leader 无法再通过普通日志复制补齐缺失日志，只能发送快照。</p>
     */
    private void sendInstallSnapshotRequest(String peerNodeId) {
        RaftSnapshot snapshot = copySnapshot(latestSnapshot);
        if (snapshot == null) {
            nextIndexMap.put(peerNodeId, raftLogState.getLastIncludedIndex() + 1L);
            return;
        }

        InstallSnapshotRequest request = new InstallSnapshotRequest();
        request.setTerm(currentTerm);
        request.setLeaderId(nodeId);
        request.setLastIncludedIndex(snapshot.getLastIncludedIndex());
        request.setLastIncludedTerm(snapshot.getLastIncludedTerm());
        request.setLeaderCommit(commitIndex);
        if (snapshot.getStateMachineData() != null) {
            request.setSnapshotData(snapshot.getStateMachineData().clone());
        }

        addRaftRpcMessage(peerNodeId, RaftRpcMessageType.INSTALL_SNAPSHOT, request);
    }

    /**
     * 处理 InstallSnapshot 请求。
     *
     * <p>调用场景：远端 Leader 发现本节点日志落后到 snapshot 边界之前时，
     * 会通过 EtcdNode RPC 层发送 InstallSnapshotRequest，本地 EtcdNode 再投递为 INSTALL_SNAPSHOT 事件。</p>
     *
     * <p>处理流程：</p>
     * <p>1) 校验 term 和 leader 身份；</p>
     * <p>2) 构造 RaftSnapshot，并恢复 RaftLogState 的日志边界；</p>
     * <p>3) 更新 latestSnapshot；</p>
     * <p>4) 清理已经被快照覆盖的 pending 日志和 apply 消息；</p>
     * <p>5) 推进 commitIndex / lastApplied 到快照边界；</p>
     * <p>6) 将 snapshot 放入 pendingSnapshotToPersist / pendingSnapshotToApply，等待 EtcdNode 持久化并恢复状态机；</p>
     * <p>7) 生成 InstallSnapshotResponse，等待 EtcdNode 网络发送。</p>
     */
    private void handleInstallSnapshot(InstallSnapshotRequest request) {
        if (request == null || !isKnownPeerNodeId(request.getLeaderId())) {
            return;
        }
        if (request.getTerm() < currentTerm) {
            sendInstallSnapshotResponse(request.getLeaderId(), false, request.getLastIncludedIndex());
            return;
        }

        if (request.getTerm() > currentTerm || role != RaftRoleType.FOLLOWER) {
            becomeFollower(request.getTerm(), request.getLeaderId());
        }

        leaderId = request.getLeaderId();
        electionElapsed = 0;

        RaftSnapshot snapshot = new RaftSnapshot();
        snapshot.setLastIncludedIndex(request.getLastIncludedIndex());
        snapshot.setLastIncludedTerm(request.getLastIncludedTerm());
        if (request.getSnapshotData() != null) {
            snapshot.setStateMachineData(request.getSnapshotData().clone());
        }

        if (snapshot.getLastIncludedIndex() > raftLogState.getLastIncludedIndex()) {
            // 1) 快照已经覆盖旧日志，RaftLogState 只保留 snapshot 边界之后仍可复用的日志。
            raftLogState.restoreLogStateBySnapshotBoundary(snapshot.getLastIncludedIndex(), snapshot.getLastIncludedTerm());

            // 2) 完整快照数据由 RaftNode 持有，并通过 Ready 交给 EtcdNode 持久化 / apply。
            latestSnapshot = copySnapshot(snapshot);

            // 3) 快照覆盖范围内的日志不需要再持久化或 apply，清理旧 pending。
            pendingEntries.clear();
            pendingCommittedEntries.clear();

            // 4) 安装快照后，本地提交和应用进度至少推进到快照边界。
            commitIndex = Math.max(commitIndex, snapshot.getLastIncludedIndex());
            lastApplied = Math.max(lastApplied, snapshot.getLastIncludedIndex());

            pendingSnapshotToPersist = copySnapshot(snapshot);
            pendingSnapshotToApply = copySnapshot(snapshot);
        }

        sendInstallSnapshotResponse(request.getLeaderId(), true, snapshot.getLastIncludedIndex());
    }

    private void handleInstallSnapshotResponse(InstallSnapshotResponse response) {
        if (response == null || !isKnownPeerNodeId(response.getFollowerId())) {
            return;
        }
        if (response.getTerm() > currentTerm) {
            becomeFollower(response.getTerm(), null);
            return;
        }
        if (role != RaftRoleType.LEADER || response.getTerm() != currentTerm) {
            return;
        }

        String followerId = response.getFollowerId();
        if (!response.isSuccess()) {
            sendInstallSnapshotRequest(followerId);
            return;
        }

        long lastIncludedIndex = response.getLastIncludedIndex();
        long oldMatchIndex = 0L;
        if (matchIndexMap.get(followerId) != null) {
            oldMatchIndex = matchIndexMap.get(followerId);
        }
        long newMatchIndex = Math.max(oldMatchIndex, lastIncludedIndex);
        matchIndexMap.put(followerId, newMatchIndex);
        nextIndexMap.put(followerId, newMatchIndex + 1L);
        sendAppendEntriesRequest(followerId);
    }

    private void sendInstallSnapshotResponse(String targetNodeId, boolean success, long lastIncludedIndex) {
        InstallSnapshotResponse response = new InstallSnapshotResponse();
        response.setTerm(currentTerm);
        response.setFollowerId(nodeId);
        response.setSuccess(success);
        response.setLastIncludedIndex(lastIncludedIndex);
        addRaftRpcMessage(targetNodeId, RaftRpcMessageType.INSTALL_SNAPSHOT_RESPONSE, response);
    }

    /**
     * 创建本地快照。
     *
     * <p>调用场景：EtcdNode 的状态机在 apply 到一定日志位置后，生成状态机快照，
     * 然后通过 submitRaftCreateSnapshotEvent 将快照数据交给 RaftNode。</p>
     *
     * <p>处理流程：</p>
     * <p>1) 忽略已经被 compact 的快照边界；</p>
     * <p>2) 只允许对已经 apply 的日志位置创建快照；</p>
     * <p>3) 查询 lastIncludedIndex 对应的 term；</p>
     * <p>4) 构造 RaftSnapshot；</p>
     * <p>5) 压缩 RaftLogState；</p>
     * <p>6) 将快照放入 pendingSnapshotToPersist，等待 EtcdNode 持久化。</p>
     */
    private void createSnapshot(RaftCreateSnapshotEventData eventData) {
        if (eventData == null || eventData.getLastIncludedIndex() <= raftLogState.getLastIncludedIndex()) {
            return;
        }
        if (eventData.getLastIncludedIndex() > lastApplied) {
            return;
        }

        long lastIncludedTerm = raftLogState.getSnapshotBoundaryLogTerm(eventData.getLastIncludedIndex());

        RaftSnapshot snapshot = new RaftSnapshot();
        snapshot.setLastIncludedIndex(eventData.getLastIncludedIndex());
        snapshot.setLastIncludedTerm(lastIncludedTerm);
        if (eventData.getStateMachineData() != null) {
            snapshot.setStateMachineData(eventData.getStateMachineData().clone());
        }

        raftLogState.compactLogEntriesToSnapshotBoundary(snapshot.getLastIncludedIndex(), snapshot.getLastIncludedTerm());
        latestSnapshot = copySnapshot(snapshot);
        pendingSnapshotToPersist = copySnapshot(snapshot);

        committedLogCountSinceSnapshot = Math.max(0L, commitIndex - snapshot.getLastIncludedIndex());
        snapshotCreateRequestedPending.set(false);
    }

    // ==================== 提交推进逻辑 ====================

    /**
     * 尝试推进 Leader 的 commitIndex。
     *
     * <p>Leader 只提交当前任期内的日志条目。
     * 找到被多数派复制的最高当前任期日志后，推进 commitIndex 并收集 committed entries。</p>
     */
    private void maybeAdvanceCommitIndex() {
        if (role != RaftRoleType.LEADER) {
            return;
        }

        for (long index = raftLogState.getLastLogIndex(); index > commitIndex; index--) {
            RaftLogEntry entry = raftLogState.getLogEntryByIndex(index);
            if (entry == null || entry.getTerm() != currentTerm) {
                continue;
            }
            if (matchCount(index) >= majority()) {
                commitIndex = index;
                collectCommittedEntries();
                broadcastAppendEntries();
                return;
            }
        }
    }

    /**
     * 收集已经提交但尚未 apply 的日志。
     *
     * <p>该方法会把 lastApplied 到 commitIndex 之间的日志转换为 RaftApplyMessage，
     * 等待 EtcdNode 按顺序 apply 到状态机。</p>
     */
    private void collectCommittedEntries() {
        while (lastApplied < commitIndex) {
            lastApplied++;
            RaftLogEntry entry = raftLogState.getLogEntryByIndex(lastApplied);
            if (entry == null) {
                continue;
            }
            pendingCommittedEntries.add(RaftApplyMessage.command(lastApplied, entry.getCommandData()));
            committedLogCountSinceSnapshot++;
            maybeRequestSnapshotCreate();
        }
    }

    /**
     * 根据提交日志计数阈值决定是否请求上层创建快照。
     */
    private void maybeRequestSnapshotCreate() {
        int triggerLogCount = config.getSnapshotTriggerLogCount();
        if (triggerLogCount <= 0) {
            return;
        }
        if (snapshotCreateRequestedPending.get()) {
            return;
        }
        if (committedLogCountSinceSnapshot < triggerLogCount) {
            return;
        }
        snapshotCreateRequestedPending.set(true);
    }

    // ==================== 角色切换 ====================

    private void becomeFollower(long term, String newLeaderId) {
        role = RaftRoleType.FOLLOWER;
        currentTerm = term;
        votedFor = null;
        markHardStateDirty();
        leaderId = newLeaderId;
        electionElapsed = 0;
        heartbeatElapsed = 0;
        grantedVoteNodeIds.clear();
        rejectedVoteNodeIds.clear();
    }

    private void becomeLeader() {
        role = RaftRoleType.LEADER;
        leaderId = nodeId;
        votedFor = nodeId;
        markHardStateDirty();
        electionElapsed = 0;
        heartbeatElapsed = 0;
        grantedVoteNodeIds.clear();
        rejectedVoteNodeIds.clear();

        long nextIndex = raftLogState.getLastLogIndex() + 1L;
        matchIndexMap.put(nodeId, raftLogState.getLastLogIndex());

        for (String peerNodeId : config.getPeerNodeIds()) {
            nextIndexMap.put(peerNodeId, nextIndex);
            matchIndexMap.put(peerNodeId, 0L);
        }

        broadcastAppendEntries();
        maybeAdvanceCommitIndex();
    }

    // ==================== RaftRpcMessage 构造：Raft 层到 Etcd 网络层 ====================

    /**
     * 构造待发送的 Raft RPC 消息。
     *
     * <p>该方法是 RaftNode 和 EtcdNode 网络发送层之间的出口。
     * RaftNode 在选举、日志复制、快照安装过程中调用该方法生成 RaftRpcMessage，
     * 但不会直接访问 RPC 客户端，也不会直接写网络。</p>
     *
     * <p>交互流程：</p>
     * <p>1) RaftNode 根据协议状态决定要发送的 RPC 类型和目标节点；</p>
     * <p>2) addRaftRpcMessage 将 RPC body 序列化并封装为 RaftRpcMessage；</p>
     * <p>3) RaftRpcMessage 进入 pendingRaftRpcMessages；</p>
     * <p>4) ready().messagesToSend 将其暴露给 EtcdNode；</p>
     * <p>5) EtcdNode 根据 targetNodeId 找到 NodeEndpoint，并调用 RPC 客户端发送。</p>
     *
     * @param targetNodeId 目标节点 ID
     * @param type         Raft RPC 消息类型
     * @param body         Raft RPC 请求或响应对象
     */
    private void addRaftRpcMessage(String targetNodeId, RaftRpcMessageType type, Object body) {
        if (targetNodeId == null || targetNodeId.trim().length() == 0) {
            return;
        }
        RaftRpcMessage message = new RaftRpcMessage();
        message.setType(type);
        message.setTargetNodeId(targetNodeId);
        message.setData(serializer.serialize(body));
        pendingRaftRpcMessages.add(message);
    }

    // ==================== 工具方法 ====================

    private boolean isKnownPeerNodeId(String nodeId) {
        if (nodeId == null || nodeId.trim().length() == 0) {
            return false;
        }
        return config.getPeerNodeIds() != null && config.getPeerNodeIds().contains(nodeId);
    }

    private long getNextIndex(String peerNodeId) {
        Long nextIndex = nextIndexMap.get(peerNodeId);
        if (nextIndex == null || nextIndex <= 0L) {
            return raftLogState.getLastLogIndex() + 1L;
        }
        return nextIndex;
    }

    private int matchCount(long index) {
        int count = 0;
        for (Long matchIndex : matchIndexMap.values()) {
            if (matchIndex != null && matchIndex >= index) {
                count++;
            }
        }
        return count;
    }

    private boolean hasMajority(int count) {
        return count >= majority();
    }

    private int majority() {
        return (config.getPeerNodeIds().size() + 1) / 2 + 1;
    }

    private int buildElectionTimeoutTicks(String nodeId, RaftConfig config) {
        int baseTimeout = config.getElectionTimeoutTicks();
        if (baseTimeout <= config.getHeartbeatTimeoutTicks()) {
            baseTimeout = config.getHeartbeatTimeoutTicks() + 3;
        }
        int offset = Math.abs(nodeId.hashCode() % 5);
        return baseTimeout + offset;
    }

    private String nextRaftEventId() {
        return nodeId + "-raft-event-" + raftEventSequence.incrementAndGet();
    }

    private <T> void removeReadyItems(List<T> source, int count) {
        if (count <= 0 || source.isEmpty()) {
            return;
        }
        int toIndex = Math.min(count, source.size());
        source.subList(0, toIndex).clear();
    }

    private void markHardStateDirty() {
        RaftHardState hardState = new RaftHardState();
        hardState.setCurrentTerm(currentTerm);
        hardState.setVotedFor(votedFor);
        pendingHardStateToPersist = hardState;
    }

    private boolean isSameHardState(RaftHardState left, RaftHardState right) {
        if (left == null || right == null) {
            return left == right;
        }
        if (left.getCurrentTerm() != right.getCurrentTerm()) {
            return false;
        }
        if (left.getVotedFor() == null) {
            return right.getVotedFor() == null;
        }
        return left.getVotedFor().equals(right.getVotedFor());
    }

    private RaftHardState copyHardState(RaftHardState source) {
        if (source == null) {
            return null;
        }
        RaftHardState target = new RaftHardState();
        target.setCurrentTerm(source.getCurrentTerm());
        target.setVotedFor(source.getVotedFor());
        return target;
    }

    private RaftSnapshot copySnapshot(RaftSnapshot source) {
        if (source == null) {
            return null;
        }
        RaftSnapshot target = new RaftSnapshot();
        target.setLastIncludedIndex(source.getLastIncludedIndex());
        target.setLastIncludedTerm(source.getLastIncludedTerm());
        if (source.getStateMachineData() != null) {
            target.setStateMachineData(source.getStateMachineData().clone());
        }
        return target;
    }

    // ==================== Getters ====================

    public String getNodeId() {
        return nodeId;
    }

    public RaftRoleType getRole() {
        return role;
    }

    public long getCurrentTerm() {
        return currentTerm;
    }

    public String getVotedFor() {
        return votedFor;
    }

    public String getLeaderId() {
        return leaderId;
    }

    public long getCommitIndex() {
        return commitIndex;
    }

    public long getLastApplied() {
        return lastApplied;
    }

    public RaftSnapshot getLatestSnapshot() {
        return latestSnapshot;
    }

    public RaftLogState getRaftLogState() {
        return raftLogState;
    }

    public RaftConfig getConfig() {
        return config;
    }
}
