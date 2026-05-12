package com.xhj.etcd.kernel.etcd.node;

import com.xhj.etcd.kernel.raft.apply.RaftApplyMessage;
import com.xhj.etcd.kernel.raft.core.RaftConfig;
import com.xhj.etcd.kernel.raft.core.RaftHardState;
import com.xhj.etcd.kernel.raft.core.RaftNode;
import com.xhj.etcd.kernel.raft.core.RaftProposeResult;
import com.xhj.etcd.kernel.raft.core.RaftReady;
import com.xhj.etcd.kernel.raft.core.RaftRoleType;
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
import com.xhj.etcd.kernel.raft.storage.RaftPersistentState;
import com.xhj.etcd.kernel.etcd.command.EtcdCommand;
import com.xhj.etcd.kernel.etcd.command.EtcdCommandApplyResult;
import com.xhj.etcd.kernel.etcd.command.EtcdCommandCodec;
import com.xhj.etcd.kernel.etcd.command.EtcdCommandRegistry;
import com.xhj.etcd.kernel.etcd.command.EtcdCommandType;
import com.xhj.etcd.kernel.etcd.etcdrpc.DeleteRangeRequest;
import com.xhj.etcd.kernel.etcd.etcdrpc.DeleteRangeResponse;
import com.xhj.etcd.kernel.etcd.etcdrpc.DeleteRequest;
import com.xhj.etcd.kernel.etcd.etcdrpc.DeleteResponse;
import com.xhj.etcd.kernel.etcd.etcdrpc.EtcdResponseHeader;
import com.xhj.etcd.kernel.etcd.etcdrpc.EtcdRpcResponse;
import com.xhj.etcd.kernel.etcd.etcdrpc.GetRequest;
import com.xhj.etcd.kernel.etcd.etcdrpc.GetResponse;
import com.xhj.etcd.kernel.etcd.etcdrpc.KeyValueView;
import com.xhj.etcd.kernel.etcd.etcdrpc.PutRequest;
import com.xhj.etcd.kernel.etcd.etcdrpc.PutResponse;
import com.xhj.etcd.kernel.etcd.etcdrpc.RangeRequest;
import com.xhj.etcd.kernel.etcd.etcdrpc.RangeResponse;
import com.xhj.etcd.kernel.etcd.event.EtcdEvent;
import com.xhj.etcd.kernel.etcd.event.EtcdEventCodec;
import com.xhj.etcd.kernel.etcd.event.EtcdEventType;
import com.xhj.etcd.kernel.etcd.registry.NodeEndpointRegistry;
import com.xhj.etcd.kernel.etcd.store.KeyValueDeleteResult;
import com.xhj.etcd.kernel.etcd.store.KeyValueRecord;
import com.xhj.etcd.kernel.etcd.store.KeyValueStore;
import com.xhj.etcd.kernel.etcd.store.KeyValueStoreSnapshot;
import com.xhj.etcd.rpc.NodeEndpoint;
import com.xhj.etcd.rpc.RpcClient;
import com.xhj.etcd.serializer.Serializer;
import com.xhj.etcd.serializer.SerializerRegistry;
import com.xhj.etcd.storage.Storage;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * EtcdNode
 *
 * @author XJks
 * @description 当前阶段的 Etcd 核心节点，负责把 MVCC KV 请求接入 Raft，并消费 RaftReady 完成持久化、网络发送和状态机 apply。
 *
 * <p>阶段边界：</p>
 * <ul>
 *     <li>当前实现服务于 MVCC KV 与 Raft / RPC / Storage 联调闭环，不是未来完整 etcd server 基类。</li>
 *     <li>PUT、DELETE_RANGE 一定转换为 EtcdCommand 并进入 Raft propose。</li>
 *     <li>GET、RANGE 默认线性一致读会进入 Raft，显式非线性一致读或历史 revision 读取则由 etcd-event-loop 本地处理。</li>
 *     <li>状态机使用 KeyValueStore 表达，快照数据只保存在 RaftSnapshot.stateMachineData 中。</li>
 *     <li>后续完整 Txn、Lease、Watch、HashKv、Status、SDK、Console 等能力应在独立阶段演进。</li>
 * </ul>
 *
 * <p>核心交互流程：</p>
 * <ul>
 *     <li>客户端 KV 请求：Etcd RPC Handler -> EtcdEvent -> etcd-event-loop -> EtcdCommand -> RaftNode.submitRaftProposeEvent。</li>
 *     <li>Raft 副作用输出：RaftNode 产生 RaftReady -> raftReadyEventQueue -> EtcdNode 持久化 / apply / 发送消息。</li>
 *     <li>命令结果返回：committed entry apply -> EtcdCommandRegistry.complete -> pending EtcdEvent future 唤醒 RPC 线程。</li>
 *     <li>节点间消息：远端 Raft RPC Handler -> RaftNode.submitXXXEvent；本地 Ready.messagesToSend -> RpcClient.send。</li>
 *     <li>快照流程：状态机生成快照数据 -> submitRaftCreateSnapshotEvent -> RaftSnapshot.stateMachineData -> RaftPersistentState 整体持久化。</li>
 * </ul>
 *
 * <p>与 RaftNode 的职责边界：</p>
 * <ul>
 *     <li>EtcdNode 不直接修改 Raft 核心状态，例如 role、term、log、commitIndex。</li>
 *     <li>客户端请求和远端 Raft RPC 都通过 RaftNode 的 submit 方法进入 raft-event-loop。</li>
 *     <li>RaftNode 通过 RaftReady 输出需要 EtcdNode 处理的副作用。</li>
 *     <li>EtcdNode 处理完当前 Ready 后，必须提交 Advance 事件，让 RaftNode 清理 pending 并发布下一轮 Ready。</li>
 * </ul>
 *
 * <p>TODO: Ready 处理必须先完成关键持久化，再 apply 状态机和发送 RPC，最后才能 Advance。
 * 如果持久化或 apply 失败，不能盲目 Advance，否则可能清理掉尚未安全处理的 pending 状态。</p>
 */
public class EtcdNode {

    // ==================== Storage group / key 常量 ====================

    /**
     * Raft 持久状态存储分组。
     *
     * <p>当前 mini etcd 实现基础持久化恢复能力，但暂不实现生产级 WAL / manifest / checksum /
     * 批量事务写入等复杂崩溃一致性协议。因此将 HardState、Snapshot、剩余日志和状态机 apply 边界
     * 聚合为 RaftPersistentState，并使用单个 storage key 整体写入和恢复，降低多 key 分散读写导致的状态版本不一致风险。</p>
     */
    private static final String RAFT_PERSISTENT_STATE_GROUP = "raft";

    /**
     * Raft 持久状态存储 key。
     */
    private static final String RAFT_PERSISTENT_STATE_KEY = "persistent-state";

    // ==================== Timeout 常量 ====================

    /**
     * 命令从 propose 到 apply 的最大等待时间。
     *
     * <p>当前阶段 RPC 线程会同步等待命令 apply 结果；超时后会清理 commandRegistry 中的等待关系，
     * 并把 timeout 结果返回给临时客户端。</p>
     */
    private static final long COMMAND_APPLY_TIMEOUT_MILLIS = 10000L;

    // ==================== 基础依赖 ====================

    /**
     * 当前 Etcd 节点 ID，同时也是本节点对应的 Raft nodeId。
     */
    private final String nodeId;

    /**
     * 当前阶段统一使用的持久化组件，负责保存 RaftPersistentState 聚合对象。
     */
    private final Storage storage;

    /**
     * 当前节点 Raft 持久状态的内存镜像。
     *
     * <p>每次处理 RaftReady 时，EtcdNode 先将 Ready 中的 HardState、entries、snapshot 合并到该对象，
     * 再将整个 RaftPersistentState 一次性写入 storage。启动恢复时也只从该对象读出，避免多 key 拼装恢复状态。</p>
     */
    private final RaftPersistentState raftPersistentState = new RaftPersistentState();

    /**
     * EtcdCommand、Raft 状态和状态机快照共用的序列化器。
     *
     * <p>当前阶段为了减少联调变量，命令信封、Raft 持久状态和状态机快照统一使用同一个 Serializer。</p>
     */
    private final Serializer serializer;

    /**
     * MVCC KV 命令编解码器，负责 EtcdCommand 的日志边界序列化，以及命令 data 的类型安全读取。
     */
    private final EtcdCommandCodec commandCodec;

    /**
     * Etcd 内部事件辅助器，负责构造 EtcdEvent，并按 type 安全读取事件 data。
     */
    private final EtcdEventCodec eventCodec;

    /**
     * 等待 apply 的命令注册表，用于把 Raft propose 阶段的请求和 apply 阶段的结果关联起来。
     */
    private final EtcdCommandRegistry commandRegistry;

    /**
     * Raft 协议核心实现，EtcdNode 只能通过 submit/ready/advance 边界与其交互。
     */
    private final RaftNode raftNode;

    /**
     * Raft 节点 ID 到 RPC 端点的映射表，用于发送 RaftReady.messagesToSend。
     */
    private final NodeEndpointRegistry nodeEndpointRegistry;

    /**
     * Raft 节点间 RPC 客户端。
     *
     * <p>测试或单节点模式可以为 null；发送 Raft RPC 时会按 best-effort 语义跳过空客户端。</p>
     */
    private final RpcClient raftRpcClient;

    // ==================== 状态机相关状态 ====================

    /**
     * 当前 Etcd 状态机已经 apply 的最高 Raft 日志 index。
     *
     * <p>该字段用于快照创建边界和重启恢复边界；启动恢复时会从 RaftPersistentState.lastAppliedRaftLogIndex 恢复。</p>
     */
    private long lastAppliedRaftLogIndex;

    /**
     * Etcd 应用层 KV 状态机。
     */
    private final KeyValueStore keyValueStore = new KeyValueStore();

    // ==================== 序列 ====================

    /**
     * 命令 ID 序号。
     */
    private final AtomicLong commandSequence = new AtomicLong(0L);

    /**
     * EtcdEvent ID 序号。
     */
    private final AtomicLong etcdEventSequence = new AtomicLong(0L);

    // ==================== EtcdEvent 队列 ====================

    /**
     * EtcdNode 内部事件队列。
     *
     * <p>RPC handler 收到用户请求后，只负责把 XxxRequest 包装为 EtcdEvent 投递到该队列；
     * 是否本地处理、是否进入 Raft、如何完成响应，都由 etcd-event-loop 串行决策。</p>
     */
    private final BlockingQueue<EtcdEvent> etcdEventQueue = new LinkedBlockingQueue<>();

    /**
     * 等待 EtcdEvent 处理结果的 Future 映射。
     *
     * <p>RPC 线程提交事件后在对应 future 上等待；etcd-event-loop 本地读完成或 Raft apply 完成后回填结果。</p>
     */
    private final Map<String, CompletableFuture<EtcdCommandApplyResult>> pendingEtcdEventFutureMap = new ConcurrentHashMap<>();

    // ==================== Ready 队列 ====================

    /**
     * RaftReady 单槽队列。
     *
     * <p>RaftNode 同一时间只允许存在一个未 Advance 的 Ready；容量为 1 的队列用于表达“只交接一个 Ready”这一约束。
     * EtcdNode 从队列取走 Ready 后，仍必须处理完成并提交 Advance，RaftNode 才会发布下一轮 Ready。</p>
     */
    private final BlockingQueue<RaftReady> raftReadyEventQueue = new ArrayBlockingQueue<>(1);

    // ==================== 生命周期 ====================

    private volatile boolean running;

    private Thread etcdEventLoopThread;

    /**
     * Ready 处理失败时统一错误消息前缀。
     */
    private static final String READY_PROCESS_FAILURE_MESSAGE_PREFIX = "raft ready process failed, node will stop for safety, nodeId=";

    // ==================== Etcd RPC 方法名常量 ====================

    public static final String RPC_SERVICE_NAME = "EtcdNode";

    public static final String HANDLE_ETCD_RPC_PUT_REQUEST_METHOD_NAME = "handleEtcdRpcPutRequest";

    public static final String HANDLE_ETCD_RPC_GET_REQUEST_METHOD_NAME = "handleEtcdRpcGetRequest";

    public static final String HANDLE_ETCD_RPC_DELETE_REQUEST_METHOD_NAME = "handleEtcdRpcDeleteRequest";

    public static final String HANDLE_ETCD_RPC_RANGE_REQUEST_METHOD_NAME = "handleEtcdRpcRangeRequest";

    public static final String HANDLE_ETCD_RPC_DELETE_RANGE_REQUEST_METHOD_NAME = "handleEtcdRpcDeleteRangeRequest";

    public static final String HANDLE_RAFT_RPC_REQUEST_VOTE_REQUEST_METHOD_NAME = "handleRaftRpcRequestVoteRequest";

    public static final String HANDLE_RAFT_RPC_REQUEST_VOTE_RESPONSE_METHOD_NAME = "handleRaftRpcRequestVoteResponse";

    public static final String HANDLE_RAFT_RPC_APPEND_ENTRIES_REQUEST_METHOD_NAME = "handleRaftRpcAppendEntriesRequest";

    public static final String HANDLE_RAFT_RPC_APPEND_ENTRIES_RESPONSE_METHOD_NAME = "handleRaftRpcAppendEntriesResponse";

    public static final String HANDLE_RAFT_RPC_INSTALL_SNAPSHOT_REQUEST_METHOD_NAME = "handleRaftRpcInstallSnapshotRequest";

    public static final String HANDLE_RAFT_RPC_INSTALL_SNAPSHOT_RESPONSE_METHOD_NAME = "handleRaftRpcInstallSnapshotResponse";

    // ==================== 构造方法 ====================

    public EtcdNode(String nodeId, RaftConfig raftConfig, Storage storage) {
        this(nodeId, raftConfig, storage, SerializerRegistry.getDefaultSerializer(), null);
    }

    public EtcdNode(String nodeId, RaftConfig raftConfig, Storage storage, Serializer serializer) {
        this(nodeId, raftConfig, storage, serializer, null);
    }

    public EtcdNode(String nodeId, RaftConfig raftConfig, Storage storage, Serializer serializer, RpcClient raftRpcClient) {
        if (nodeId == null || nodeId.trim().isEmpty()) {
            throw new IllegalArgumentException("nodeId must not be empty");
        }
        if (storage == null) {
            throw new IllegalArgumentException("storage must not be null");
        }
        if (serializer == null) {
            throw new IllegalArgumentException("serializer must not be null");
        }

        this.nodeId = nodeId;
        this.storage = storage;
        this.serializer = serializer;
        this.commandCodec = new EtcdCommandCodec(serializer);
        this.eventCodec = new EtcdEventCodec();
        this.commandRegistry = new EtcdCommandRegistry();
        this.nodeEndpointRegistry = new NodeEndpointRegistry();
        this.raftRpcClient = raftRpcClient;
        this.lastAppliedRaftLogIndex = 0L;

        // RaftLogState 是 RaftNode 的内存日志状态入口，启动恢复时会再根据持久化 snapshot 恢复边界。
        RaftLogState raftLogState = new RaftLogState();
        this.raftNode = new RaftNode(nodeId, raftConfig, raftLogState, serializer);
    }

    // ==================== 生命周期 ====================

    /**
     * 启动 EtcdNode。
     *
     * <p>启动顺序不能随意调整：</p>
     * <p>1) 先恢复 Raft 持久状态和状态机快照，保证内存状态与磁盘状态对齐；</p>
     * <p>2) 再启动 RaftNode 事件循环，使 Raft 可以开始产生 Ready；</p>
     * <p>3) 最后启动 etcd-event-loop，消费 RaftReady 并完成上层副作用。</p>
     *
     * <p>如果先启动事件循环再恢复数据，节点可能在状态未恢复完成时对外处理请求或参与 Raft 消息，
     * 这会让 MVCC KV 状态机和 Raft 持久边界产生不一致。</p>
     */
    public void start() {
        if (running) {
            return;
        }

        // 1) 先恢复 Raft 持久状态与状态机快照，再启动事件循环，避免重启后先对外服务再补数据。
        restoreOnStart();
        running = true;

        raftNode.startRaftEventLoop(raftReadyEventQueue);

        etcdEventLoopThread = new Thread(new Runnable() {
            @Override
            public void run() {
                runEtcdEventLoop();
            }
        }, "etcd-event-loop-" + nodeId);
        etcdEventLoopThread.setDaemon(true);
        etcdEventLoopThread.start();
    }

    /**
     * 停止 EtcdNode。
     *
     * <p>该方法只负责停止事件循环和 RaftNode，不额外提交 Advance。
     * 如果当前 Ready 正在处理过程中失败，仍应保持未 Advance 语义，等待重启后基于持久化状态恢复。</p>
     */
    public void stop() {
        running = false;
        raftNode.stopRaftEventLoop();
        if (etcdEventLoopThread != null) {
            etcdEventLoopThread.interrupt();
        }
    }

    // ==================== EventLoop ====================

    /**
     * Etcd 事件循环主流程。
     *
     * <p>该循环同时处理两类输入：</p>
     * <ul>
     *     <li>EtcdEvent：来自用户 RPC 请求，由 etcd-event-loop 决定本地处理还是转换为 EtcdCommand 进入 Raft。</li>
     *     <li>RaftReady：来自 RaftNode，承载需要 EtcdNode 执行的持久化、apply、快照和消息发送副作用。</li>
     * </ul>
     *
     * <p>为什么 RPC handler 不直接提交 Raft：</p>
     * <p>RPC 线程只负责把请求投递为 EtcdEvent 并等待结果；真正是否进入 Raft 由该循环统一判断。
     * 这样后续非线性一致读、状态查询、管理类请求都可以通过 EtcdEvent 分流，而不是散落在各个 RPC handler 中。</p>
     */
    private void runEtcdEventLoop() {
        while (running) {
            try {
                // 1) 先处理一个 EtcdEvent。该事件可能本地完成，也可能转换为 EtcdCommand 异步提交给 Raft。
                EtcdEvent event = etcdEventQueue.poll(20, TimeUnit.MILLISECONDS);
                if (event != null) {
                    processEtcdEventFromQueue(event);
                }

                // 2) 再处理一个 RaftReady。Ready 处理成功后才能 Advance，失败时必须保持未确认状态。
                RaftReady ready = raftReadyEventQueue.poll();
                if (ready != null) {
                    processRaftReadyAndAdvance(ready);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                // 学习版暂不引入日志框架；Ready 处理失败时不 Advance，EtcdEvent 处理失败时由事件 future 返回错误。
            }
        }
    }

    /**
     * 处理单个 RaftReady，并在成功后提交 Advance。
     *
     * <p>
     * TODO:
     *  Ready 处理失败时不能继续运行 event-loop，否则 RaftNode 会永久等待该 Ready 的 advance，导致后续 Ready 不再发布。
     *  当前阶段采用 fail-fast：保持该 Ready 未 advance，并立即停止节点，等待人工介入或重启恢复。</p>
     *
     * @param ready 当前 RaftReady
     */
    private void processRaftReadyAndAdvance(RaftReady ready) {
        try {
            processRaftReadyFromQueue(ready);
            raftNode.submitRaftAdvanceEvent(ready);
        } catch (Exception e) {
            running = false;
            raftNode.stopRaftEventLoop();
            throw new IllegalStateException(READY_PROCESS_FAILURE_MESSAGE_PREFIX + nodeId, e);
        }
    }

    /**
     * 处理单个 EtcdEvent。
     *
     * <p>处理流程：</p>
     * <p>1) 根据 event.type 取出对应 XxxRequest；</p>
     * <p>2) PUT / DELETE 必然转换为 EtcdCommand 进入 Raft；</p>
     * <p>3) GET / RANGE 根据 linearizableRead 和历史 revision 决定进入 Raft 还是本地读取；</p>
     * <p>4) 本地处理直接完成 event future，Raft 路径则等待 apply 阶段回填。</p>
     *
     * @param event Etcd 内部事件
     */
    private void processEtcdEventFromQueue(EtcdEvent event) {
        try {
            switch (event.getType()) {
                case PUT:
                    submitEtcdCommandFromEvent(event, EtcdCommandType.PUT, eventCodec.decodeEtcdEventData(event, EtcdEventType.PUT, PutRequest.class));
                    return;
                case DELETE:
                    submitEtcdCommandFromEvent(event, EtcdCommandType.DELETE, eventCodec.decodeEtcdEventData(event, EtcdEventType.DELETE, DeleteRequest.class));
                    return;
                case GET:
                    processGetEvent(event);
                    return;
                case RANGE:
                    processRangeEvent(event);
                    return;
                case DELETE_RANGE:
                    submitEtcdCommandFromEvent(event, EtcdCommandType.DELETE_RANGE, eventCodec.decodeEtcdEventData(event, EtcdEventType.DELETE_RANGE, DeleteRangeRequest.class));
                    return;
                default:
                    completeEtcdEvent(event.getEventId(), EtcdCommandApplyResult.error(event.getEventId(), "unsupported etcd event type: " + event.getType()));
            }
        } catch (Exception e) {
            completeEtcdEvent(event.getEventId(), EtcdCommandApplyResult.error(event.getEventId(), e.getMessage()));
        }
    }

    /**
     * 处理 GET 事件。
     *
     * <p>linearizableRead=true 时进入 Raft，和写命令处于同一条 apply 顺序流；
     * linearizableRead=false 时只读取当前节点本地状态机，可能读到旧数据，但延迟更低且不要求当前节点是 Leader。</p>
     */
    private void processGetEvent(EtcdEvent event) {
        GetRequest request = eventCodec.decodeEtcdEventData(event, EtcdEventType.GET, GetRequest.class);
        if (request != null && (!request.isLinearizableRead() || request.getRevision() > 0L)) {
            GetResponse response = applyGetRequest(request);
            completeEtcdEvent(event.getEventId(), EtcdCommandApplyResult.success(event.getEventId(), response));
            return;
        }
        submitEtcdCommandFromEvent(event, EtcdCommandType.GET, request);
    }

    /**
     * 处理 RANGE 事件。
     *
     * <p>线性一致读进入 Raft；非线性一致读直接读取本地状态机并返回当前节点视图。</p>
     */
    private void processRangeEvent(EtcdEvent event) {
        RangeRequest request = eventCodec.decodeEtcdEventData(event, EtcdEventType.RANGE, RangeRequest.class);
        if (request != null && (!request.isLinearizableRead() || request.getRevision() > 0L)) {
            RangeResponse response = applyRangeRequest(request);
            completeEtcdEvent(event.getEventId(), EtcdCommandApplyResult.success(event.getEventId(), response));
            return;
        }
        submitEtcdCommandFromEvent(event, EtcdCommandType.RANGE, request);
    }

    /**
     * 将 EtcdEvent 转换为 EtcdCommand 并提交 Raft。
     *
     * <p>这里不阻塞等待 apply，否则 etcd-event-loop 会无法继续消费 RaftReady，导致自己等待自己。
     * 正确做法是注册 command future、提交 Raft propose，然后在 apply 阶段由 commandRegistry 回填结果，
     * 最后桥接完成 RPC handler 等待的 event future。</p>
     */
    private void submitEtcdCommandFromEvent(EtcdEvent event, EtcdCommandType commandType, Object request) {
        EtcdCommand command = buildEtcdCommand(commandType, event.getEventId(), request);
        submitEtcdCommandAsync(event.getEventId(), command);
    }

    /**
     * 异步提交 EtcdCommand 到 Raft。
     *
     * <p>处理流程：</p>
     * <p>1) 先注册 commandId，保证 apply 结果有地方回填；</p>
     * <p>2) 将 apply future 桥接到 event future，用于唤醒 RPC 线程；</p>
     * <p>3) EtcdCommand 整体序列化后提交给 RaftNode；</p>
     * <p>4) Raft propose accepted 后绑定 logIndex；如果不是 Leader，则立即完成 event future。</p>
     */
    private void submitEtcdCommandAsync(final String eventId, final EtcdCommand command) {
        final CompletableFuture<EtcdCommandApplyResult> applyFuture = commandRegistry.register(command.getCommandId());
        applyFuture.whenComplete((result, cause) -> {
            if (cause != null) {
                completeEtcdEvent(eventId, EtcdCommandApplyResult.error(command.getCommandId(), cause.getMessage()));
                return;
            }
            completeEtcdEvent(eventId, result);
        });

        CompletableFuture<RaftProposeResult> proposeFuture = raftNode.submitRaftProposeEvent(commandCodec.encodeEtcdCommand(command));
        proposeFuture.whenComplete((proposeResult, cause) -> {
            if (cause != null) {
                commandRegistry.remove(command.getCommandId());
                completeEtcdEvent(eventId, EtcdCommandApplyResult.error(command.getCommandId(), cause.getMessage()));
                return;
            }
            if (proposeResult == null || !proposeResult.isAccepted()) {
                commandRegistry.remove(command.getCommandId());
                String leaderId = proposeResult == null ? null : proposeResult.getLeaderId();
                completeEtcdEvent(eventId, EtcdCommandApplyResult.notLeader(leaderId));
                return;
            }
            commandRegistry.bindLogIndex(proposeResult.getLogIndex(), command.getCommandId());
        });
    }

    /**
     * 完成 EtcdEvent 等待结果。
     */
    private void completeEtcdEvent(String eventId, EtcdCommandApplyResult result) {
        if (eventId == null) {
            return;
        }
        CompletableFuture<EtcdCommandApplyResult> future = pendingEtcdEventFutureMap.remove(eventId);
        if (future != null) {
            future.complete(result);
        }
    }

    /**
     * 处理单个 RaftReady。
     *
     * <p>Ready 是 RaftNode 输出给 EtcdNode 的副作用批次，本方法就是这些副作用的执行入口。</p>
     *
     * <p>处理顺序不能随意调换：</p>
     * <p>1) 先持久化 HardState、日志和 snapshot，保证 Raft 关键状态先落盘；</p>
     * <p>2) 再 apply snapshotToApply，确保状态机基线恢复到快照边界；</p>
     * <p>3) 再 apply committed entries，保证快照后的日志顺序衔接正确；</p>
     * <p>4) 如果 Ready 请求创建快照，则由 EtcdNode 基于当前状态机生成快照并回传给 RaftNode；</p>
     * <p>5) 最后按 best-effort 语义发送 Raft RPC 消息。</p>
     *
     * <p>为什么发送 RPC 放在最后：Raft 消息可以丢失并由后续 tick 重试，但本地 HardState / Log / Snapshot
     * 不能在消息发出后才落盘，否则可能出现“已经告诉其他节点我有某条日志，但本机宕机恢复后日志不存在”的风险。</p>
     */
    private void processRaftReadyFromQueue(RaftReady ready) {
        // 1) 先持久化 Raft 必要状态。后续 apply 和网络发送都必须建立在本地状态已经落盘的前提上。
        persistRaftReady(ready);

        // 2) 先应用 snapshotToApply，确保状态机基线先恢复到快照边界。
        applyRaftReadySnapshotToStateMachine(ready);

        // 3) 再应用 committed entries，保证快照后的日志顺序衔接正确。
        applyRaftReadyCommittedEntries(ready);

        // 4) 状态机推进后再根据 Ready 请求生成本地状态机快照，保证快照覆盖的是已经 apply 的状态。
        processSnapshotCreateRequest(ready);

        // 5) 最后发送 Raft RPC。消息允许丢失，后续 tick 可以重试，因此不放在持久化之前。
        sendRaftReadyMessages(ready);
    }

    /**
     * 启动阶段恢复入口。
     *
     * <p>恢复分为三步：</p>
     * <p>1) 从单个 RaftPersistentState 中恢复 HardState、Snapshot 和剩余日志；</p>
     * <p>2) 如果存在 snapshot，则直接使用 RaftSnapshot.stateMachineData 恢复内存 KV Map；</p>
     * <p>3) 重放 snapshot 边界之后、且已经 apply 过的持久化日志，使状态机恢复到上次已 apply 的位置。</p>
     */
    private void restoreOnStart() {
        RaftPersistentState persistentState = restoreRaftPersistentState();
        if (persistentState == null) {
            return;
        }

        restoreStateMachineFromPersistedState(persistentState);
    }

    /**
     * 恢复 Raft 持久状态。
     *
     * <p>当前 mini etcd 阶段使用 RaftPersistentState 作为唯一的 Raft 持久化入口。
     * 这样启动时只需要读取一个 storage key，不再从 hardState、snapshot、log 多个 key 中拼装状态，
     * 避免节点宕机后恢复到半新半旧的 Raft 状态。</p>
     *
     * @return 已恢复的 Raft 持久状态；没有持久化状态时返回 null
     */
    private RaftPersistentState restoreRaftPersistentState() {
        byte[] persistentStateBytes = storage.get(RAFT_PERSISTENT_STATE_GROUP, RAFT_PERSISTENT_STATE_KEY);
        if (persistentStateBytes == null) {
            return null;
        }

        RaftPersistentState restoredState = serializer.deserialize(persistentStateBytes, RaftPersistentState.class);
        if (restoredState == null) {
            throw new IllegalStateException("raft persistent state decode failed");
        }

        // 1) 先恢复 HardState，避免节点参与选举前忘记 currentTerm / votedFor。
        if (restoredState.getHardState() != null) {
            raftNode.restoreHardState(restoredState.getHardState());
        }

        // 2) 再恢复 Snapshot 边界，使 RaftLogState 知道日志压缩位置。
        if (restoredState.getSnapshot() != null) {
            raftPersistentState.setSnapshot(copySnapshot(restoredState.getSnapshot()));
            raftNode.restoreFromSnapshot(restoredState.getSnapshot());
        }

        raftPersistentState.setHardState(copyHardState(restoredState.getHardState()));
        // 3) 最后恢复 snapshot 边界之后仍保留的日志条目。
        raftPersistentState.setEntries(copyLogEntries(restoredState.getEntries()));
        raftNode.restoreLogEntries(restoredState.getEntries());

        // 注意：这里不能直接把 EtcdNode.lastAppliedRaftLogIndex 推进到 restoredState 中的边界。
        // 状态机还没有重放日志，提前推进会导致 replayAppliedLogEntries 跳过需要恢复的日志。
        raftPersistentState.setLastAppliedRaftLogIndex(restoredState.getLastAppliedRaftLogIndex());
        return restoredState;
    }

    /**
     * 从 RaftPersistentState 恢复内存 KV Map。
     *
     * <p>状态机恢复只使用 RaftSnapshot.stateMachineData 作为快照数据来源。
     * EtcdNode 不再从 storage 中读取第二份状态机快照，避免 RaftSnapshot 和 Etcd 状态机快照出现双写不一致。</p>
     *
     * <p>恢复流程：</p>
     * <p>1) 先用 snapshot.stateMachineData 恢复快照覆盖位置的完整状态机视图；</p>
     * <p>2) 再重放 snapshot 边界之后、且 index 不超过 lastAppliedRaftLogIndex 的日志；</p>
     * <p>3) 重放时只恢复状态机数据，不唤醒 commandRegistry 中的运行期 future。</p>
     *
     * @param persistentState 已恢复的 Raft 持久状态
     */
    private void restoreStateMachineFromPersistedState(RaftPersistentState persistentState) {
        if (persistentState == null) {
            return;
        }

        RaftSnapshot snapshot = persistentState.getSnapshot();
        if (snapshot != null) {
            restoreStateMachineSnapshotData(snapshot.getStateMachineData(), snapshot.getLastIncludedIndex());
        }

        replayAppliedLogEntries(persistentState.getEntries(), persistentState.getLastAppliedRaftLogIndex());
    }

    /**
     * 应用 Ready 中待恢复的 snapshot。
     *
     * <p>该路径主要对应 Follower 收到 Leader 的 InstallSnapshot 后，RaftNode 通过 Ready.snapshotToApply
     * 把快照交给 EtcdNode。EtcdNode 必须先恢复 snapshot，再 apply snapshot 之后的 committed entries。</p>
     *
     * @param ready 当前 RaftReady
     */
    private void applyRaftReadySnapshotToStateMachine(RaftReady ready) {
        RaftSnapshot snapshotToApply = ready.getSnapshotToApply();
        if (snapshotToApply == null || snapshotToApply.getStateMachineData() == null) {
            return;
        }

        // InstallSnapshot 产生的是状态机完整快照，必须先覆盖状态机，再继续 apply 后续 committed entries。
        restoreStateMachineFromSnapshot(snapshotToApply);
    }

    /**
     * 从指定 RaftSnapshot 恢复状态机。
     *
     * <p>运行时 InstallSnapshot 路径会调用该方法。RaftSnapshot.stateMachineData 是状态机快照的唯一数据来源，
     * 恢复时先清空当前内存状态机，再用快照内容整体重建。</p>
     *
     * @param snapshotToApply 需要应用到状态机的 RaftSnapshot
     */
    private void restoreStateMachineFromSnapshot(RaftSnapshot snapshotToApply) {
        if (snapshotToApply == null) {
            return;
        }
        restoreStateMachineSnapshotData(snapshotToApply.getStateMachineData(), snapshotToApply.getLastIncludedIndex());
    }

    /**
     * 使用状态机快照数据恢复内存 KV Map。
     *
     * <p>该方法是启动恢复和运行期 InstallSnapshot 的共同恢复实现。
     * 调用方负责传入 RaftSnapshot.stateMachineData，不能再从 storage 中读取另一份状态机快照。</p>
     *
     * @param stateMachineData  状态机快照数据
     * @param lastIncludedIndex 快照覆盖的最后日志 index
     */
    private void restoreStateMachineSnapshotData(byte[] stateMachineData, long lastIncludedIndex) {
        if (stateMachineData == null) {
            throw new IllegalStateException("raft snapshot stateMachineData must not be null");
        }

        KeyValueStoreSnapshot snapshot = decodeSnapshotData(stateMachineData);
        if (snapshot == null) {
            throw new IllegalStateException("raft snapshot stateMachineData decode failed");
        }
        keyValueStore.restoreSnapshot(snapshot);
        lastAppliedRaftLogIndex = Math.max(lastAppliedRaftLogIndex, lastIncludedIndex);
    }

    /**
     * 重放已经 apply 过的持久化日志。
     *
     * <p>启动恢复时，状态机先恢复到 snapshot 边界；如果 snapshot 之后还有已经 apply 的日志，
     * 则需要重放这些日志才能恢复到上次停机前的内存 KV 状态。</p>
     *
     * @param entries            snapshot 边界之后持久化保留的日志条目
     * @param targetAppliedIndex 需要恢复到的 apply 边界
     */
    private void replayAppliedLogEntries(List<RaftLogEntry> entries, long targetAppliedIndex) {
        if (entries == null || entries.isEmpty()) {
            return;
        }

        List<RaftLogEntry> sortedEntries = copyLogEntries(entries);
        sortedEntries.sort((left, right) -> Long.compare(left.getIndex(), right.getIndex()));
        for (RaftLogEntry entry : sortedEntries) {
            if (entry.getIndex() <= lastAppliedRaftLogIndex || entry.getIndex() > targetAppliedIndex) {
                continue;
            }
            EtcdCommand command = commandCodec.decodeEtcdCommand(entry.getCommandData());
            if (command != null) {
                applyCommand(command);
                lastAppliedRaftLogIndex = Math.max(lastAppliedRaftLogIndex, entry.getIndex());
            }
        }
    }

    /**
     * 解码状态机快照。
     *
     * <p>当前阶段状态机快照序列化前的结构是 KeyValueStoreSnapshot。
     * 由于反序列化时需要按照该类型整体恢复，因此这里直接以该对象为边界，避免错误快照数据
     * 被恢复进运行态 KeyValueStore。</p>
     *
     * <p>返回 null 表示快照格式不符合当前阶段 MVCC KV 状态机约定，由调用方决定是否忽略或启动失败。</p>
     *
     * @param snapshotData 状态机快照字节
     * @return 解码后的快照 Map；格式错误时返回 null
     */
    private KeyValueStoreSnapshot decodeSnapshotData(byte[] snapshotData) {
        return serializer.deserialize(snapshotData, KeyValueStoreSnapshot.class);
    }

    /**
     * 发送当前 Ready 批次中的 Raft RPC 消息。
     *
     * <p>RaftNode 只生成 RaftRpcMessage，不直接访问网络。EtcdNode 在这里根据 targetNodeId 找到 NodeEndpoint，
     * 再调用 RpcClient 发送到对应远端节点。</p>
     *
     * <p>发送失败不阻塞 Ready 生命周期：Raft 消息本身允许丢失，后续选举 tick、心跳或复制重试会重新产生消息。</p>
     *
     * @param ready 当前 RaftReady
     */
    private void sendRaftReadyMessages(RaftReady ready) {
        List<RaftRpcMessage> messages = ready.getMessagesToSend();
        if (messages == null || messages.isEmpty()) {
            return;
        }
        for (RaftRpcMessage message : messages) {
            // 每条消息单独安全发送，避免某个 follower 不可达影响同一 Ready 中其他消息。
            sendRaftRpcMessage(message);
        }
    }

    /**
     * 安全发送单条 Raft RPC 消息。
     *
     * <p>当前阶段 Raft RPC 发送采用 best-effort 语义：缺少 RpcClient、缺少 endpoint 或单次发送失败时直接跳过，
     * 不让网络异常中断当前 Ready 的持久化 / apply / Advance 流程。</p>
     *
     * @param endpoint   目标节点地址
     * @param methodName EtcdNode 暴露的 Raft RPC 方法名
     * @param request    具体 Raft RPC 请求或响应对象
     */
    private void sendRaftRpcMessageSafely(NodeEndpoint endpoint, String methodName, Object request) {
        if (raftRpcClient == null || endpoint == null || methodName == null || request == null) {
            return;
        }
        try {
            raftRpcClient.send(endpoint, RPC_SERVICE_NAME, methodName, request);
        } catch (Exception ignore) {
            // 不因单条发送异常中断当前 Ready 处理循环。
        }
    }

    /**
     * 按消息类型分发发送 Raft RPC。
     *
     * <p>RaftRpcMessage 是 Raft 层输出的统一信封，message.data 中保存的是具体 RPC body。
     * 本方法根据 message.type 反序列化为对应对象，并路由到 EtcdNode 暴露的 RPC 方法名。</p>
     *
     * <p>这样设计可以让 RaftNode 保持纯协议层职责：它只知道要发送什么类型的 Raft 消息和目标 nodeId，
     * 不直接依赖 RpcClient、NodeEndpoint 或服务方法名。</p>
     *
     * @param message RaftNode 输出的待发送 RPC 消息
     */
    private void sendRaftRpcMessage(RaftRpcMessage message) {
        if (message == null || message.getType() == null || message.getTargetNodeId() == null) {
            return;
        }

        // 1) RaftNode 只输出 targetNodeId，这里由 EtcdNode 映射成真实 RPC endpoint。
        NodeEndpoint endpoint = nodeEndpointRegistry.resolve(message.getTargetNodeId());
        if (endpoint == null) {
            return;
        }

        // 2) RaftRpcMessage.data 是具体 RPC body 的序列化结果，需要按 type 反序列化并路由到对应方法名。
        RaftRpcMessageType type = message.getType();
        switch (type) {
            case REQUEST_VOTE: {
                RequestVoteRequest request = serializer.deserialize(message.getData(), RequestVoteRequest.class);
                sendRaftRpcMessageSafely(endpoint, HANDLE_RAFT_RPC_REQUEST_VOTE_REQUEST_METHOD_NAME, request);
                return;
            }
            case REQUEST_VOTE_RESPONSE: {
                RequestVoteResponse response = serializer.deserialize(message.getData(), RequestVoteResponse.class);
                sendRaftRpcMessageSafely(endpoint, HANDLE_RAFT_RPC_REQUEST_VOTE_RESPONSE_METHOD_NAME, response);
                return;
            }
            case APPEND_ENTRIES: {
                AppendEntriesRequest request = serializer.deserialize(message.getData(), AppendEntriesRequest.class);
                sendRaftRpcMessageSafely(endpoint, HANDLE_RAFT_RPC_APPEND_ENTRIES_REQUEST_METHOD_NAME, request);
                return;
            }
            case APPEND_ENTRIES_RESPONSE: {
                AppendEntriesResponse response = serializer.deserialize(message.getData(), AppendEntriesResponse.class);
                sendRaftRpcMessageSafely(endpoint, HANDLE_RAFT_RPC_APPEND_ENTRIES_RESPONSE_METHOD_NAME, response);
                return;
            }
            case INSTALL_SNAPSHOT: {
                InstallSnapshotRequest request = serializer.deserialize(message.getData(), InstallSnapshotRequest.class);
                sendRaftRpcMessageSafely(endpoint, HANDLE_RAFT_RPC_INSTALL_SNAPSHOT_REQUEST_METHOD_NAME, request);
                return;
            }
            case INSTALL_SNAPSHOT_RESPONSE: {
                InstallSnapshotResponse response = serializer.deserialize(message.getData(), InstallSnapshotResponse.class);
                sendRaftRpcMessageSafely(endpoint, HANDLE_RAFT_RPC_INSTALL_SNAPSHOT_RESPONSE_METHOD_NAME, response);
                return;
            }
            default:
                return;
        }
    }

    // ==================== Raft RPC Handler 方法 ====================

    /**
     * 处理 RequestVote 请求。
     *
     * <p>该方法是 RPC 层进入 Raft 层的入口之一。它不直接调用 Raft 投票逻辑，
     * 而是把请求提交到 RaftNode 的事件队列，由 raft-event-loop 串行处理。</p>
     */
    public void handleRaftRpcRequestVoteRequest(RequestVoteRequest request) {
        raftNode.submitRequestVoteRequestEvent(request);
    }

    /**
     * 处理 RequestVote 响应。
     */
    public void handleRaftRpcRequestVoteResponse(RequestVoteResponse response) {
        raftNode.submitRequestVoteResponseEvent(response);
    }

    /**
     * 处理 AppendEntries 请求。
     *
     * <p>远端 Leader 发来的日志复制或心跳请求会进入这里。EtcdNode 只负责转交给 RaftNode，
     * 不在 RPC 线程内直接修改本地日志或 commitIndex。</p>
     */
    public void handleRaftRpcAppendEntriesRequest(AppendEntriesRequest request) {
        raftNode.submitAppendEntriesRequestEvent(request);
    }

    /**
     * 处理 AppendEntries 响应。
     */
    public void handleRaftRpcAppendEntriesResponse(AppendEntriesResponse response) {
        raftNode.submitAppendEntriesResponseEvent(response);
    }

    /**
     * 处理 InstallSnapshot 请求。
     *
     * <p>远端 Leader 发来的快照安装请求会进入这里。真正的 snapshot 边界更新、Ready.snapshotToPersist
     * 和 Ready.snapshotToApply 生成都由 RaftNode 在事件循环中完成。</p>
     */
    public void handleRaftRpcInstallSnapshotRequest(InstallSnapshotRequest request) {
        raftNode.submitInstallSnapshotRequestEvent(request);
    }

    /**
     * 处理 InstallSnapshot 响应。
     */
    public void handleRaftRpcInstallSnapshotResponse(InstallSnapshotResponse response) {
        raftNode.submitInstallSnapshotResponseEvent(response);
    }

    // ==================== RaftReady 持久化 ====================

    /**
     * 持久化 Ready 中的 Raft 关键状态。
     *
     * <p>当前 mini etcd 阶段采用 RaftPersistentState 单对象持久化方案。
     * 每次处理 Ready 时，先将 HardState、entries、snapshot 合并到内存镜像，
     * 再整体序列化写入同一个 storage key。</p>
     *
     * <p>这样做的原因：</p>
     * <p>1) 当前项目需要支持节点重启后的基础状态恢复；</p>
     * <p>2) 当前阶段暂不实现生产级 WAL、manifest、checksum、批量事务写入等复杂崩溃一致性协议；</p>
     * <p>3) 如果把 HardState、Snapshot、Log 分散写到多个 key，宕机可能导致恢复时读到半新半旧的状态；</p>
     * <p>4) 将状态聚合后整体写入，可以让当前阶段的恢复边界更简单、更清晰。</p>
     *
     * @param ready 当前 RaftReady
     */
    private void persistRaftReady(RaftReady ready) {
        if (ready == null) {
            return;
        }

        // 1) 先合并 Ready 中的新 HardState / Entries / Snapshot。
        updateRaftPersistentState(ready);

        // 2) 再整体写入单个 storage key，避免多 key 分散写入造成恢复版本不一致。
        saveRaftPersistentState();
    }

    /**
     * 将 Ready 合并进 RaftPersistentState 内存镜像。
     *
     * <p>这里处理的是“准备落盘的 Raft 状态”，不直接修改状态机。
     * 状态机 apply 边界会在 applyRaftApplyMessage 中随日志应用推进并再次整体写入。</p>
     *
     * @param ready 当前 RaftReady
     */
    private void updateRaftPersistentState(RaftReady ready) {
        if (ready.getHardStateToPersist() != null) {
            raftPersistentState.setHardState(copyHardState(ready.getHardStateToPersist()));
        }

        if (ready.getSnapshotToPersist() != null) {
            RaftSnapshot snapshot = copySnapshot(ready.getSnapshotToPersist());
            raftPersistentState.setSnapshot(snapshot);
            removeCompactedPersistentEntries(snapshot.getLastIncludedIndex());
        }

        appendOrReplacePersistentEntries(ready.getEntriesToPersist());
    }

    /**
     * 整体保存 RaftPersistentState。
     */
    private void saveRaftPersistentState() {
        storage.put(RAFT_PERSISTENT_STATE_GROUP, RAFT_PERSISTENT_STATE_KEY, serializer.serialize(raftPersistentState));
    }

    /**
     * 把 Ready 中的新日志合并进持久状态镜像。
     *
     * <p>Follower 接收 Leader 日志时可能覆盖冲突后缀，因此不能简单 addAll；
     * 每写入一条新日志，都要先删除该 index 及其之后的旧日志，再追加新日志。</p>
     *
     * @param entries Ready 中需要持久化的日志条目
     */
    private void appendOrReplacePersistentEntries(List<RaftLogEntry> entries) {
        if (entries == null || entries.isEmpty()) {
            return;
        }
        if (raftPersistentState.getEntries() == null) {
            raftPersistentState.setEntries(new ArrayList<>());
        }

        for (RaftLogEntry entry : entries) {
            if (entry == null) {
                continue;
            }
            removePersistentEntriesFromIndex(entry.getIndex());
            raftPersistentState.getEntries().add(copyLogEntry(entry));
        }
        raftPersistentState.getEntries().sort((left, right) -> Long.compare(left.getIndex(), right.getIndex()));
    }

    private void removePersistentEntriesFromIndex(long index) {
        List<RaftLogEntry> entries = raftPersistentState.getEntries();
        if (entries == null || entries.isEmpty()) {
            return;
        }
        entries.removeIf(entry -> entry != null && entry.getIndex() >= index);
    }

    private void removeCompactedPersistentEntries(long lastIncludedIndex) {
        List<RaftLogEntry> entries = raftPersistentState.getEntries();
        if (entries == null || entries.isEmpty()) {
            return;
        }
        entries.removeIf(entry -> entry != null && entry.getIndex() <= lastIncludedIndex);
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
            target.setStateMachineData(Arrays.copyOf(source.getStateMachineData(), source.getStateMachineData().length));
        }
        return target;
    }

    private List<RaftLogEntry> copyLogEntries(List<RaftLogEntry> sourceEntries) {
        List<RaftLogEntry> copiedEntries = new ArrayList<>();
        if (sourceEntries == null) {
            return copiedEntries;
        }
        for (RaftLogEntry entry : sourceEntries) {
            copiedEntries.add(copyLogEntry(entry));
        }
        return copiedEntries;
    }

    private RaftLogEntry copyLogEntry(RaftLogEntry source) {
        if (source == null) {
            return null;
        }
        RaftLogEntry target = new RaftLogEntry();
        target.setIndex(source.getIndex());
        target.setTerm(source.getTerm());
        if (source.getCommandData() != null) {
            target.setCommandData(Arrays.copyOf(source.getCommandData(), source.getCommandData().length));
        }
        return target;
    }

    // ==================== RaftReady Apply ====================

    /**
     * 应用 Ready 中已经 committed 的日志。
     *
     * <p>RaftNode 只保证这些日志已经提交，并把 commandData 透传回来；
     * EtcdNode 在这里按顺序反序列化 EtcdCommand，并执行到 MVCC KV 状态机。</p>
     *
     * @param ready 当前 RaftReady
     */
    private void applyRaftReadyCommittedEntries(RaftReady ready) {
        for (RaftApplyMessage applyMessage : ready.getCommittedEntries()) {
            // RaftReady.committedEntries 已经按日志顺序输出，这里保持顺序 apply，不能并发打乱。
            applyRaftApplyMessage(applyMessage);
        }
    }

    /**
     * 应用单条 RaftApplyMessage。
     *
     * <p>处理流程：</p>
     * <p>1) 忽略非命令消息；</p>
     * <p>2) 将 Raft 透传的 commandData 解码为 EtcdCommand；</p>
     * <p>3) 根据 command.type 执行到 MVCC KV 状态机；</p>
     * <p>4) 推进 lastAppliedRaftLogIndex；</p>
     * <p>5) 通过 commandId + logIndex 唤醒等待该 EtcdEvent 的 RPC 调用。</p>
     *
     * <p>这里必须使用 commandId 参与完成结果，避免旧 Leader 未提交日志被新 Leader 覆盖后，
     * 仅凭相同 logIndex 错误唤醒旧请求。</p>
     *
     * @param applyMessage Raft 层输出的 apply 消息
     */
    private void applyRaftApplyMessage(RaftApplyMessage applyMessage) {
        if (!applyMessage.isCommandValid()) {
            return;
        }

        // 1) Raft 层只透传 commandData，EtcdNode 在 apply 阶段才解析出 EtcdCommand。
        EtcdCommand command = commandCodec.decodeEtcdCommand(applyMessage.getCommandData());
        if (command == null) {
            return;
        }

        // 2) 在真正修改状态机之前先持久化 apply 边界。
        //    重启时会基于持久化日志重放到该边界，避免“状态机已修改但 apply 边界未落盘”导致恢复丢失。
        lastAppliedRaftLogIndex = Math.max(lastAppliedRaftLogIndex, applyMessage.getLogIndex());
        raftPersistentState.setLastAppliedRaftLogIndex(lastAppliedRaftLogIndex);
        saveRaftPersistentState();

        // 3) 命令真正执行到 MVCC KV 状态机，并生成可返回给 RPC 调用方的 apply 结果。
        EtcdCommandApplyResult result = applyCommand(command);

        // 4) 使用 logIndex + commandId 完成等待方，避免旧 Leader 的同 index 命令被错误唤醒。
        commandRegistry.complete(applyMessage.getLogIndex(), command.getCommandId(), result);
    }

    /**
     * 执行 EtcdCommand。
     *
     * <p>EtcdCommand.data 直接保存 XxxRequest 对象，不再经过 XxxCommand 中间壳。
     * 该方法运行在 apply 阶段，此时命令已经经过 Raft committed 顺序，因此可以安全作用到 MVCC KV 状态机。</p>
     *
     * @param command Etcd 命令信封
     * @return 命令执行结果
     */
    private EtcdCommandApplyResult applyCommand(EtcdCommand command) {
        try {
            EtcdCommandType type = command.getType();
            switch (type) {
                case PUT: {
                    PutRequest request = commandCodec.decodeCommandData(command, EtcdCommandType.PUT, PutRequest.class);
                    if (request == null) {
                        return EtcdCommandApplyResult.error(command.getCommandId(), "put request is null");
                    }
                    return EtcdCommandApplyResult.success(command.getCommandId(), applyPutRequest(request));
                }
                case DELETE: {
                    DeleteRequest request = commandCodec.decodeCommandData(command, EtcdCommandType.DELETE, DeleteRequest.class);
                    if (request == null) {
                        return EtcdCommandApplyResult.error(command.getCommandId(), "delete request is null");
                    }
                    return EtcdCommandApplyResult.success(command.getCommandId(), applyDeleteRequest(request));
                }
                case GET: {
                    GetRequest request = commandCodec.decodeCommandData(command, EtcdCommandType.GET, GetRequest.class);
                    if (request == null) {
                        return EtcdCommandApplyResult.error(command.getCommandId(), "get request is null");
                    }
                    return EtcdCommandApplyResult.success(command.getCommandId(), applyGetRequest(request));
                }
                case RANGE: {
                    RangeRequest request = commandCodec.decodeCommandData(command, EtcdCommandType.RANGE, RangeRequest.class);
                    if (request == null) {
                        return EtcdCommandApplyResult.error(command.getCommandId(), "range request is null");
                    }
                    return EtcdCommandApplyResult.success(command.getCommandId(), applyRangeRequest(request));
                }
                case DELETE_RANGE: {
                    DeleteRangeRequest request = commandCodec.decodeCommandData(command, EtcdCommandType.DELETE_RANGE, DeleteRangeRequest.class);
                    if (request == null) {
                        return EtcdCommandApplyResult.error(command.getCommandId(), "delete-range request is null");
                    }
                    return EtcdCommandApplyResult.success(command.getCommandId(), applyDeleteRangeRequest(request));
                }
                default:
                    return EtcdCommandApplyResult.error(command.getCommandId(), "unsupported command type: " + type);
            }
        } catch (Exception e) {
            return EtcdCommandApplyResult.error(command.getCommandId(), e.getMessage());
        }
    }

    /**
     * 应用 PUT 请求。
     *
     * <p>PUT 请求已经通过 Raft apply 顺序到达这里，因此可以直接修改当前节点状态机。</p>
     *
     * @param request PUT 请求
     */
    private PutResponse applyPutRequest(PutRequest request) {
        KeyValueRecord record = keyValueStore.put(request.getKey(), request.getValue());
        return PutResponse.of(record.getModRevision());
    }

    /**
     * 应用 DELETE 请求。
     *
     * @param request DELETE 请求
     * @return 实际删除的 key 数量
     */
    private DeleteResponse applyDeleteRequest(DeleteRequest request) {
        KeyValueDeleteResult result = keyValueStore.delete(request.getKey());
        return DeleteResponse.of(result.getDeletedCount(), result.getRevision());
    }

    /**
     * 应用 GET 请求。
     *
     * <p>该方法既可用于线性一致读的 apply 阶段，也可用于非线性一致读的本地事件处理。
     * 两者的区别不在读取逻辑，而在调用方是否先让请求经过 Raft 顺序化。</p>
     *
     * @param request GET 请求
     * @return key 对应的字符串值；不存在时返回 null
     */
    private GetResponse applyGetRequest(GetRequest request) {
        KeyValueRecord record = keyValueStore.get(request.getKey(), request.getRevision());
        long effectiveRevision = request.getRevision() > 0L ? request.getRevision() : keyValueStore.currentRevision();
        if (record == null) {
            return GetResponse.empty(effectiveRevision);
        }
        return GetResponse.of(
                record.getValue(),
                record.getCreateRevision(),
                record.getModRevision(),
                record.getVersion(),
                effectiveRevision);
    }

    /**
     * 应用 RANGE 请求。
     *
     * <p>返回前对 key 排序，避免 ConcurrentHashMap 遍历顺序不稳定导致测试结果不稳定。</p>
     *
     * @param request LIST_KEYS 请求
     * @return 排序后的 key 列表
     */
    private RangeResponse applyRangeRequest(RangeRequest request) {
        List<KeyValueRecord> records = keyValueStore.range(
                request.getStartKey(),
                request.getEndKeyExclusive(),
                request.isPrefixMatch(),
                request.getLimit(),
                request.isKeysOnly(),
                request.isCountOnly(),
                request.getRevision());

        List<KeyValueView> items = new ArrayList<>();
        int matchedCount;
        if (request.isCountOnly()) {
            List<KeyValueRecord> matchedRecords = keyValueStore.range(
                    request.getStartKey(),
                    request.getEndKeyExclusive(),
                    request.isPrefixMatch(),
                    request.getLimit(),
                    request.isKeysOnly(),
                    false,
                    request.getRevision());
            matchedCount = matchedRecords.size();
        } else {
            for (KeyValueRecord record : records) {
                items.add(KeyValueView.of(
                        record.getKey(),
                        record.getValue(),
                        record.getCreateRevision(),
                        record.getModRevision(),
                        record.getVersion()));
            }
            matchedCount = items.size();
        }
        long effectiveRevision = request.getRevision() > 0L ? request.getRevision() : keyValueStore.currentRevision();
        return RangeResponse.of(items, matchedCount, effectiveRevision);
    }

    private DeleteRangeResponse applyDeleteRangeRequest(DeleteRangeRequest request) {
        KeyValueDeleteResult result = keyValueStore.deleteRange(
                request.getStartKey(),
                request.getEndKeyExclusive(),
                request.isPrefixMatch(),
                request.isPrevKv());
        List<KeyValueView> prevItems = new ArrayList<>();
        for (KeyValueRecord record : result.getPreviousRecords()) {
            prevItems.add(KeyValueView.of(
                    record.getKey(),
                    record.getValue(),
                    record.getCreateRevision(),
                    record.getModRevision(),
                    record.getVersion()));
        }
        return DeleteRangeResponse.of(result.getDeletedCount(), result.getRevision(), prevItems);
    }

    /**
     * 处理 RaftReady 的快照创建请求。
     *
     * <p>当 RaftNode 认为可以创建快照时，会在 Ready 中设置 snapshotCreateRequested。
     * EtcdNode 只负责把当前内存状态机编码为 stateMachineData 并提交给 RaftNode；
     * 真正的 RaftSnapshot 会由 RaftNode 创建，并在后续 Ready.snapshotToPersist 中通过 RaftPersistentState 整体落盘。</p>
     *
     * @param ready 当前 RaftReady
     */
    private void processSnapshotCreateRequest(RaftReady ready) {
        if (!ready.isSnapshotCreateRequested()) {
            return;
        }

        // 1) 从当前内存状态机构建完整快照数据。这里不能只保存增量，因为恢复时会整体覆盖状态机。
        byte[] stateMachineSnapshotData = serializer.serialize(buildSnapshotStateMachineData());

        // 2) 不再单独持久化 kv-snapshot。stateMachineSnapshotData 会进入 RaftSnapshot，
        //    后续通过 Ready.snapshotToPersist 合并进 RaftPersistentState 统一写入。
        raftNode.submitRaftCreateSnapshotEvent(lastAppliedRaftLogIndex, stateMachineSnapshotData);
    }

    /**
     * 构建状态机快照数据。
     *
     * <p>该方法会深拷贝当前运行态状态机，避免把运行态对象直接交给序列化器或 RaftSnapshot。
     * 这样可以防止快照生成过程中状态机继续变化，导致快照数据被后续写入污染。</p>
     *
     * @return 可序列化的状态机快照数据
     */
    private KeyValueStoreSnapshot buildSnapshotStateMachineData() {
        return keyValueStore.createSnapshot();
    }

    // ==================== EtcdEvent 提交与等待 ====================

    /**
     * 提交 EtcdEvent 并等待处理结果。
     *
     * <p>RPC handler 统一走该入口：先把用户 XxxRequest 包装为 EtcdEvent 投递到 etcdEventQueue，
     * 再等待 etcd-event-loop 本地处理或 Raft apply 后回填结果。</p>
     *
     * @param type    事件类型
     * @param request 用户请求对象
     * @return 事件处理结果
     */
    private EtcdCommandApplyResult submitEtcdEventAndWait(EtcdEventType type, Object request) {
        String eventId = nextEtcdEventId();
        CompletableFuture<EtcdCommandApplyResult> future = new CompletableFuture<>();
        pendingEtcdEventFutureMap.put(eventId, future);

        // RPC 线程只投递 EtcdEvent，不直接判断本地读或提交 Raft，避免 EtcdNode 主流程分散在多个 RPC handler 中。
        etcdEventQueue.offer(eventCodec.encodeEtcdEvent(type, eventId, request));

        try {
            return future.get(COMMAND_APPLY_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            pendingEtcdEventFutureMap.remove(eventId);
            commandRegistry.remove(eventId);
            return EtcdCommandApplyResult.timeout(eventId);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            pendingEtcdEventFutureMap.remove(eventId);
            commandRegistry.remove(eventId);
            return EtcdCommandApplyResult.error(eventId, "interrupted while waiting etcd event");
        } catch (Exception e) {
            pendingEtcdEventFutureMap.remove(eventId);
            commandRegistry.remove(eventId);
            return EtcdCommandApplyResult.error(eventId, e.getMessage());
        }
    }

    // ==================== Etcd RPC Handler 方法 ====================

    /**
     * 提交命令到 Raft 共识。
     *
     * @param commandData 命令数据
     * @return 包含共识结果的 CompletableFuture
     */
    public CompletableFuture<RaftProposeResult> propose(byte[] commandData) {
        return raftNode.submitRaftProposeEvent(commandData);
    }

    /**
     * 处理 PUT 请求。
     *
     * <p>RPC 线程只负责把 PutRequest 投递为 EtcdEvent 并等待结果；真正的 EtcdCommand 构建和 Raft 提交由 etcd-event-loop 完成。</p>
     */
    public EtcdRpcResponse<PutResponse> handleEtcdRpcPutRequest(PutRequest request) {
        return buildRpcResponse(submitEtcdEventAndWait(EtcdEventType.PUT, request), PutResponse.class);
    }

    /**
     * 处理 GET 请求。
     *
     * <p>linearizableRead=true 时，etcd-event-loop 会将请求转换为 EtcdCommand 进入 Raft；
     * linearizableRead=false 时，etcd-event-loop 直接读取本地状态机。</p>
     */
    public EtcdRpcResponse<GetResponse> handleEtcdRpcGetRequest(GetRequest request) {
        return buildRpcResponse(submitEtcdEventAndWait(EtcdEventType.GET, request), GetResponse.class);
    }

    /**
     * 处理 DELETE 请求。
     */
    public EtcdRpcResponse<DeleteResponse> handleEtcdRpcDeleteRequest(DeleteRequest request) {
        return buildRpcResponse(submitEtcdEventAndWait(EtcdEventType.DELETE, request), DeleteResponse.class);
    }

    /**
     * 处理 RANGE 请求。
     */
    public EtcdRpcResponse<RangeResponse> handleEtcdRpcRangeRequest(RangeRequest request) {
        return buildRpcResponse(submitEtcdEventAndWait(EtcdEventType.RANGE, request), RangeResponse.class);
    }

    /**
     * 处理 DELETE_RANGE 请求。
     */
    public EtcdRpcResponse<DeleteRangeResponse> handleEtcdRpcDeleteRangeRequest(DeleteRangeRequest request) {
        return buildRpcResponse(submitEtcdEventAndWait(EtcdEventType.DELETE_RANGE, request), DeleteRangeResponse.class);
    }

    /**
     * 根据 apply/event 结果构造 RPC 响应信封。
     */
    private <T> EtcdRpcResponse<T> buildRpcResponse(EtcdCommandApplyResult result, Class<T> bodyClass) {
        try {
            T body = result == null ? null : result.getBodyAs(bodyClass);
            return EtcdRpcResponse.fromApplyResult(result, body);
        } catch (Exception e) {
            return EtcdRpcResponse.of(EtcdResponseHeader.error(e.getMessage()), null);
        }
    }

    // ==================== 命令构建 ====================

    /**
     * 构建 EtcdCommand 信封。
     *
     * <p>当前阶段不再为每种请求创建额外中间命令对象，而是直接把 XxxRequest 放入 command.data。
     * 这样 EtcdEvent 和 EtcdCommand 都使用同一份业务请求对象，只有进入 Raft 日志前才将 EtcdCommand 整体序列化。</p>
     */
    private EtcdCommand buildEtcdCommand(EtcdCommandType type, String commandId, Object request) {
        EtcdCommand command = new EtcdCommand();
        command.setType(type);
        command.setCommandId(commandId == null ? nextCommandId() : commandId);
        command.setData(request);
        return command;
    }

    private String nextCommandId() {
        return nodeId + "-command-" + commandSequence.incrementAndGet();
    }

    private String nextEtcdEventId() {
        return nodeId + "-event-" + etcdEventSequence.incrementAndGet();
    }

    // ==================== Getters ====================

    public String getNodeId() {
        return nodeId;
    }

    public RaftRoleType getRole() {
        return raftNode.getRole();
    }

    public RaftNode getRaftNode() {
        return raftNode;
    }

    public Storage getStorage() {
        return storage;
    }

    public EtcdCommandCodec getCommandCodec() {
        return commandCodec;
    }

    public void registerNodeEndpoint(NodeEndpoint endpoint) {
        if (endpoint != null) {
            nodeEndpointRegistry.register(endpoint);
        }
    }

    public NodeEndpointRegistry getNodeEndpointRegistry() {
        return nodeEndpointRegistry;
    }

    public long getLastAppliedIndex() {
        return lastAppliedRaftLogIndex;
    }

    /**
     * 测试辅助：读取当前节点本地状态机中的 value。
     *
     * <p>该方法绕过 Raft，不提供强一致读语义，只用于测试或调试当前节点本地状态机内容。</p>
     */
    public String getLocalStateMachineValue(String group, String key) {
        KeyValueRecord record = keyValueStore.get(key, 0L);
        if (record == null) {
            return null;
        }
        return record.getValue();
    }
}
