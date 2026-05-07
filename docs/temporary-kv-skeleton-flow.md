# 临时 KV 骨架联调全链路说明（EtcdClient / RPC / EtcdNode / RaftNode）

## 1. 文档范围

本文只描述当前项目“临时 KV 服务”阶段的真实实现，不覆盖未来完整 etcd 能力（MVCC、Lease、Watch、ReadIndex 等）。

当前目标是验证以下联调链路是否打通：

1. `EtcdClient` 调用 -> RPC -> `EtcdNode`。
2. `EtcdNode` 事件循环 -> `RaftNode` propose/step。
3. `RaftNode` 输出 `RaftReady` -> `EtcdNode` 持久化 / apply / 发送 Raft RPC。
4. 命令结果通过 `EtcdCommandRegistry` 回传给 RPC 调用线程。
5. 快照触发、快照安装、崩溃恢复、apply 边界恢复。

---

## 2. 当前骨架核心组件与职责

### 2.1 客户端与 RPC 信封

- `EtcdClient`
  - 面向临时 KV 的客户端入口（put/get/delete/listKeys）。
  - 写请求和线性一致读默认走 Leader 路由。
  - 非线性一致读可直接请求当前节点（`linearizableRead=false`）。

- `EtcdRpcResponse<T>` + `EtcdResponseHeader`
  - 统一响应信封。
  - `header` 表达 success / notLeader / leaderId / message。
  - `body` 承载具体业务响应对象（`PutResponse`、`GetResponse` 等）。

### 2.2 Etcd 层

- `EtcdNode`
  - Etcd 层主协调者。
  - 负责两条输入流：
    1. 用户 RPC 请求 -> `EtcdEvent`。
    2. Raft 输出副作用 -> `RaftReady`。
  - 负责持久化 `RaftPersistentState`（HardState + Snapshot + Entries + lastApplied）。
  - 负责临时 KV 状态机 `stateMachineKvMap` 的 apply 和快照恢复。

- `EtcdEvent` / `EtcdEventCodec`
  - 仅 JVM 内部事件，不进网络、不落盘。
  - data 直接携带 `PutRequest` / `GetRequest` 等对象，避免额外套壳。

- `EtcdCommand` / `EtcdCommandCodec`
  - Etcd 层进入 Raft 日志复制边界的命令信封。
  - 只有在进入 Raft 前才整体序列化为 `byte[]`。

- `EtcdCommandRegistry`
  - 连接 propose 阶段与 apply 阶段。
  - 先按 `commandId` 注册，再在 propose accepted 后绑定 `logIndex`。
  - apply 时按 `logIndex + commandId` 唤醒等待方，避免旧 Leader 日志覆盖导致误唤醒。

### 2.3 Raft 层

- `RaftNode`
  - Raft 协议状态机核心（term/role/log/commitIndex/lastApplied）。
  - 只在 `raft-event-loop` 单线程内修改 Raft 核心状态。
  - 不直接做持久化、不直接发网络、不直接 apply 业务状态机。

- `RaftEvent` / `RaftEventCodec`
  - Raft 内部事件信封（PROPOSE、REQUEST_VOTE、APPEND_ENTRIES、INSTALL_SNAPSHOT、ADVANCE、CREATE_SNAPSHOT）。
  - 当前仅 JVM 内部流转，data 直接保存对象。

- `RaftReady`
  - Raft 层输出给 Etcd 层的“待执行副作用批次”：
  - `hardStateToPersist`
  - `entriesToPersist`
  - `snapshotToPersist`
  - `snapshotToApply`
  - `messagesToSend`
  - `committedEntries`
  - `snapshotCreateRequested`

- `RaftLogState`
  - 维护快照边界后的日志、冲突截断、日志匹配、压缩边界。

---

## 3. 用户请求主链路（EtcdClient -> EtcdNode -> RaftNode）

## 3.1 PUT/DELETE（写请求）

1. `EtcdClient.put/delete` 调用 RPC 方法到 `EtcdNode`。
2. `EtcdNode` RPC handler 只做一件事：包装为 `EtcdEvent` 入队，并等待结果。
3. `etcd-event-loop` 取到事件后：
   - 构造 `EtcdCommand(type, commandId, request)`。
   - 在 `EtcdCommandRegistry` 注册等待 future。
   - 命令整体序列化后调用 `RaftNode.submitRaftProposeEvent(...)`。
4. `RaftNode` 在 `raft-event-loop` 中处理 PROPOSE：
   - 若当前不是 Leader：返回 `accepted=false` + leaderId。
   - 若是 Leader：追加日志，返回 `accepted=true` + logIndex。
5. propose accepted 后，`EtcdNode` 将 `commandId` 绑定到 `logIndex`。
6. 当日志 committed 后，`RaftNode` 产出 `RaftApplyMessage` 到 `RaftReady.committedEntries`。
7. `EtcdNode` 消费 `RaftReady`，按顺序 apply 命令到临时 KV 状态机。
8. `EtcdNode` 调用 `EtcdCommandRegistry.complete(logIndex, commandId, result)`。
9. 等待中的 RPC future 被唤醒，返回 `EtcdRpcResponse` 给客户端。

## 3.2 GET/LIST_KEYS（读请求）

- `linearizableRead=true`（默认）
  - 路径与写请求一致：进入 Raft 日志顺序流后再返回。
  - 验证“读也经过共识”的临时联调语义。

- `linearizableRead=false`
  - 不进入 Raft。
  - 在 `etcd-event-loop` 直接读取本地 `stateMachineKvMap` 返回。
  - 只保证本地视图，不保证线性一致。

---

## 4. Raft RPC 消息链路（节点间）

## 4.1 入站：RPC -> EtcdNode -> RaftNode

远端节点通过 RPC 调用本节点的：

- `handleRaftRpcRequestVoteRequest/Response`
- `handleRaftRpcAppendEntriesRequest/Response`
- `handleRaftRpcInstallSnapshotRequest/Response`

这些 handler **只做转发**：调用 `RaftNode.submitXXXEvent(...)`，不在 RPC 线程中直接改 Raft 状态。

## 4.2 出站：RaftNode -> RaftReady.messagesToSend -> RpcClient

1. `RaftNode` 在协议推进中产生 `RaftRpcMessage`（含 type/targetNodeId/data）。
2. `RaftReady` 输出 `messagesToSend`。
3. `EtcdNode` 按 `targetNodeId` 通过 `NodeEndpointRegistry` 找 endpoint。
4. 反序列化 data 为具体 RPC body，并调用 `raftRpcClient.send(...)`。
5. 发送失败采用 best-effort：不阻断当前 Ready 生命周期。

---

## 5. Ready / Advance 生命周期（Raft 与 Etcd 的关键边界）

1. `RaftNode` 处理事件后形成 pending 副作用。
2. 若当前没有等待中的 Ready（`waitingReadyAdvance=false`），发布一个 `RaftReady` 到单槽队列。
3. `EtcdNode` 取出 `RaftReady` 后按顺序处理：
   1. 持久化 Raft 状态（HardState/Entries/Snapshot）。
   2. apply `snapshotToApply`（若有）。
   3. apply `committedEntries`。
   4. 若请求创建快照，提交 `submitRaftCreateSnapshotEvent`。
   5. 发送 `messagesToSend`。
4. 处理完成后，`EtcdNode` 提交 `submitRaftAdvanceEvent(ready)`。
5. `RaftNode.advance(ready)` 清理本轮 pending，允许发布下一轮 Ready。

这个机制确保：

- Raft 核心状态推进与上层副作用执行解耦。
- 同一时刻只有一个待确认的 Ready，避免并发交叉处理破坏顺序。

---

## 6. 快照流程

## 6.1 快照触发

1. `RaftNode` 每次收集 committed entries 时累计 `committedLogCountSinceSnapshot`。
2. 达到 `snapshotTriggerLogCount` 后，在下一轮 `RaftReady` 中设置 `snapshotCreateRequested=true`。

## 6.2 快照创建

1. `EtcdNode` 处理该 Ready 时，深拷贝当前状态机 Map。
2. 序列化为 `stateMachineSnapshotData`。
3. 调用 `RaftNode.submitRaftCreateSnapshotEvent(lastAppliedIndex, stateMachineSnapshotData)`。
4. `RaftNode` 创建 `RaftSnapshot(lastIncludedIndex,lastIncludedTerm,stateMachineData)`，压缩日志边界。
5. 快照通过后续 Ready 的 `snapshotToPersist` 返回 EtcdNode 持久化。

## 6.3 快照安装（Follower）

1. Leader 发送 `InstallSnapshotRequest`。
2. Follower `RaftNode.step(installSnapshot)` 更新日志边界并产出：
   - `snapshotToPersist`
   - `snapshotToApply`
3. `EtcdNode` 先持久化快照，再用 `snapshotToApply.stateMachineData` 覆盖恢复状态机。
4. 再继续 apply 快照后 committed 的日志。

---

## 7. 崩溃恢复流程

当前阶段使用单 key 聚合持久化：

- group: `raft`
- key: `persistent-state`
- value: `RaftPersistentState`

包含：

- `hardState`
- `snapshot`
- `entries`（snapshot 边界之后）
- `lastAppliedRaftLogIndex`

启动恢复步骤：

1. 读取并反序列化 `RaftPersistentState`。
2. 恢复 `RaftNode`：
   - `restoreHardState(...)`
   - `restoreFromSnapshot(...)`
   - `restoreLogEntries(...)`
3. 恢复状态机：
   - 先用 `snapshot.stateMachineData` 重建 KV Map。
   - 再重放 `entries` 中 `index <= lastAppliedRaftLogIndex` 的日志。
4. 完成后再启动事件循环。

这样保证重启后：

- Raft 任期/投票信息连续。
- 日志边界与快照边界一致。
- 状态机恢复到崩溃前已 apply 的位置。

---

## 8. apply 语义与结果回传

1. `RaftNode` 输出 `RaftApplyMessage(logIndex, commandData)`。
2. `EtcdNode` decode 成 `EtcdCommand`。
3. 在临时 KV 状态机执行具体命令（PUT/DELETE/GET/LIST_KEYS）。
4. 推进并持久化 `lastAppliedRaftLogIndex`。
5. 通过 `EtcdCommandRegistry.complete(logIndex, commandId, result)` 唤醒请求。
6. RPC 线程拿到 `EtcdCommandApplyResult`，转换为 `EtcdRpcResponse`。

关键点：

- 回传时同时使用 `logIndex + commandId`，避免旧 Leader 日志冲突误唤醒。
- apply 顺序严格跟随 committed 顺序。

---

## 9. 当前骨架阶段结论（仅临时 KV）

当前实现已经形成“可验证联调闭环”的最小骨架：

1. 客户端请求能贯通到 Raft，并在 apply 后回包。
2. Raft RPC 请求/响应具备独立入口并纳入事件循环。
3. Ready/Advance 边界明确，副作用执行顺序清晰。
4. 快照触发、快照安装、重启恢复均有实现路径。
5. 代码层已去掉多余事件 data 套壳（如 propose/advance），保持主路径精简。

因此它适合作为“临时 KV 骨架”验证 Raft/Etcd/RPC 联调正确性的当前阶段基础。

