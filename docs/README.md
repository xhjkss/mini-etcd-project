# mini-etcd 文档索引

本文档目录仅描述当前已经实现并对外可读的模块设计与行为语义。

## 文档列表

1. `architecture-01-runtime.md`
- EtcdClient、RPC、EtcdNode、RaftNode 的运行时链路与网络交互说明。

2. `architecture-02-mvcc.md`
- MVCC KV 状态机的数据结构、方法链、版本语义与快照恢复说明。

3. `architecture-03-txn.md`
- Txn 原子事务模型、校验逻辑、执行方法链与一致性语义说明。

4. `architecture-04-compact.md`
- Compact 历史压缩模型、边界错误语义、快照恢复与网络一致性说明。

5. `architecture-05-lease.md`
- Lease 租约模型、过期撤销执行链路、KV 绑定关系与恢复语义说明。

6. `architecture-06-watch.md`
- Watch 长连接订阅/取消、Leader 路由、历史回放与 compact 边界取消语义说明。

7. `architecture-07-diagnostic.md`
- KvStateHash 与 NodeStatus 诊断读、字段含义、事件链路与节点运行态说明。