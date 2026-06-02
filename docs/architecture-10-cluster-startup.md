# 集群启动与 Console 使用指南

## 1. 文档范围

本文面向第一次运行项目的同学，目标是：

1. 理解 `etcd-console/scripts` 当前脚本模型。
2. 用脚本拉起 3 节点 mini-etcd 集群。
3. 启动 Console 并连接节点完成最小闭环验证。
4. 清楚 runtime 目录里每类文件的作用。

## 2. 先准备好这些环境

1. JDK 8+（本项目编译目标是 Java 8）。
2. Maven（命令行可执行 `mvn -v`）。
3. 可用端口：
- 节点默认 `2379/2380/2381`
- Console 默认 `8080`

## 3. 脚本清单（当前版本）

目录：`etcd-console/scripts`

Windows（CMD）：

1. `start-cluster.cmd`
2. `clean-runtime.cmd`

Linux/macOS（Shell）：

1. `start-cluster.sh`
2. `clean-runtime.sh`

停止方式见第 6 节和第 7 节。

## 4. runtime 目录是什么

脚本会在 `etcd-console/scripts/runtime` 下生成运行时文件。

典型结构：

1. `runtime/profiles/<profile>/data/`
- 节点数据目录（`FileStorage` 持久化数据）。

2. `runtime/profiles/<profile>/logs/`
- 节点日志（每个节点一个日志文件）。

3. `runtime/profiles/<profile>/state/`
- 进程状态文件（`cluster.pids`、`n1.pid` 等）。

一个 `profile` 就是一套独立运行环境，便于并行实验。

## 5. start-cluster 参数说明

公共参数（CMD/SH 一致）：

1. `--clusterSize`
- 节点数，默认 `3`。

2. `--host`
- 节点主机地址，默认 `127.0.0.1`。

3. `--basePort`
- 起始端口，默认 `2379`。
- 例如 3 节点时使用 `2379/2380/2381`。

4. `--profile`
- 运行环境名，默认 `default`。

5. `--dataRoot`
- 自定义数据目录根（可选）。

6. `--logDir`
- 自定义日志目录根（可选）。

7. `--electionTimeoutTicks`
- 选举超时 tick，默认 `10`。

8. `--heartbeatTimeoutTicks`
- 心跳超时 tick，默认 `3`。

9. `--snapshotTriggerLogCount`
- 快照触发阈值，默认 `50`。

10. `--noHold`
- 是否启动后不占用当前终端（默认 `false`）。

11. `--noPause`
- Windows 批处理防双击窗口关闭参数；Shell 脚本兼容接收该参数。

## 6. Windows CMD 使用方式

先进入脚本目录：

```cmd
cd /d D:\IDEA_Code_Store_Position\mini-etcd-project\etcd-console\scripts
```

默认启动 3 节点：

```cmd
start-cluster.cmd
```

自定义 profile 与端口：

```cmd
start-cluster.cmd --profile=e2e --clusterSize=3 --basePort=2500
```

启动后窗口会保持（用于托管集群生命周期），停止方式：

1. 在窗口中按 `Q`，脚本会停止所有节点并退出。
2. 或直接关闭窗口，脚本同样会清理对应节点进程。

清理 runtime：

```cmd
clean-runtime.cmd
clean-runtime.cmd --profile=e2e
```

## 7. Linux/macOS 使用方式

进入脚本目录并赋权：

```bash
cd /path/to/mini-etcd-project/etcd-console/scripts
chmod +x start-cluster.sh clean-runtime.sh
```

默认启动：

```bash
./start-cluster.sh
```

自定义参数：

```bash
./start-cluster.sh --profile=e2e --clusterSize=3 --basePort=2500
```

停止方式：

1. 在当前终端按 `Ctrl + C`，脚本会停止所有节点后退出。

清理 runtime：

```bash
./clean-runtime.sh
./clean-runtime.sh --profile=e2e
```

## 8. start-cluster 内部做了什么

可按下面顺序理解：

1. 校验参数、命令依赖与端口范围。
2. 检查目标端口是否已被占用。
3. 执行 `mvn -pl etcd-kernel -am -DskipTests package`。
4. 组装 `peerEndpoints`（`n1@host:port,n2@host:port,...`）。
5. 启动每个节点 `MiniEtcdNodeLauncher`。
6. 等待各节点写出 pid 文件并监听端口，超时则打印节点日志尾部、回滚并退出。
7. 启动成功后保持终端，托管集群生命周期。

说明：`start-cluster` 只清理当前 profile 的旧 pid 记录，不做全局残留进程清理，避免误杀其他 profile 的实验节点。需要全局清理时使用不带 `--profile` 的 `clean-runtime`。

## 9. 启动 Console

在项目根目录执行：

```bash
mvn -pl etcd-console -am spring-boot:run
```

默认访问地址：

1. `http://127.0.0.1:8080`

## 10. Console 连接集群步骤

1. 打开“连接管理”。
2. 依次连接：
- `127.0.0.1:2379`
- `127.0.0.1:2380`
- `127.0.0.1:2381`
3. 连接成功后进入“数据浏览/操作中心/节点状态”页面操作。

说明：

1. 前端只输入 `host + port`。
2. 后端会自动探测节点并填充真实 `nodeId`。

## 11. 最小验收清单

1. 连接 3 个节点成功。
2. 执行一次 `put`，再 `get`/`range` 验证数据可读。
3. 创建 watch，执行 put/delete 观察事件推送。
4. 执行 compact、lease、诊断接口确认有正常响应。
5. 节点状态页可看到 leader/term/revision/hash。

## 12. 常见问题排查

1. 启动报端口占用
- 启动脚本会输出占用端口与进程信息；换 `--basePort`，或按需执行 `clean-runtime`。
- 端口预检会一次性扫描本次集群需要的端口范围，发现占用后直接失败退出，不会继续构建或启动节点。

2. 节点启动失败
- 启动脚本会打印失败节点日志尾部；也可以查看 `runtime/profiles/<profile>/logs/*.log`。

3. Console 无法连接
- 检查节点是否真的启动成功（看启动终端输出）。
- 检查端口是否与 `start-cluster` 参数一致。

4. runtime 目录内容很多
- 这是正常运行产物；实验结束后可用 `clean-runtime` 清理。
