package com.xhj.etcd.kernel.testsupport.network;

import java.util.List;

/**
 * DistributedClusterHarness
 *
 * @author XJks
 * @description Raft / Etcd 真实网络测试的集群控制通用协议。
 */
public interface DistributedClusterHarness {

    /**
     * 启动指定节点数量的集群。
     */
    void startCluster(int nodeCount) throws Exception;

    /**
     * 停止整个集群。
     */
    void stopAll();

    /**
     * 获取集群节点 ID 列表（建议返回稳定顺序）。
     */
    List<String> getNodeIds();

    /**
     * 停止指定节点。
     */
    void stopNode(String nodeId);

    /**
     * 重启指定节点。
     */
    void restartNode(String nodeId) throws Exception;

    /**
     * 等待 leader 选举成功并返回 leaderId。
     */
    String awaitLeaderElected(long timeoutMillis) throws Exception;

    /**
     * 排除指定节点后等待 leader 选举成功并返回 leaderId。
     */
    String awaitLeaderElectedExcluding(String excludedNodeId, long timeoutMillis) throws Exception;

    /**
     * 从集群中选择一个 follower 节点。
     */
    String chooseFollowerId(String leaderId);

    /**
     * 停止指定数量的 follower 并返回实际停止的节点 ID 列表。
     */
    List<String> stopFollowers(String leaderId, int maxCount);

    /**
     * 双向隔离两个分区的节点通信。
     */
    void isolateBidirectional(List<String> leftNodeIds, List<String> rightNodeIds) throws Exception;

    /**
     * 恢复全部网络隔离。
     */
    void healAllNetworkIsolation();

    /**
     * 配置快照触发阈值（用于快照相关测试）。
     */
    void setSnapshotTriggerLogCount(int snapshotTriggerLogCount);
}
