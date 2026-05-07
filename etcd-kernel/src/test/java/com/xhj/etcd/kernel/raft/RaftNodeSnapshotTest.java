package com.xhj.etcd.kernel.raft;

import com.xhj.etcd.kernel.raft.core.RaftConfig;
import com.xhj.etcd.kernel.raft.core.RaftNode;
import com.xhj.etcd.kernel.raft.core.RaftReady;
import com.xhj.etcd.kernel.raft.log.RaftLogState;
import com.xhj.etcd.kernel.raft.raftrpc.AppendEntriesRequest;
import com.xhj.etcd.kernel.raft.raftrpc.InstallSnapshotRequest;
import org.junit.Test;

import java.util.ArrayList;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * RaftNodeSnapshotTest
 *
 * @author XJks
 * @description RaftNode 快照创建与安装路径测试。
 */
public class RaftNodeSnapshotTest {

    @Test
    public void shouldInstallSnapshotFromLeader() {
        RaftNode node = createNode("n1", "n2");
        BlockingQueue<RaftReady> queue = new ArrayBlockingQueue<RaftReady>(1);
        node.startRaftEventLoop(queue);

        AppendEntriesRequest becomeFollower = new AppendEntriesRequest();
        becomeFollower.setTerm(1);
        becomeFollower.setLeaderId("n2");
        becomeFollower.setPrevLogIndex(0);
        becomeFollower.setPrevLogTerm(0);
        becomeFollower.setEntries(new ArrayList<>());
        becomeFollower.setLeaderCommit(0);
        node.step(becomeFollower);

        InstallSnapshotRequest snapshot = new InstallSnapshotRequest();
        snapshot.setTerm(1);
        snapshot.setLeaderId("n2");
        snapshot.setLastIncludedIndex(5);
        snapshot.setLastIncludedTerm(1);
        snapshot.setLeaderCommit(5);
        snapshot.setSnapshotData(new byte[]{1, 2, 3});
        node.step(snapshot);

        assertEquals(5L, node.getRaftLogState().getLastIncludedIndex());
        assertNotNull(node.getLatestSnapshot());
        assertArrayEquals(new byte[]{1, 2, 3}, node.getLatestSnapshot().getStateMachineData());
    }

    private RaftNode createNode(String nodeId, String... peers) {
        RaftConfig config = new RaftConfig();
        config.setElectionTimeoutTicks(10);
        config.setHeartbeatTimeoutTicks(3);
        for (String peer : peers) {
            config.getPeerNodeIds().add(peer);
        }
        return new RaftNode(nodeId, config, new RaftLogState());
    }
}
