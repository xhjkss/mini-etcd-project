package com.xhj.etcd.kernel.raft;

import com.xhj.etcd.kernel.raft.core.RaftConfig;
import com.xhj.etcd.kernel.raft.core.RaftNode;
import com.xhj.etcd.kernel.raft.log.RaftLogState;
import com.xhj.etcd.kernel.raft.raftrpc.AppendEntriesRequest;
import com.xhj.etcd.kernel.raft.raftrpc.AppendEntriesResponse;
import org.junit.Test;

import java.util.ArrayList;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import static org.junit.Assert.assertEquals;

/**
 * RaftNodeReplicationTest
 *
 * @author XJks
 * @description RaftNode 日志复制与提交推进测试。
 */
public class RaftNodeReplicationTest {

    @Test
    public void shouldKeepCommitIndexWhenOnlyLeaderHasLog() {
        RaftNode node = createLeaderNode("n1", "n2", "n3");
        node.propose(new byte[]{1});

        assertEquals(0L, node.getCommitIndex());
    }

    @Test
    public void shouldAdvanceCommitIndexWhenMajorityAck() {
        RaftNode node = createLeaderNode("n1", "n2", "n3");
        node.propose(new byte[]{1});

        AppendEntriesResponse response = new AppendEntriesResponse();
        response.setTerm(1);
        response.setFollowerId("n2");
        response.setSuccess(true);
        response.setMatchIndex(1);
        node.step(response);

        assertEquals(1L, node.getCommitIndex());
    }

    @Test
    public void shouldRejectAppendEntriesWithStaleTerm() {
        RaftNode node = createFollowerNode("n1", "n2");

        AppendEntriesRequest request = new AppendEntriesRequest();
        request.setTerm(0);
        request.setLeaderId("n2");
        request.setPrevLogIndex(0);
        request.setPrevLogTerm(0);
        request.setEntries(new ArrayList<>());
        request.setLeaderCommit(0);
        node.step(request);

        assertEquals(1L, node.getCurrentTerm());
    }

    private RaftNode createLeaderNode(String nodeId, String... peers) {
        RaftNode node = createNode(nodeId, peers);
        node.becomeLeaderForTest();
        return node;
    }

    private RaftNode createFollowerNode(String nodeId, String... peers) {
        RaftNode node = createNode(nodeId, peers);
        AppendEntriesRequest becomeFollower = new AppendEntriesRequest();
        becomeFollower.setTerm(1);
        becomeFollower.setLeaderId(peers[0]);
        becomeFollower.setPrevLogIndex(0);
        becomeFollower.setPrevLogTerm(0);
        becomeFollower.setEntries(new ArrayList<>());
        becomeFollower.setLeaderCommit(0);
        node.step(becomeFollower);
        return node;
    }

    private RaftNode createNode(String nodeId, String... peers) {
        RaftConfig config = new RaftConfig();
        config.setElectionTimeoutTicks(10);
        config.setHeartbeatTimeoutTicks(3);
        for (String peer : peers) {
            config.getPeerNodeIds().add(peer);
        }
        RaftNode node = new RaftNode(nodeId, config, new RaftLogState());
        BlockingQueue<com.xhj.etcd.kernel.raft.core.RaftReady> queue = new ArrayBlockingQueue<com.xhj.etcd.kernel.raft.core.RaftReady>(1);
        node.startRaftEventLoop(queue);
        return node;
    }
}
