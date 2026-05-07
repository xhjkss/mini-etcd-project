package com.xhj.etcd.kernel.raft;

import com.xhj.etcd.kernel.raft.core.RaftConfig;
import com.xhj.etcd.kernel.raft.core.RaftNode;
import com.xhj.etcd.kernel.raft.core.RaftReady;
import com.xhj.etcd.kernel.raft.core.RaftRoleType;
import com.xhj.etcd.kernel.raft.log.RaftLogState;
import com.xhj.etcd.kernel.raft.raftrpc.RequestVoteRequest;
import org.junit.Test;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import static org.junit.Assert.assertEquals;

/**
 * RaftNodeElectionTest
 *
 * @author XJks
 * @description RaftNode 选举与投票关键路径测试。
 */
public class RaftNodeElectionTest {

    @Test
    public void shouldRejectStaleTermVoteRequest() {
        RaftNode node = createNode("n1", "n2");
        BlockingQueue<RaftReady> queue = new ArrayBlockingQueue<RaftReady>(1);
        node.startRaftEventLoop(queue);
        node.becomeLeaderForTest();

        RequestVoteRequest stale = new RequestVoteRequest();
        stale.setTerm(0);
        stale.setCandidateId("n2");
        stale.setLastLogIndex(0);
        stale.setLastLogTerm(0);
        node.step(stale);

        assertEquals(RaftRoleType.LEADER, node.getRole());
        assertEquals(1L, node.getCurrentTerm());
    }

    @Test
    public void shouldGrantVoteWhenCandidateLogIsUpToDate() {
        RaftNode node = createNode("n1", "n2", "n3");
        BlockingQueue<RaftReady> queue = new ArrayBlockingQueue<RaftReady>(1);
        node.startRaftEventLoop(queue);

        RequestVoteRequest request = new RequestVoteRequest();
        request.setTerm(2);
        request.setCandidateId("n2");
        request.setLastLogIndex(0);
        request.setLastLogTerm(0);
        node.step(request);

        assertEquals(RaftRoleType.FOLLOWER, node.getRole());
        assertEquals(2L, node.getCurrentTerm());
        assertEquals("n2", node.getVotedFor());
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
