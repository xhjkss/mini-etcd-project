package com.xhj.etcd.kernel.raft;

import com.xhj.etcd.kernel.raft.core.RaftConfig;
import com.xhj.etcd.kernel.raft.core.RaftNode;
import com.xhj.etcd.kernel.raft.core.RaftReady;
import com.xhj.etcd.kernel.raft.log.RaftLogState;
import org.junit.Test;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * RaftNodeReadyAdvanceTest
 *
 * @author XJks
 * @description RaftNode Ready 与 Advance 生命周期测试。
 */
public class RaftNodeReadyAdvanceTest {

    @Test
    public void shouldClearReadyAfterAdvance() {
        RaftNode node = createNode("n1", "n2");
        BlockingQueue<RaftReady> queue = new ArrayBlockingQueue<RaftReady>(1);
        node.startRaftEventLoop(queue);
        node.becomeLeaderForTest();

        node.propose(new byte[]{1});

        assertTrue(node.hasReady());
        RaftReady ready = node.ready();
        node.advance(ready);

        assertFalse(node.hasReady());
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
