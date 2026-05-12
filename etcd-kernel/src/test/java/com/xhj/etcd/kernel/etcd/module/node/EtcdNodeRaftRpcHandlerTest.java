package com.xhj.etcd.kernel.etcd.module.node;

import com.xhj.etcd.kernel.etcd.node.EtcdNode;

import com.xhj.etcd.kernel.raft.core.RaftConfig;
import com.xhj.etcd.kernel.raft.core.RaftRoleType;
import com.xhj.etcd.kernel.raft.raftrpc.AppendEntriesRequest;
import com.xhj.etcd.kernel.raft.raftrpc.AppendEntriesResponse;
import com.xhj.etcd.kernel.raft.raftrpc.InstallSnapshotRequest;
import com.xhj.etcd.kernel.raft.raftrpc.RequestVoteRequest;
import com.xhj.etcd.storage.memory.MemoryStorage;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.concurrent.Callable;

import static org.junit.Assert.assertEquals;

/**
 * EtcdNodeRaftRpcHandlerTest
 *
 * @author XJks
 * @description EtcdNode 的 Raft RPC handler 语义测试，验证路由后的状态推进。
 */
public class EtcdNodeRaftRpcHandlerTest {

    private EtcdNode node;

    @Before
    public void setUp() {
        RaftConfig config = new RaftConfig();
        config.setElectionTimeoutTicks(10);
        config.setHeartbeatTimeoutTicks(3);
        config.getPeerNodeIds().add("n2");
        node = new EtcdNode("n1", config, new MemoryStorage());
        node.start();
    }

    @After
    public void tearDown() {
        if (node != null) {
            node.stop();
        }
    }

    @Test
    public void shouldRouteRequestVoteRequestToRaftNode() throws Exception {
        RequestVoteRequest request = new RequestVoteRequest();
        request.setTerm(2);
        request.setCandidateId("n2");
        request.setLastLogIndex(0);
        request.setLastLogTerm(0);

        node.handleRaftRpcRequestVoteRequest(request);

        awaitTrue(new Callable<Boolean>() {
            @Override
            public Boolean call() {
                return node.getRaftNode().getCurrentTerm() == 2L && "n2".equals(node.getRaftNode().getVotedFor());
            }
        }, 3000L, "request vote request is not routed to raft node");
    }

    @Test
    public void shouldRouteAppendEntriesRequestToRaftNode() throws Exception {
        AppendEntriesRequest request = new AppendEntriesRequest();
        request.setTerm(3);
        request.setLeaderId("n2");
        request.setPrevLogIndex(0);
        request.setPrevLogTerm(0);
        request.setEntries(new ArrayList<>());
        request.setLeaderCommit(0);

        node.handleRaftRpcAppendEntriesRequest(request);

        awaitTrue(new Callable<Boolean>() {
            @Override
            public Boolean call() {
                return node.getRaftNode().getCurrentTerm() == 3L && "n2".equals(node.getRaftNode().getLeaderId());
            }
        }, 3000L, "append entries request is not routed to raft node");
    }

    @Test
    public void shouldRouteInstallSnapshotRequestToRaftNode() throws Exception {
        InstallSnapshotRequest request = new InstallSnapshotRequest();
        request.setTerm(4);
        request.setLeaderId("n2");
        request.setLastIncludedIndex(5);
        request.setLastIncludedTerm(4);
        request.setLeaderCommit(5);
        request.setSnapshotData(new byte[]{1, 2, 3});

        node.handleRaftRpcInstallSnapshotRequest(request);

        awaitTrue(new Callable<Boolean>() {
            @Override
            public Boolean call() {
                return node.getRaftNode().getCurrentTerm() == 4L
                        && node.getRaftNode().getRaftLogState().getLastIncludedIndex() == 5L;
            }
        }, 3000L, "install snapshot request is not routed to raft node");
    }

    @Test
    public void shouldRouteAppendEntriesResponseAndDemoteLeaderOnHigherTerm() throws Exception {
        node.getRaftNode().becomeLeaderForTest();
        assertEquals(RaftRoleType.LEADER, node.getRaftNode().getRole());

        AppendEntriesResponse response = new AppendEntriesResponse();
        response.setTerm(6);
        response.setFollowerId("n2");
        response.setSuccess(false);
        response.setMatchIndex(0);

        node.handleRaftRpcAppendEntriesResponse(response);

        awaitTrue(new Callable<Boolean>() {
            @Override
            public Boolean call() {
                return node.getRaftNode().getRole() == RaftRoleType.FOLLOWER && node.getRaftNode().getCurrentTerm() == 6L;
            }
        }, 3000L, "append entries response is not routed to raft node");
    }

    private void awaitTrue(Callable<Boolean> predicate, long timeoutMillis, String message) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (System.currentTimeMillis() < deadline) {
            if (Boolean.TRUE.equals(predicate.call())) {
                return;
            }
            Thread.sleep(20L);
        }
        throw new AssertionError(message);
    }
}
