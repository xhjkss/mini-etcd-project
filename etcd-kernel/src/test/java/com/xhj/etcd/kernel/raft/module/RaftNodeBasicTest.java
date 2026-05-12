package com.xhj.etcd.kernel.raft.module;

import com.xhj.etcd.kernel.raft.core.*;
import com.xhj.etcd.kernel.raft.log.RaftLogEntry;
import com.xhj.etcd.kernel.raft.log.RaftLogState;
import com.xhj.etcd.kernel.raft.raftrpc.AppendEntriesRequest;
import com.xhj.etcd.kernel.raft.raftrpc.AppendEntriesResponse;
import com.xhj.etcd.kernel.raft.raftrpc.InstallSnapshotRequest;
import com.xhj.etcd.kernel.raft.raftrpc.InstallSnapshotResponse;
import com.xhj.etcd.kernel.raft.raftrpc.RequestVoteRequest;
import org.junit.Test;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/**
 * RaftNodeBasicTest
 *
 * @author XJks
 * @description RaftNode基础功能单元测试。
 */
public class RaftNodeBasicTest {

    private RaftConfig createConfig(String... peerNodeIds) {
        RaftConfig config = new RaftConfig();
        for (String id : peerNodeIds) {
            config.getPeerNodeIds().add(id);
        }
        config.setElectionTimeoutTicks(10);
        config.setHeartbeatTimeoutTicks(3);
        return config;
    }

    @Test
    public void testInitialState() {
        RaftConfig config = createConfig("node2");
        RaftLogState logState = new RaftLogState();
        RaftNode node = new RaftNode("node1", config, logState);

        assertEquals("node1", node.getNodeId());
        assertEquals(RaftRoleType.FOLLOWER, node.getRole());
        assertEquals(0, node.getCurrentTerm());
        assertNull(node.getVotedFor());
        assertNull(node.getLeaderId());
        assertEquals(0, node.getCommitIndex());
        assertEquals(0, node.getLastApplied());
    }

    @Test
    public void testBecomeLeaderForTest() {
        RaftConfig config = createConfig("node2");
        RaftLogState logState = new RaftLogState();
        RaftNode node = new RaftNode("node1", config, logState);

        node.becomeLeaderForTest();

        assertEquals(RaftRoleType.LEADER, node.getRole());
        assertEquals(1, node.getCurrentTerm());
        assertEquals("node1", node.getLeaderId());
        assertEquals("node1", node.getVotedFor());
    }

    @Test
    public void testRestoreHardState() {
        RaftConfig config = createConfig("node2");
        RaftLogState logState = new RaftLogState();
        RaftNode node = new RaftNode("node1", config, logState);

        RaftHardState hardState = new RaftHardState();
        hardState.setCurrentTerm(5);
        hardState.setVotedFor("node2");
        node.restoreHardState(hardState);

        assertEquals(5, node.getCurrentTerm());
        assertEquals("node2", node.getVotedFor());
    }

    @Test
    public void testProposeOnlyWhenLeader() {
        RaftConfig config = createConfig("node2");
        RaftLogState logState = new RaftLogState();
        RaftNode node = new RaftNode("node1", config, logState);

        BlockingQueue<RaftReady> readyQueue = new ArrayBlockingQueue<>(1);
        node.startRaftEventLoop(readyQueue);

        RaftProposeResult result = node.propose(new byte[]{1});
        assertFalse(result.isAccepted());
    }

    @Test
    public void testProposeAcceptedWhenLeader() {
        RaftConfig config = createConfig("node2");
        RaftLogState logState = new RaftLogState();
        RaftNode node = new RaftNode("node1", config, logState);

        BlockingQueue<RaftReady> readyQueue = new ArrayBlockingQueue<>(1);
        node.startRaftEventLoop(readyQueue);
        node.becomeLeaderForTest();

        RaftProposeResult result = node.propose(new byte[]{1});
        assertTrue(result.isAccepted());
        assertEquals(1, result.getLogIndex());
        assertEquals(1, result.getCurrentTerm());
    }

    @Test
    public void testReadyContainsHardStateWhenLeader() {
        RaftConfig config = createConfig("node2");
        RaftLogState logState = new RaftLogState();
        RaftNode node = new RaftNode("node1", config, logState);

        BlockingQueue<RaftReady> readyQueue = new ArrayBlockingQueue<>(1);
        node.startRaftEventLoop(readyQueue);
        node.becomeLeaderForTest();

        assertTrue(node.hasReady());

        RaftReady ready = node.ready();
        assertNotNull(ready.getHardStateToPersist());
        assertEquals(1, ready.getHardStateToPersist().getCurrentTerm());
    }

    @Test
    public void testRequestVoteFromHigherTerm() {
        RaftConfig config = createConfig("node2");
        RaftLogState logState = new RaftLogState();
        RaftNode node = new RaftNode("node1", config, logState);

        BlockingQueue<RaftReady> readyQueue = new ArrayBlockingQueue<>(1);
        node.startRaftEventLoop(readyQueue);
        node.becomeLeaderForTest();
        assertEquals(1, node.getCurrentTerm());

        RequestVoteRequest request = new RequestVoteRequest();
        request.setTerm(5);
        request.setCandidateId("node2");
        request.setLastLogIndex(10);
        request.setLastLogTerm(3);
        node.step(request);

        assertEquals(RaftRoleType.FOLLOWER, node.getRole());
        assertEquals(5, node.getCurrentTerm());
        assertEquals("node2", node.getVotedFor());
    }

    @Test
    public void testAppendEntriesFromHigherTerm() {
        RaftConfig config = createConfig("node2");
        RaftLogState logState = new RaftLogState();
        RaftNode node = new RaftNode("node1", config, logState);

        BlockingQueue<RaftReady> readyQueue = new ArrayBlockingQueue<>(1);
        node.startRaftEventLoop(readyQueue);
        node.becomeLeaderForTest();
        assertEquals(1, node.getCurrentTerm());

        AppendEntriesRequest request = new AppendEntriesRequest();
        request.setTerm(5);
        request.setLeaderId("node2");
        request.setPrevLogIndex(0);
        request.setPrevLogTerm(0);
        request.setEntries(java.util.Collections.emptyList());
        request.setLeaderCommit(0);
        node.step(request);

        assertEquals(RaftRoleType.FOLLOWER, node.getRole());
        assertEquals(5, node.getCurrentTerm());
        assertEquals("node2", node.getLeaderId());
    }

    @Test
    public void testAppendEntriesUpdatesLeaderAndResetsElection() {
        RaftConfig config = createConfig("node2");
        RaftLogState logState = new RaftLogState();
        RaftNode node = new RaftNode("node1", config, logState);

        BlockingQueue<RaftReady> readyQueue = new ArrayBlockingQueue<>(1);
        node.startRaftEventLoop(readyQueue);

        AppendEntriesRequest request = new AppendEntriesRequest();
        request.setTerm(1);
        request.setLeaderId("node2");
        request.setPrevLogIndex(0);
        request.setPrevLogTerm(0);
        request.setEntries(java.util.Collections.emptyList());
        request.setLeaderCommit(0);
        node.step(request);

        assertEquals(RaftRoleType.FOLLOWER, node.getRole());
        assertEquals("node2", node.getLeaderId());
    }

    @Test
    public void testLogEntryReplication() {
        RaftConfig config = createConfig("node2");
        RaftLogState logState = new RaftLogState();
        RaftNode node = new RaftNode("node1", config, logState);

        BlockingQueue<RaftReady> readyQueue = new ArrayBlockingQueue<>(1);
        node.startRaftEventLoop(readyQueue);
        node.becomeLeaderForTest();

        node.propose(new byte[]{1});
        node.propose(new byte[]{2});

        assertTrue(node.hasReady());

        RaftReady ready = node.ready();
        assertEquals(2, ready.getEntriesToPersist().size());
    }

    @Test
    public void testCommitIndexAdvancesWithMajority() {
        RaftConfig config = createConfig("node2", "node3");
        RaftLogState logState = new RaftLogState();
        RaftNode node = new RaftNode("node1", config, logState);

        BlockingQueue<RaftReady> readyQueue = new ArrayBlockingQueue<>(1);
        node.startRaftEventLoop(readyQueue);
        node.becomeLeaderForTest();

        node.propose(new byte[]{1});

        AppendEntriesResponse response = new AppendEntriesResponse();
        response.setTerm(1);
        response.setFollowerId("node2");
        response.setSuccess(true);
        response.setMatchIndex(1);
        node.step(response);

        assertEquals(1, node.getCommitIndex());
    }

    @Test
    public void testHasReady() {
        RaftConfig config = createConfig("node2");
        RaftLogState logState = new RaftLogState();
        RaftNode node = new RaftNode("node1", config, logState);

        BlockingQueue<RaftReady> readyQueue = new ArrayBlockingQueue<>(1);
        node.startRaftEventLoop(readyQueue);

        assertFalse(node.hasReady());

        node.becomeLeaderForTest();
        assertTrue(node.hasReady());
    }

    @Test
    public void testCreateSnapshotForTest() {
        RaftConfig config = createConfig("node2");
        RaftLogState logState = new RaftLogState();
        RaftNode node = new RaftNode("node1", config, logState);

        BlockingQueue<RaftReady> readyQueue = new ArrayBlockingQueue<>(1);
        node.startRaftEventLoop(readyQueue);
        node.becomeLeaderForTest();

        // First add some log entries
        node.propose(new byte[]{1});
        node.propose(new byte[]{2});
        node.propose(new byte[]{3});

        // Advance to get entries committed and applied
        AppendEntriesResponse response = new AppendEntriesResponse();
        response.setTerm(1);
        response.setFollowerId("node2");
        response.setSuccess(true);
        response.setMatchIndex(3);
        node.step(response);

        // Now create snapshot at index 3
        node.createSnapshotForTest(3, new byte[]{1, 2, 3});

        assertEquals(3, node.getRaftLogState().getLastIncludedIndex());
        assertNotNull(node.getLatestSnapshot());
    }

    // ==================== 选举失败测试 ====================

    /**
     * 测试单节点集群选举成功。
     *
     * <p>场景：只有自身一个节点，应该立即成为 Leader。</p>
     */
    @Test
    public void testSingleNodeElectionWins() {
        RaftConfig config = createConfig(); // 无 peer
        RaftLogState logState = new RaftLogState();
        RaftNode node = new RaftNode("node1", config, logState);

        BlockingQueue<RaftReady> readyQueue = new ArrayBlockingQueue<>(1);
        node.startRaftEventLoop(readyQueue);

        node.becomeLeaderForTest();

        assertEquals(RaftRoleType.LEADER, node.getRole());
        assertEquals("node1", node.getLeaderId());
    }

    /**
     * 测试收到更高 term 的 RequestVote 后重置 votedFor。
     *
     * <p>场景：已投票给 node2 后，收到更高 term 的 node3 请求，应该投票给 node3。</p>
     */
    @Test
    public void testVotedForResetOnHigherTerm() {
        RaftConfig config = createConfig("node2", "node3");
        RaftLogState logState = new RaftLogState();
        RaftNode node = new RaftNode("node1", config, logState);

        BlockingQueue<RaftReady> readyQueue = new ArrayBlockingQueue<>(1);
        node.startRaftEventLoop(readyQueue);

        // 模拟收到 term=2 的投票请求并投票
        RequestVoteRequest vote1 = new RequestVoteRequest();
        vote1.setTerm(2);
        vote1.setCandidateId("node2");
        vote1.setLastLogIndex(0);
        vote1.setLastLogTerm(0);
        node.step(vote1);

        assertEquals("node2", node.getVotedFor());

        // 收到 term=3 的更高 term 请求，应该重置 votedFor
        RequestVoteRequest vote2 = new RequestVoteRequest();
        vote2.setTerm(3);
        vote2.setCandidateId("node3");
        vote2.setLastLogIndex(0);
        vote2.setLastLogTerm(0);
        node.step(vote2);

        assertEquals("node3", node.getVotedFor());
        assertEquals(3, node.getCurrentTerm());
    }

    // ==================== 日志复制测试 ====================

    /**
     * 测试 Follower 拒绝 prevLogIndex/prevLogTerm 不匹配的日志。
     *
     * <p>场景：Follower 本地日志与 Leader 不一致，应返回 rejectHint。</p>
     */
    @Test
    public void testAppendEntriesRejectsConflictingPrevLog() {
        RaftConfig config = createConfig("node2");
        RaftLogState logState = new RaftLogState();
        RaftNode node = new RaftNode("node1", config, logState);

        BlockingQueue<RaftReady> readyQueue = new ArrayBlockingQueue<>(1);
        node.startRaftEventLoop(readyQueue);

        // 节点成为 Follower
        AppendEntriesRequest request1 = new AppendEntriesRequest();
        request1.setTerm(1);
        request1.setLeaderId("node2");
        request1.setPrevLogIndex(0);
        request1.setPrevLogTerm(0);
        request1.setEntries(new ArrayList<>());
        request1.setLeaderCommit(0);
        node.step(request1);

        // 模拟 Follower 本地有一条旧 term 的日志
        RaftLogEntry oldEntry = new RaftLogEntry();
        oldEntry.setTerm(1);
        oldEntry.setIndex(1);
        oldEntry.setCommandData(new byte[]{1});
        logState.appendNewLocalLogEntry(oldEntry);

        // Leader 发送 prevLogIndex=1, prevLogTerm=2，但本地是 term=1
        AppendEntriesRequest request2 = new AppendEntriesRequest();
        request2.setTerm(2);
        request2.setLeaderId("node2");
        request2.setPrevLogIndex(1);
        request2.setPrevLogTerm(2); // 与本地不匹配
        request2.setEntries(new ArrayList<>());
        request2.setLeaderCommit(0);
        node.step(request2);

        // 验证节点仍为 Follower，且 term 已更新为 2
        assertEquals(RaftRoleType.FOLLOWER, node.getRole());
    }

    /**
     * 测试日志冲突截断后继续追加。
     *
     * <p>场景：Follower 日志有冲突，Leader 发送正确日志后应该追加成功。</p>
     */
    @Test
    public void testConflictTruncationThenAppend() {
        RaftConfig config = createConfig("node2");
        RaftLogState logState = new RaftLogState();
        RaftNode node = new RaftNode("node1", config, logState);

        BlockingQueue<RaftReady> readyQueue = new ArrayBlockingQueue<>(1);
        node.startRaftEventLoop(readyQueue);

        // 先成为 Follower
        AppendEntriesRequest req1 = new AppendEntriesRequest();
        req1.setTerm(1);
        req1.setLeaderId("node2");
        req1.setPrevLogIndex(0);
        req1.setPrevLogTerm(0);
        req1.setEntries(new ArrayList<>());
        req1.setLeaderCommit(0);
        node.step(req1);

        // 模拟 Follower 有冲突的日志（index=1, term=1）
        RaftLogEntry conflictEntry = new RaftLogEntry();
        conflictEntry.setTerm(1);
        conflictEntry.setIndex(1);
        conflictEntry.setCommandData(new byte[]{99});
        logState.appendNewLocalLogEntry(conflictEntry);

        // Leader 发送正确日志：prevLogIndex=0，entries=[term=2, index=1]
        RaftLogEntry correctEntry = new RaftLogEntry();
        correctEntry.setTerm(2);
        correctEntry.setIndex(1);
        correctEntry.setCommandData(new byte[]{1});

        AppendEntriesRequest req2 = new AppendEntriesRequest();
        req2.setTerm(2);
        req2.setLeaderId("node2");
        req2.setPrevLogIndex(0);
        req2.setPrevLogTerm(0);
        List<RaftLogEntry> entries = new ArrayList<>();
        entries.add(correctEntry);
        req2.setEntries(entries);
        req2.setLeaderCommit(0);
        node.step(req2);

        // 验证日志已追加（冲突被截断）
        RaftLogEntry entry = logState.getLogEntryByIndex(1);
        assertNotNull(entry);
        assertEquals(2, entry.getTerm());
        assertArrayEquals(new byte[]{1}, entry.getCommandData());
    }

    /**
     * 测试连续 Propose 多次后单次 Advance。
     *
     * <p>场景：连续 propose 两次后调用一次 advance，应该清理对应的 pending entries。</p>
     */
    @Test
    public void testAdvanceClearsPendingEntries() {
        RaftConfig config = createConfig("node2");
        RaftLogState logState = new RaftLogState();
        RaftNode node = new RaftNode("node1", config, logState);

        BlockingQueue<RaftReady> readyQueue = new ArrayBlockingQueue<>(1);
        node.startRaftEventLoop(readyQueue);
        node.becomeLeaderForTest();

        // 连续 propose 两次
        node.propose(new byte[]{1});
        node.propose(new byte[]{2});

        assertTrue(node.hasReady());
        RaftReady ready1 = node.ready();
        assertEquals(2, ready1.getEntriesToPersist().size());

        // Advance 清理
        node.advance(ready1);

        // pending 应该被清理
        assertFalse(node.hasReady());
    }

    /**
     * 测试 Advance 时清理 pendingHardState。
     *
     * <p>场景：Leader 当选后产生 pendingHardState，Advance 后应该清理。</p>
     */
    @Test
    public void testAdvanceClearsPendingHardState() {
        RaftConfig config = createConfig("node2");
        RaftLogState logState = new RaftLogState();
        RaftNode node = new RaftNode("node1", config, logState);

        BlockingQueue<RaftReady> readyQueue = new ArrayBlockingQueue<>(1);
        node.startRaftEventLoop(readyQueue);
        node.becomeLeaderForTest();

        assertTrue(node.hasReady());
        RaftReady ready = node.ready();
        assertNotNull(ready.getHardStateToPersist());

        // Advance
        node.advance(ready);

        // 再次调用 ready，hardState 应该为 null
        RaftReady ready2 = node.ready();
        assertNull(ready2.getHardStateToPersist());
    }

    // ==================== 快照测试 ====================

    /**
     * 测试 InstallSnapshot 完整流程。
     *
     * <p>场景：Leader 发送 InstallSnapshot，Follower 接收并恢复状态。</p>
     */
    @Test
    public void testInstallSnapshotFlow() {
        RaftConfig config = createConfig("node2");
        RaftLogState logState = new RaftLogState();
        RaftNode node = new RaftNode("node1", config, logState);

        BlockingQueue<RaftReady> readyQueue = new ArrayBlockingQueue<>(1);
        node.startRaftEventLoop(readyQueue);

        // 成为 Follower
        AppendEntriesRequest req1 = new AppendEntriesRequest();
        req1.setTerm(1);
        req1.setLeaderId("node2");
        req1.setPrevLogIndex(0);
        req1.setPrevLogTerm(0);
        req1.setEntries(new ArrayList<>());
        req1.setLeaderCommit(0);
        node.step(req1);

        // 模拟 Leader 发送 InstallSnapshot
        InstallSnapshotRequest snapshotRequest = new InstallSnapshotRequest();
        snapshotRequest.setTerm(1);
        snapshotRequest.setLeaderId("node2");
        snapshotRequest.setLastIncludedIndex(5);
        snapshotRequest.setLastIncludedTerm(1);
        snapshotRequest.setLeaderCommit(5);
        snapshotRequest.setSnapshotData(new byte[]{1, 2, 3, 4, 5});
        node.step(snapshotRequest);

        // 验证快照已安装
        assertEquals(5, node.getRaftLogState().getLastIncludedIndex());
        assertEquals(1, node.getRaftLogState().getLastIncludedTerm());
        assertNotNull(node.getLatestSnapshot());
        assertArrayEquals(new byte[]{1, 2, 3, 4, 5}, node.getLatestSnapshot().getStateMachineData());
    }

    /**
     * 测试 Follower 忽略旧 term 的 InstallSnapshot。
     *
     * <p>场景：收到过时的 snapshot 请求，应该忽略。</p>
     */
    @Test
    public void testInstallSnapshotIgnoresOldTerm() {
        RaftConfig config = createConfig("node2");
        RaftLogState logState = new RaftLogState();
        RaftNode node = new RaftNode("node1", config, logState);

        BlockingQueue<RaftReady> readyQueue = new ArrayBlockingQueue<>(1);
        node.startRaftEventLoop(readyQueue);

        // 成为 Follower，term=5
        AppendEntriesRequest req1 = new AppendEntriesRequest();
        req1.setTerm(5);
        req1.setLeaderId("node2");
        req1.setPrevLogIndex(0);
        req1.setPrevLogTerm(0);
        req1.setEntries(new ArrayList<>());
        req1.setLeaderCommit(0);
        node.step(req1);

        assertEquals(5, node.getCurrentTerm());

        // 收到旧 term 的 InstallSnapshot（term=3）
        InstallSnapshotRequest oldSnapshot = new InstallSnapshotRequest();
        oldSnapshot.setTerm(3);
        oldSnapshot.setLeaderId("node2");
        oldSnapshot.setLastIncludedIndex(5);
        oldSnapshot.setLastIncludedTerm(3);
        oldSnapshot.setLeaderCommit(5);
        oldSnapshot.setSnapshotData(new byte[]{1, 2, 3});
        node.step(oldSnapshot);

        // 快照不应该被安装（term 过时）
        assertEquals(0, node.getRaftLogState().getLastIncludedIndex());
    }

    // ==================== 高 term 处理测试 ====================

    /**
     * 测试收到更高 term 的 AppendEntriesResponse 后降级为 Follower。
     *
     * <p>场景：Leader 收到更高 term 的响应，应该立即变为 Follower。</p>
     */
    @Test
    public void testAppendEntriesResponseHigherTermRevertsToFollower() {
        RaftConfig config = createConfig("node2", "node3");
        RaftLogState logState = new RaftLogState();
        RaftNode node = new RaftNode("node1", config, logState);

        BlockingQueue<RaftReady> readyQueue = new ArrayBlockingQueue<>(1);
        node.startRaftEventLoop(readyQueue);
        node.becomeLeaderForTest();

        assertEquals(RaftRoleType.LEADER, node.getRole());
        assertEquals(1, node.getCurrentTerm());

        // 收到更高 term 的响应
        AppendEntriesResponse response = new AppendEntriesResponse();
        response.setTerm(5); // 更高 term
        response.setFollowerId("node2");
        response.setSuccess(false);
        response.setMatchIndex(0);
        node.step(response);

        // 应该降级为 Follower
        assertEquals(RaftRoleType.FOLLOWER, node.getRole());
        assertEquals(5, node.getCurrentTerm());
    }

    /**
     * 测试收到更高 term 的 InstallSnapshotResponse 后降级为 Follower。
     */
    @Test
    public void testInstallSnapshotResponseHigherTermRevertsToFollower() {
        RaftConfig config = createConfig("node2");
        RaftLogState logState = new RaftLogState();
        RaftNode node = new RaftNode("node1", config, logState);

        BlockingQueue<RaftReady> readyQueue = new ArrayBlockingQueue<>(1);
        node.startRaftEventLoop(readyQueue);
        node.becomeLeaderForTest();

        assertEquals(RaftRoleType.LEADER, node.getRole());

        // 收到更高 term 的响应
        InstallSnapshotResponse response = new InstallSnapshotResponse();
        response.setTerm(5);
        response.setFollowerId("node2");
        response.setSuccess(false);
        response.setLastIncludedIndex(0);
        node.step(response);

        assertEquals(RaftRoleType.FOLLOWER, node.getRole());
        assertEquals(5, node.getCurrentTerm());
    }

    // ==================== nextIndex / matchIndex 测试 ====================

    /**
     * 测试 becomeLeader 时初始化 nextIndexMap 和 matchIndexMap。
     */
    @Test
    public void testBecomeLeaderInitializesMaps() {
        RaftConfig config = createConfig("node2", "node3");
        RaftLogState logState = new RaftLogState();
        RaftNode node = new RaftNode("node1", config, logState);

        BlockingQueue<RaftReady> readyQueue = new ArrayBlockingQueue<>(1);
        node.startRaftEventLoop(readyQueue);
        node.becomeLeaderForTest();

        // 验证成为 leader 后的状态
        assertEquals(RaftRoleType.LEADER, node.getRole());
        assertEquals("node1", node.getLeaderId());
    }

    // ==================== commitIndex 推进测试 ====================

    /**
     * 测试 CommitIndex 在收到多数派确认后推进。
     *
     * <p>场景：3 节点集群，Leader 提出日志后收到 node2 确认，此时 matchCount=2（leader+node2），满足多数派，commitIndex 推进。</p>
     */
    @Test
    public void testCommitIndexAdvancesWithMajorityConfirmation() {
        RaftConfig config = createConfig("node2", "node3");
        RaftLogState logState = new RaftLogState();
        RaftNode node = new RaftNode("node1", config, logState);

        BlockingQueue<RaftReady> readyQueue = new ArrayBlockingQueue<>(1);
        node.startRaftEventLoop(readyQueue);
        node.becomeLeaderForTest();

        // 提出一条日志
        node.propose(new byte[]{1});

        // 收到 node2 的确认（leader 自身 matchIndex=1 + node2 matchIndex=1 = 2，满足多数派）
        AppendEntriesResponse response = new AppendEntriesResponse();
        response.setTerm(1);
        response.setFollowerId("node2");
        response.setSuccess(true);
        response.setMatchIndex(1);
        node.step(response);

        // matchCount(1) = 2（leader + node2），满足 majority=2，commitIndex 应该推进到 1
        assertEquals(1, node.getCommitIndex());
    }
}
