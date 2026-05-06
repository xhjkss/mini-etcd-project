package com.xhj.etcd.kernel.raft;

import com.xhj.etcd.kernel.raft.event.*;
import com.xhj.etcd.kernel.raft.log.RaftLogEntry;
import com.xhj.etcd.kernel.raft.raftrpc.*;
import com.xhj.etcd.serializer.Serializer;
import com.xhj.etcd.serializer.SerializerRegistry;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.*;

/**
 * RaftEventCodecTest
 *
 * @author XJks
 * @description RaftEventCodec编解码测试。
 */
public class RaftEventCodecTest {

    private Serializer serializer;
    private RaftEventCodec codec;

    @Before
    public void setUp() {
        serializer = SerializerRegistry.getDefaultSerializer();
        codec = new RaftEventCodec(serializer);
    }

    @Test
    public void testEncodeAndDecodeProposeEvent() {
        RaftProposeEventData eventData = new RaftProposeEventData();
        eventData.setCommandData(new byte[]{1, 2, 3});

        RaftEvent event = codec.encodeRaftEvent(RaftEventType.PROPOSE, "test-event-1", eventData);
        assertEquals(RaftEventType.PROPOSE, event.getType());
        assertEquals("test-event-1", event.getEventId());
        assertNotNull(event.getEventData());

        RaftProposeEventData decoded = codec.decodeRaftProposeEventData(event);
        assertArrayEquals(new byte[]{1, 2, 3}, decoded.getCommandData());
    }

    @Test
    public void testEncodeAndDecodeRequestVoteRequest() {
        RequestVoteRequest request = new RequestVoteRequest();
        request.setTerm(1);
        request.setCandidateId("node1");
        request.setLastLogIndex(10);
        request.setLastLogTerm(1);

        RaftEvent event = codec.encodeRaftEvent(RaftEventType.REQUEST_VOTE, request);
        RequestVoteRequest decoded = codec.decodeRequestVoteRequest(event);

        assertEquals(1, decoded.getTerm());
        assertEquals("node1", decoded.getCandidateId());
        assertEquals(10, decoded.getLastLogIndex());
        assertEquals(1, decoded.getLastLogTerm());
    }

    @Test
    public void testEncodeAndDecodeRequestVoteResponse() {
        RequestVoteResponse response = new RequestVoteResponse();
        response.setTerm(1);
        response.setVoterId("node2");
        response.setVoteGranted(true);

        RaftEvent event = codec.encodeRaftEvent(RaftEventType.REQUEST_VOTE_RESPONSE, response);
        RequestVoteResponse decoded = codec.decodeRequestVoteResponse(event);

        assertEquals(1, decoded.getTerm());
        assertEquals("node2", decoded.getVoterId());
        assertTrue(decoded.isVoteGranted());
    }

    @Test
    public void testEncodeAndDecodeAppendEntriesRequest() {
        RaftLogEntry entry1 = new RaftLogEntry();
        entry1.setIndex(5);
        entry1.setTerm(1);
        entry1.setCommandData(new byte[]{1});

        RaftLogEntry entry2 = new RaftLogEntry();
        entry2.setIndex(6);
        entry2.setTerm(1);
        entry2.setCommandData(new byte[]{2});

        AppendEntriesRequest request = new AppendEntriesRequest();
        request.setTerm(1);
        request.setLeaderId("leader1");
        request.setPrevLogIndex(4);
        request.setPrevLogTerm(1);
        request.setEntries(Arrays.asList(entry1, entry2));
        request.setLeaderCommit(5);

        RaftEvent event = codec.encodeRaftEvent(RaftEventType.APPEND_ENTRIES, request);
        AppendEntriesRequest decoded = codec.decodeAppendEntriesRequest(event);

        assertEquals(1, decoded.getTerm());
        assertEquals("leader1", decoded.getLeaderId());
        assertEquals(4, decoded.getPrevLogIndex());
        assertEquals(1, decoded.getPrevLogTerm());
        assertEquals(5, decoded.getLeaderCommit());
        assertEquals(2, decoded.getEntries().size());
    }

    @Test
    public void testEncodeAndDecodeAppendEntriesResponse() {
        AppendEntriesResponse response = new AppendEntriesResponse();
        response.setTerm(1);
        response.setFollowerId("follower1");
        response.setSuccess(true);
        response.setMatchIndex(10);
        response.setRejectHint(0);

        RaftEvent event = codec.encodeRaftEvent(RaftEventType.APPEND_ENTRIES_RESPONSE, response);
        AppendEntriesResponse decoded = codec.decodeAppendEntriesResponse(event);

        assertEquals(1, decoded.getTerm());
        assertEquals("follower1", decoded.getFollowerId());
        assertTrue(decoded.isSuccess());
        assertEquals(10, decoded.getMatchIndex());
    }

    @Test
    public void testEncodeAndDecodeInstallSnapshotRequest() {
        InstallSnapshotRequest request = new InstallSnapshotRequest();
        request.setTerm(1);
        request.setLeaderId("leader1");
        request.setLastIncludedIndex(100);
        request.setLastIncludedTerm(5);
        request.setLeaderCommit(95);
        request.setSnapshotData(new byte[]{1, 2, 3, 4, 5});

        RaftEvent event = codec.encodeRaftEvent(RaftEventType.INSTALL_SNAPSHOT, request);
        InstallSnapshotRequest decoded = codec.decodeInstallSnapshotRequest(event);

        assertEquals(1, decoded.getTerm());
        assertEquals("leader1", decoded.getLeaderId());
        assertEquals(100, decoded.getLastIncludedIndex());
        assertEquals(5, decoded.getLastIncludedTerm());
        assertEquals(95, decoded.getLeaderCommit());
        assertArrayEquals(new byte[]{1, 2, 3, 4, 5}, decoded.getSnapshotData());
    }

    @Test
    public void testEncodeAndDecodeInstallSnapshotResponse() {
        InstallSnapshotResponse response = new InstallSnapshotResponse();
        response.setTerm(1);
        response.setFollowerId("follower1");
        response.setSuccess(true);
        response.setLastIncludedIndex(100);

        RaftEvent event = codec.encodeRaftEvent(RaftEventType.INSTALL_SNAPSHOT_RESPONSE, response);
        InstallSnapshotResponse decoded = codec.decodeInstallSnapshotResponse(event);

        assertEquals(1, decoded.getTerm());
        assertEquals("follower1", decoded.getFollowerId());
        assertTrue(decoded.isSuccess());
        assertEquals(100, decoded.getLastIncludedIndex());
    }

    @Test
    public void testEncodeAndDecodeAdvanceEvent() {
        RaftAdvanceEventData eventData = new RaftAdvanceEventData();

        RaftEvent event = codec.encodeRaftEvent(RaftEventType.ADVANCE, eventData);
        RaftAdvanceEventData decoded = codec.decodeRaftAdvanceEventData(event);

        assertNotNull(decoded);
    }

    @Test
    public void testEncodeAndDecodeCreateSnapshotEvent() {
        RaftCreateSnapshotEventData eventData = new RaftCreateSnapshotEventData();
        eventData.setLastIncludedIndex(100);
        eventData.setStateMachineData(new byte[]{1, 2, 3});

        RaftEvent event = codec.encodeRaftEvent(RaftEventType.CREATE_SNAPSHOT, eventData);
        RaftCreateSnapshotEventData decoded = codec.decodeRaftCreateSnapshotEventData(event);

        assertEquals(100, decoded.getLastIncludedIndex());
        assertArrayEquals(new byte[]{1, 2, 3}, decoded.getStateMachineData());
    }

    @Test
    public void testNullDataHandling() {
        RaftEvent event = new RaftEvent();
        event.setType(RaftEventType.PROPOSE);
        event.setEventId("test");
        event.setEventData(null);

        RaftProposeEventData decoded = codec.decodeRaftProposeEventData(event);
        assertNotNull(decoded);
    }
}
