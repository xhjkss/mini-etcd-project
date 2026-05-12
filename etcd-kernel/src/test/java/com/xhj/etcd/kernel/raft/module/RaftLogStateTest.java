package com.xhj.etcd.kernel.raft.module;

import com.xhj.etcd.kernel.raft.log.RaftLogEntry;
import com.xhj.etcd.kernel.raft.log.RaftLogState;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.*;

/**
 * RaftLogStateTest
 *
 * @author XJks
 * @description RaftLogState单元测试。
 */
public class RaftLogStateTest {

    @Test
    public void testAppendNewLocalLogEntry() {
        RaftLogState state = new RaftLogState();

        RaftLogEntry entry1 = new RaftLogEntry();
        entry1.setTerm(1);
        entry1.setCommandData(new byte[]{1});

        long index1 = state.appendNewLocalLogEntry(entry1);
        assertEquals(1, index1);
        assertEquals(1, state.getLastLogIndex());
        assertEquals(1, state.getLastLogTerm());

        RaftLogEntry entry2 = new RaftLogEntry();
        entry2.setTerm(1);
        entry2.setCommandData(new byte[]{2});

        long index2 = state.appendNewLocalLogEntry(entry2);
        assertEquals(2, index2);
        assertEquals(2, state.getLastLogIndex());
        assertEquals(1, state.getLastLogTerm());
    }

    @Test
    public void testGetLogEntryByIndex() {
        RaftLogState state = new RaftLogState();

        RaftLogEntry entry = new RaftLogEntry();
        entry.setTerm(1);
        entry.setCommandData(new byte[]{1});
        state.appendNewLocalLogEntry(entry);

        RaftLogEntry result = state.getLogEntryByIndex(1);
        assertNotNull(result);
        assertEquals(1, result.getIndex());
        assertEquals(1, result.getTerm());

        assertNull(state.getLogEntryByIndex(0));
        assertNull(state.getLogEntryByIndex(-1));
        assertNull(state.getLogEntryByIndex(100));
    }

    @Test
    public void testGetLogTermByIndex() {
        RaftLogState state = new RaftLogState();

        assertEquals(0, state.getLogTermByIndex(0));

        RaftLogEntry entry1 = new RaftLogEntry();
        entry1.setTerm(1);
        entry1.setCommandData(new byte[]{1});
        state.appendNewLocalLogEntry(entry1);

        RaftLogEntry entry2 = new RaftLogEntry();
        entry2.setTerm(2);
        entry2.setCommandData(new byte[]{2});
        state.appendNewLocalLogEntry(entry2);

        assertEquals(1, state.getLogTermByIndex(1));
        assertEquals(2, state.getLogTermByIndex(2));
        assertEquals(-1, state.getLogTermByIndex(100));
    }

    @Test
    public void testAppendLeaderReplicatedLogEntries() {
        RaftLogState state = new RaftLogState();

        RaftLogEntry localEntry = new RaftLogEntry();
        localEntry.setTerm(1);
        localEntry.setCommandData(new byte[]{1});
        state.appendNewLocalLogEntry(localEntry);

        RaftLogEntry leaderEntry1 = new RaftLogEntry();
        leaderEntry1.setTerm(2);
        leaderEntry1.setCommandData(new byte[]{2});
        RaftLogEntry leaderEntry2 = new RaftLogEntry();
        leaderEntry2.setTerm(2);
        leaderEntry2.setCommandData(new byte[]{3});

        List<RaftLogEntry> appended = state.appendLeaderReplicatedLogEntries(1,
                java.util.Arrays.asList(leaderEntry1, leaderEntry2));

        assertEquals(2, appended.size());
        assertEquals(3, state.getLastLogIndex());
        assertEquals(2, state.getLastLogTerm());
    }

    @Test
    public void testAppendLeaderReplicatedLogEntriesWithConflict() {
        RaftLogState state = new RaftLogState();

        RaftLogEntry entry1 = new RaftLogEntry();
        entry1.setTerm(1);
        entry1.setCommandData(new byte[]{1});
        state.appendNewLocalLogEntry(entry1);

        RaftLogEntry entry2 = new RaftLogEntry();
        entry2.setTerm(1);
        entry2.setCommandData(new byte[]{2});
        state.appendNewLocalLogEntry(entry2);

        RaftLogEntry conflictingEntry = new RaftLogEntry();
        conflictingEntry.setTerm(2);
        conflictingEntry.setCommandData(new byte[]{99});

        List<RaftLogEntry> appended = state.appendLeaderReplicatedLogEntries(1,
                java.util.Collections.singletonList(conflictingEntry));

        assertEquals(1, appended.size());
        assertEquals(2, state.getLastLogIndex());

        RaftLogEntry newEntry2 = state.getLogEntryByIndex(2);
        assertEquals(2, newEntry2.getTerm());
        assertArrayEquals(new byte[]{99}, newEntry2.getCommandData());
    }

    @Test
    public void testCompactLogEntriesToSnapshotBoundary() {
        RaftLogState state = new RaftLogState();

        for (int i = 1; i <= 5; i++) {
            RaftLogEntry entry = new RaftLogEntry();
            entry.setTerm(1);
            entry.setCommandData(new byte[]{(byte) i});
            state.appendNewLocalLogEntry(entry);
        }

        state.compactLogEntriesToSnapshotBoundary(3, 1);

        assertEquals(3, state.getLastIncludedIndex());
        assertEquals(1, state.getLastIncludedTerm());
        assertEquals(5, state.getLastLogIndex());
        assertEquals(2, state.getAllLogEntries().size());
    }

    @Test
    public void testRestoreLogStateBySnapshotBoundary() {
        RaftLogState state = new RaftLogState();

        for (int i = 1; i <= 5; i++) {
            RaftLogEntry entry = new RaftLogEntry();
            entry.setTerm(1);
            entry.setCommandData(new byte[]{(byte) i});
            state.appendNewLocalLogEntry(entry);
        }

        state.restoreLogStateBySnapshotBoundary(3, 1);

        assertEquals(3, state.getLastIncludedIndex());
        // After restore: entries > 3 remain (4, 5) = 2 entries
        // lastLogIndex = lastIncludedIndex + entries.size() = 3 + 2 = 5
        assertEquals(5, state.getLastLogIndex());
        assertEquals(2, state.getAllLogEntries().size());
    }

    @Test
    public void testHasLogPositionWithIndexAndTerm() {
        RaftLogState state = new RaftLogState();

        RaftLogEntry entry = new RaftLogEntry();
        entry.setTerm(1);
        entry.setCommandData(new byte[]{1});
        state.appendNewLocalLogEntry(entry);

        assertTrue(state.hasLogPositionWithIndexAndTerm(0, 0));
        assertTrue(state.hasLogPositionWithIndexAndTerm(1, 1));
        assertFalse(state.hasLogPositionWithIndexAndTerm(1, 2));
        assertFalse(state.hasLogPositionWithIndexAndTerm(2, 1));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCompactBeyondLastLog() {
        RaftLogState state = new RaftLogState();

        RaftLogEntry entry = new RaftLogEntry();
        entry.setTerm(1);
        entry.setCommandData(new byte[]{1});
        state.appendNewLocalLogEntry(entry);

        state.compactLogEntriesToSnapshotBoundary(100, 1);
    }
}
