package com.xhj.etcd.kernel.raft;

import com.xhj.etcd.kernel.raft.log.RaftLogEntry;
import com.xhj.etcd.kernel.raft.log.RaftLogState;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * RaftLogStateSnapshotTest
 *
 * @author XJks
 * @description RaftLogState快照相关功能测试。
 */
public class RaftLogStateSnapshotTest {

    @Test
    public void testSnapshotBoundaryAfterCompact() {
        RaftLogState state = new RaftLogState();

        for (int i = 1; i <= 10; i++) {
            RaftLogEntry entry = new RaftLogEntry();
            entry.setTerm(1);
            entry.setCommandData(new byte[]{(byte) i});
            state.appendNewLocalLogEntry(entry);
        }

        assertEquals(0, state.getLastIncludedIndex());
        assertEquals(0, state.getLastIncludedTerm());

        state.compactLogEntriesToSnapshotBoundary(5, 1);

        assertEquals(5, state.getLastIncludedIndex());
        assertEquals(1, state.getLastIncludedTerm());
        assertEquals(10, state.getLastLogIndex());
    }

    @Test
    public void testLogQueryAfterSnapshotBoundary() {
        RaftLogState state = new RaftLogState();

        for (int i = 1; i <= 10; i++) {
            RaftLogEntry entry = new RaftLogEntry();
            entry.setTerm(1);
            entry.setCommandData(new byte[]{(byte) i});
            state.appendNewLocalLogEntry(entry);
        }

        state.compactLogEntriesToSnapshotBoundary(5, 1);

        assertNull(state.getLogEntryByIndex(1));
        assertNull(state.getLogEntryByIndex(5));
        assertNotNull(state.getLogEntryByIndex(6));
        assertEquals(6, state.getLogEntryByIndex(6).getIndex());
    }

    @Test
    public void testRestoreFromSnapshotBoundary() {
        RaftLogState state = new RaftLogState();

        for (int i = 1; i <= 10; i++) {
            RaftLogEntry entry = new RaftLogEntry();
            entry.setTerm(1);
            entry.setCommandData(new byte[]{(byte) i});
            state.appendNewLocalLogEntry(entry);
        }

        state.restoreLogStateBySnapshotBoundary(7, 1);

        assertEquals(7, state.getLastIncludedIndex());
        // After restore: entries > 7 remain (8, 9, 10) = 3 entries
        // lastLogIndex = lastIncludedIndex + entries.size() = 7 + 3 = 10
        assertEquals(10, state.getLastLogIndex());
        assertEquals(3, state.getAllLogEntries().size());
    }

    @Test
    public void testRestoreWithSnapshotBoundaryLog() {
        RaftLogState state = new RaftLogState();

        for (int i = 1; i <= 5; i++) {
            RaftLogEntry entry = new RaftLogEntry();
            entry.setTerm(i <= 3 ? 1 : 2);
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
    public void testSnapshotBoundaryTermQuery() {
        RaftLogState state = new RaftLogState();

        for (int i = 1; i <= 5; i++) {
            RaftLogEntry entry = new RaftLogEntry();
            entry.setTerm(1);
            entry.setCommandData(new byte[]{(byte) i});
            state.appendNewLocalLogEntry(entry);
        }

        long term = state.getSnapshotBoundaryLogTerm(3);
        assertEquals(1, term);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSnapshotBoundaryTermQueryBeyondLastLog() {
        RaftLogState state = new RaftLogState();

        for (int i = 1; i <= 5; i++) {
            RaftLogEntry entry = new RaftLogEntry();
            entry.setTerm(1);
            entry.setCommandData(new byte[]{(byte) i});
            state.appendNewLocalLogEntry(entry);
        }

        state.getSnapshotBoundaryLogTerm(100);
    }

    @Test
    public void testAppendAfterSnapshotBoundary() {
        RaftLogState state = new RaftLogState();

        for (int i = 1; i <= 5; i++) {
            RaftLogEntry entry = new RaftLogEntry();
            entry.setTerm(1);
            entry.setCommandData(new byte[]{(byte) i});
            state.appendNewLocalLogEntry(entry);
        }

        state.compactLogEntriesToSnapshotBoundary(3, 1);

        RaftLogEntry newEntry = new RaftLogEntry();
        newEntry.setTerm(2);
        newEntry.setCommandData(new byte[]{100});
        long newIndex = state.appendNewLocalLogEntry(newEntry);

        assertEquals(6, newIndex);
        assertEquals(6, state.getLastLogIndex());
    }

    @Test
    public void testEmptyLogAfterSnapshotBoundary() {
        RaftLogState state = new RaftLogState();

        for (int i = 1; i <= 3; i++) {
            RaftLogEntry entry = new RaftLogEntry();
            entry.setTerm(1);
            entry.setCommandData(new byte[]{(byte) i});
            state.appendNewLocalLogEntry(entry);
        }

        state.compactLogEntriesToSnapshotBoundary(3, 1);

        assertTrue(state.getAllLogEntries().isEmpty());
        assertEquals(3, state.getLastIncludedIndex());
    }
}
