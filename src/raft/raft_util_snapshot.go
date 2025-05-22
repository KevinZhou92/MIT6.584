package raft

import (
	"errors"
)

// volatile state on all servers
func (rf *Raft) getLogEntryWithSnapshotInfo(logIndex int) (LogEntry, error) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	actualLogIndex := logIndex - rf.snapshotState.LastIncludedIndex - 1

	if actualLogIndex < -1 || actualLogIndex >= len(rf.logs) {
		return EMPTY_LOG_ENTRY, errors.New("log index out of range")
	}

	if actualLogIndex == -1 {
		return LogEntry{rf.snapshotState.LastIncludedTerm, -1}, nil
	}

	return rf.logs[actualLogIndex], nil
}

func (rf *Raft) getLogEntriesFromIndexWithSnapshotInfo(logIndex int) ([]LogEntry, error) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	actualLogIndex := logIndex - rf.snapshotState.LastIncludedIndex - 1

	if actualLogIndex < 0 || actualLogIndex >= len(rf.logs) {
		return []LogEntry{EMPTY_LOG_ENTRY}, errors.New("log index out of range")
	}

	logEntries := append([]LogEntry(nil), rf.logs[actualLogIndex:]...)

	return logEntries, nil
}

func (rf *Raft) getSnapshotState() SnapshotState {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return *rf.snapshotState
}
