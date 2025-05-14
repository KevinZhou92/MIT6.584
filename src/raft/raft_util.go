package raft

import (
	"time"
)

var roleMap = map[Role]string{
	LEADER:    "Leader",
	CANDIDATE: "Candidate",
	FOLLOWER:  "Follower",
}

func (rf *Raft) shoudStartElection(timeout time.Duration) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	curTime := time.Now()

	return rf.lastCommTime.Add(timeout).Before(curTime) && (rf.state.Role == FOLLOWER || rf.state.Role == CANDIDATE)
}

func (rf *Raft) getCurrentTerm() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.state.CurrentTerm
}

func (rf *Raft) getLeaderInfo() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	isLeader := rf.state.Role == LEADER
	var leaderId int
	if isLeader {
		leaderId = rf.me
	} else {
		leaderId = -1
	}

	return leaderId, isLeader
}

func (rf *Raft) getPrevLogInfo(server int) (int, int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	prevLogIndex := rf.peerIndexState.nextIndex[server] - 1
	prevLogTerm := rf.logs[prevLogIndex].Term

	return prevLogIndex, prevLogTerm
}

func (rf *Raft) getServerCommitIndex() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.logState.commitIndex
}

func (rf *Raft) setServerCommitIndex(commitIndex int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.logState.commitIndex = commitIndex
}

func (rf *Raft) getServerAppliedIndex() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.logState.lastAppliedIndex
}

func (rf *Raft) setServerAppliedIndex(appliedIndex int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.logState.lastAppliedIndex = appliedIndex
}

func (rf *Raft) isCandidate() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.state.Role == CANDIDATE
}

func (rf *Raft) isLeader() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.state.Role == LEADER
}

func (rf *Raft) GetRole() Role {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.state.Role
}

func (rf *Raft) setState(role Role, term int, votedFor int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.state.Role = role
	rf.state.CurrentTerm = term
	rf.state.VotedFor = votedFor
	rf.persist()
}

// volatile state on all servers
func (rf *Raft) getLogEntry(logIndex int) LogEntry {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.logs[logIndex]
}

func (rf *Raft) getLogEntriesFromIndex(logIndex int) []LogEntry {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.logs[logIndex:]
}

func (rf *Raft) getLogSize() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return len(rf.logs)
}

func (rf *Raft) getLastLogTerm() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.logs[len(rf.logs)-1].Term
}

// volatile state on all servers
func (rf *Raft) initializePeerIndexState() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.peerIndexState = &PeerIndexState{
		nextIndex:  make([]int, len(rf.peers)),
		matchIndex: make([]int, len(rf.peers)),
	}

	for idx := range len(rf.peers) {
		rf.peerIndexState.nextIndex[idx] = len(rf.logs)
		rf.peerIndexState.matchIndex[idx] = 0
	}
}

func (rf *Raft) getMatchIndexForPeer(server int) int {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.peerIndexState.matchIndex[server]
}

func (rf *Raft) setMatchIndexForPeer(server int, index int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.peerIndexState.matchIndex[server] = index
}

func (rf *Raft) getNextIndexForPeer(server int) int {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.peerIndexState.nextIndex[server]
}

func (rf *Raft) setNextIndexForPeer(server int, index int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.peerIndexState.nextIndex[server] = index
}
