package raft

import (
	"sort"
	"time"
)

func (rf *Raft) shoudStartElection(timeout time.Duration) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	curTime := time.Now()

	return rf.lastCommTime.Add(timeout).Before(curTime) && (rf.state.role == FOLLOWER || rf.state.role == CANDIDATE)
}

func (rf *Raft) getCurrentTerm() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.state.currentTerm
}

func (rf *Raft) setVotedFor(votedFor int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.state.votedFor = votedFor
}

func (rf *Raft) getLeaderInfo() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	isLeader := rf.state.role == LEADER
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

	return rf.state.role == CANDIDATE
}

func (rf *Raft) setState(role Role, term int, votedFor int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.state.role = role
	rf.state.currentTerm = term
	rf.state.votedFor = votedFor
}

// volatile state on all servers
func (rf *Raft) getLogEntry(logIndex int) LogEntry {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.logs[logIndex]
}

func (rf *Raft) getLogSize() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return len(rf.logs)
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

func (rf *Raft) getMajorityIndex() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	matchIndices := append([]int(nil), rf.peerIndexState.matchIndex...)

	sort.Ints(matchIndices)
	majorityIndex := matchIndices[(len(matchIndices)-1)/2]

	return majorityIndex
}

// ---------------------
// RPC Utils
// ---------------------
func (rf *Raft) buildHeartBeatArgs(server int) *AppendEntriesArgs {
	currentTerm := rf.getCurrentTerm()
	prevLogIndex, prevLogTerm := rf.getPrevLogInfo(server)
	leaderCommitIndex := rf.getServerCommitIndex()
	appendEntriesArgs := AppendEntriesArgs{
		currentTerm, rf.me, []LogEntry{}, prevLogIndex, prevLogTerm, leaderCommitIndex}

	return &appendEntriesArgs
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)

	return ok
}
