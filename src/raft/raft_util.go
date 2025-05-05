package raft

import (
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

func (rf *Raft) getPrevLogInfo() (int, int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return len(rf.logs) - 1, rf.logs[len(rf.logs)-1].Term

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

func (rf *Raft) getMatchIndex() []int {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.peerIndexState.matchIndex
}

// volatile state on all servers
func (rf *Raft) getLogEntry(logIndex int) LogEntry {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.logs[logIndex]
}

// volatile state on all servers
func (rf *Raft) getLogState() *LogState {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.logState
}

func (rf *Raft) setLogState(logState LogState) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.logState = &logState
}

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
