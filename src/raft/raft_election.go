package raft

import (
	"fmt"
	"math/rand"
	"time"
)

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

func (args RequestVoteArgs) String() string {
	return fmt.Sprintf("RequestVoteArgs{Term: %d, CandidateId: %d, LastLogIndex: %d, LastLogTerm: %d}",
		args.Term, args.CandidateId, args.LastLogIndex, args.LastLogTerm)
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool
}

func (rf *Raft) election() {
	Debug(dVote, "Server %d started election, current term %d", rf.me, rf.getCurrentTerm())
	ms := 200 + (rand.Int63() % 100)
	deadline := time.After(time.Duration(ms) * time.Millisecond)

	resultCh := make(chan RequestVoteReply, len(rf.peers))
	for idx := range rf.peers {
		if idx == rf.me {
			continue
		}
		if !rf.isCandidate() {
			return
		}

		go rf.getVote(resultCh, idx)
	}

	currentTerm := rf.getCurrentTerm()
	votes := 1
	collected := 1
	for collected < len(rf.peers) {
		select {
		case reply := <-resultCh:
			if reply.VoteGranted {
				votes += 1
				Debug(dVote, "Server %d received a vote, current vote %d\n", rf.me, votes)
			}
			// Found a peer with a greater term, this peer can't become the leader anymore, stop the leader election
			if reply.Term > currentTerm {
				Debug(dVote, "Server %d's term %d is lower than peer's term %d", rf.me, currentTerm, reply.Term)
				rf.setState(FOLLOWER, reply.Term, -1)
				return
			}
			collected += 1
			// Elected as leader
			if votes > len(rf.peers)/2 {
				Debug(dLeader, "Server %d has %d votes with term %d and is LEADER now!\n", rf.me, votes, currentTerm)
				rf.setState(LEADER, currentTerm, rf.me)
				rf.initializePeerIndexState()
				rf.startLeaderProcesses()

				return
			}
		case <-deadline:
			Debug(dInfo, "Server %d nothing happened during election for term %d. Waiting for new election\n", rf.me, currentTerm)
			rf.setState(CANDIDATE, currentTerm, -1)
			return
		}
	}
}

func (rf *Raft) startLeaderProcesses() {
	for server := range rf.peers {
		if server == rf.me {
			continue
		}

		go rf.sendAppendEntries(server, rf.buildHeartBeatArgs(server), &AppendEntriesReply{})
		go rf.runLogReplicator(server)
	}
	go rf.runReplicaCounter()
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.lastCommTime = time.Now()

	Debug(dVote, "Server %d received request vote %v, current votedFor %d\n", rf.me, args, rf.state.VotedFor)
	requestTerm, candidateId, lastLogIndex, lastLogTerm := args.Term, args.CandidateId, args.LastLogIndex, args.LastLogTerm
	reply.VoteGranted = false
	// requester's term is smaller than current peer's term
	if requestTerm < rf.state.CurrentTerm {
		reply.Term = rf.state.CurrentTerm
		return
	}

	// requester's term is greater than current peer's term, convert current peer to follower
	if requestTerm > rf.state.CurrentTerm {
		Debug(dVote, "Server %d's term %d is lower than server %d's term %d\n", rf.me, rf.state.CurrentTerm, candidateId, requestTerm)
		rf.state.CurrentTerm = requestTerm
		rf.state.Role = FOLLOWER
		rf.state.VotedFor = -1
		rf.persist()
	}

	if lastLogTerm < rf.logs[len(rf.logs)-1].Term || (lastLogTerm == rf.logs[len(rf.logs)-1].Term && lastLogIndex < len(rf.logs)-1) {
		Debug(dVote, "Server %d's lastLogIndex %d and lastLogTerm %d is more up-to-date than candidate %d's lastLogIndex %d and lastLogTerm %d\n",
			rf.me, len(rf.logs)-1, rf.logs[len(rf.logs)-1].Term, candidateId, lastLogIndex, lastLogTerm)
		reply.Term = rf.state.CurrentTerm
		reply.VoteGranted = false
		return
	}

	// vote for requester
	if rf.state.VotedFor == -1 || rf.state.VotedFor == candidateId {
		rf.state.VotedFor = candidateId
		rf.state.CurrentTerm = requestTerm
		reply.VoteGranted = true
		reply.Term = requestTerm
		rf.persist()
		Debug(dVote, "Server %d votedFor server %d\n", rf.me, candidateId)
		return
	}
}

func (rf *Raft) getVote(resultChan chan RequestVoteReply, server int) {
	lastLogIndex := rf.getLogSize() - 1
	lastLogTerm := rf.getLogEntry(lastLogIndex).Term
	currentTerm := rf.getCurrentTerm()

	requestVoteArgs := RequestVoteArgs{currentTerm, rf.me, lastLogIndex, lastLogTerm}
	requestVoteReply := RequestVoteReply{}
	ok := rf.sendRequestVote(server, &requestVoteArgs, &requestVoteReply)
	if !ok {
		return
	}

	resultChan <- requestVoteReply
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
