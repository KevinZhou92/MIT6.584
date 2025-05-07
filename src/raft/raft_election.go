package raft

import (
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

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool
}

func (rf *Raft) election() {
	Debug(dVote, "Server %d started election, current term %d", rf.me, rf.getCurrentTerm())
	ms := 50 + (rand.Int63() % 300)
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
				Debug(dVote, "Server %d's term %d is lower than peers", rf.me, currentTerm)
				rf.setState(FOLLOWER, reply.Term, -1)
				return
			}
			collected += 1
			// Elected as leader
			if votes > len(rf.peers)/2 {
				Debug(dLeader, "Server %d has %d votes. Server %d is leader now\n", rf.me, votes, rf.me)
				rf.setState(LEADER, currentTerm, rf.me)
				rf.initializePeerIndexState()
				for server := range rf.peers {
					if server == rf.me {
						continue
					}

					go rf.sendAppendEntries(server, rf.buildHeartBeatArgs(server), &AppendEntriesReply{})
					go rf.runLogReplicator(server)
				}
				go rf.runReplicaCounter()

				return
			}
		case <-deadline:
			Debug(dInfo, "Server %d nothing happened during election. Waiting for new election\n", rf.me)
			rf.setVotedFor(-1)
			return
		}
	}
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.lastCommTime = time.Now()

	Debug(dVote, "Server %d received request vote, current votedFor %d\n", rf.me, rf.state.votedFor)
	requestTerm, candidateId, lastLogIndex, lastLogTerm := args.Term, args.CandidateId, args.LastLogIndex, args.LastLogTerm
	reply.VoteGranted = false
	// requester's term is smaller than current peer's term
	if requestTerm < rf.state.currentTerm {
		reply.Term = rf.state.currentTerm
		return
	}

	// requester's term is greater than current peer's term, convert current peer to follower
	if requestTerm > rf.state.currentTerm {
		Debug(dVote, "Server %d's term is lower than server %d's term %d\n", rf.me, candidateId, requestTerm)
		rf.state.currentTerm = requestTerm
		rf.state.role = FOLLOWER
		rf.state.votedFor = -1
	}

	if lastLogTerm < rf.logs[len(rf.logs)-1].Term || (lastLogTerm == rf.logs[len(rf.logs)-1].Term && lastLogIndex < len(rf.logs)-1) {
		Debug(dVote, "Server %d's log/term is more up-to-date than candidate %d's\n", rf.me, candidateId)
		reply.Term = rf.state.currentTerm
		reply.VoteGranted = false
		return
	}

	// vote for requester
	if rf.state.votedFor == -1 {
		rf.state.votedFor = args.CandidateId
		rf.state.currentTerm = args.Term
		reply.VoteGranted = true
		reply.Term = requestTerm
		Debug(dVote, "Server %d votedFor server %d\n", rf.me, args.CandidateId)
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
