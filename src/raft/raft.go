package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"

	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 3D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type Role int

const (
	LEADER Role = iota
	CANDIDATE
	FOLLOWER
)

type State struct {
	role        Role
	currentTerm int
	votedFor    int // index of the candidate that this peer voted for
}

// A go object recording the index state for the logs
type LogState struct {
	commitIndex      int
	lastAppliedIndex int
}

// A go object recording the index state for each peer
type PeerIndexState struct {
	nextIndex  []int // index of next log entry to send to ith server
	matchIndex []int // index of highest log entry known to be replicated on server ith
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// serverState a Raft server must maintain.
	state        *State // state of current server, term and is role
	lastCommTime time.Time

	logs           []LogEntry
	logState       *LogState
	peerIndexState *PeerIndexState
	applyCh        chan ApplyMsg
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	var term int
	var isleader bool
	// Your code here (3A).
	term = rf.state.currentTerm
	isleader = rf.state.role == LEADER

	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

type LogEntry struct {
	Term    int
	Command interface{}
}

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

type AppendEntriesArgs struct {
	// Your data here (3A, 3B).
	Term         int
	LeaderId     int
	Entries      []LogEntry
	PrevLogIndex int
	PrevLogTerm  int
	LeaderCommit int
}

func (args AppendEntriesArgs) String() string {
	return fmt.Sprintf(
		"AppendEntriesArgs{Term: %d, LeaderId: %d, PrevLogIndex: %d, PrevLogTerm: %d, LeaderCommit: %d, Entries: %v}",
		args.Term, args.LeaderId, args.PrevLogIndex, args.PrevLogTerm, args.LeaderCommit, args.Entries,
	)
}

type AppendEntriesReply struct {
	// Your data here (3A, 3B).
	Term    int
	Success bool

	// Conflict Info
	ConflictEntryIndex int
	ConflictEntryTerm  int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.lastCommTime = time.Now()

	// could be a heartbeat message from a leader
	if len(args.Entries) == 0 && args.Term >= rf.state.currentTerm {
		Debug(dInfo, "Server %d received heartbeat from leader id %d with args %v\n", rf.me, args.LeaderId, args)
		rf.state.votedFor = -1
		rf.state.role = FOLLOWER
		rf.state.currentTerm = args.Term
	}

	// not a heart beat message, processing logs from append entries request
	reply.Success = false
	reqTerm := args.Term
	reply.Term = reqTerm
	// leaderId := args.LeaderId
	entries := args.Entries
	prevLogIndex := args.PrevLogIndex
	prevLogTerm := args.PrevLogTerm
	leaderCommitIndex := args.LeaderCommit

	// reply false if term < current term
	if reqTerm < rf.state.currentTerm {
		Debug(dWarn, "Server %d's term is greater than %d's term %d\n", rf.me, args.LeaderId, reqTerm)
		reply.Term = rf.state.currentTerm
		return
	}

	// reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm
	if prevLogIndex >= len(rf.logs) || (prevLogIndex > 0 && rf.logs[prevLogIndex].Term != prevLogTerm) {
		Debug(dWarn, "Server %d's term at prevLogIndex %d doesn't match prevLogTerm %d\n", rf.me, prevLogIndex, prevLogTerm)

		return
	}

	// Truncate log if prevLogIndex is not the last log in current peer's log.This could happen during a split brain scenario.
	// Note that we should not truncte committed log as once log is committed it it durable
	serverCommitIdIndex := rf.logState.commitIndex
	if prevLogIndex > 0 && prevLogIndex < len(rf.logs) && prevLogIndex >= serverCommitIdIndex && prevLogIndex != len(rf.logs)-1 {
		Debug(dWarn, "Server %d's lastLogIndex %d is not equal to prevLogIndex %d, truncate logs\n", rf.me, len(rf.logs)-1, prevLogIndex)
		rf.logs = rf.logs[:prevLogIndex+1]
	}

	Debug(dLog, "Server %d received a log %s\n", rf.me, args)

	if len(entries) != 0 {
		rf.logs = append(rf.logs, entries...)
		Debug(dLog, "Server %d append entries in log\n", rf.me)
	}

	// This check should happen after any potential log truncate that will happen during an appentry behavior
	// Otherwise we could end up mistakenly updating commit index on current peer
	if leaderCommitIndex > rf.logState.commitIndex {
		Debug(dWarn, "Server %d's commitIndex %d is behind leader %d's commitIndex %d, server log count: %d\n", rf.me, rf.logState.commitIndex, args.LeaderId, leaderCommitIndex, len(rf.logs))
		rf.logState.commitIndex = min(leaderCommitIndex, len(rf.logs)-1)
	}

	Debug(dLog, "Server %d logs: %v\n", rf.me, rf.logs)
	reply.Term = rf.state.currentTerm
	reply.Success = true
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

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	term := rf.getCurrentTerm()
	_, isLeader := rf.getLeaderInfo()

	// return immediately if current peer is not leader
	if !isLeader {
		return -1, -1, false
	}

	index := rf.appendLogLocally(LogEntry{term, command})

	return index, term, isLeader
}

func (rf *Raft) appendLogLocally(logEntry LogEntry) int {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.logs = append(rf.logs, logEntry)
	rf.peerIndexState.nextIndex[rf.me] = len(rf.logs)
	rf.peerIndexState.matchIndex[rf.me] = len(rf.logs) - 1
	Debug(dLog, "Server %d appended log %v", rf.me, logEntry)

	return len(rf.logs) - 1
}

func (rf *Raft) sendHeartbeat() {
	for !rf.killed() {
		time.Sleep(time.Duration(50) * time.Millisecond)

		if _, isLeader := rf.getLeaderInfo(); !isLeader {
			continue
		}

		for server := range rf.peers {
			if server == rf.me {
				continue
			}

			go rf.sendAppendEntries(server, rf.buildHeartBeatArgs(server), &AppendEntriesReply{})
		}
		// Debug(dInfo, "Server %d sent heartbeat to followers", rf.me)
	}
}

func (rf *Raft) getVote(resultChan chan RequestVoteReply, server int) {
	lastLogIndex := rf.getLogSize() - 1
	lastLogTerm := rf.getLogEntry(lastLogIndex).Term

	requestVoteArgs := RequestVoteArgs{rf.state.currentTerm, rf.me, lastLogIndex, lastLogTerm}
	requestVoteReply := RequestVoteReply{}
	ok := rf.sendRequestVote(server, &requestVoteArgs, &requestVoteReply)
	if !ok {
		return
	}

	resultChan <- requestVoteReply
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for !rf.killed() {
		// Your code here (3A)
		// Check if a leader election should be started.
		// pause for a random amount of time between 50 and 350
		// milliseconds.
		timeout := (50 + time.Duration(rand.Int63()%300)) * time.Millisecond
		time.Sleep(timeout)
		// Debug(dLog, "Server %d is %s", rf.me, roleMap[rf.state.role])
		// Debug(dTerm, "Server %d term is %d", rf.me, rf.state.currentTerm)
		if rf.shoudStartElection(timeout) {
			// increment term and start a new election
			rf.setState(CANDIDATE, rf.getCurrentTerm()+1, rf.me)
			rf.election()
		}
	}
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

// runReplicaCounter tracks the number of replicas that have acknowledged
// each log entry. Once a log entry is acknowledged by a majority of peers,
// it is considered committed and sent to the apply channel for execution.
//
// This method is typically run as a background goroutine during the leader's
// term to monitor replication progress and commit logs accordingly.
func (rf *Raft) runReplicaCounter() {
	for !rf.killed() {
		if _, isLeader := rf.getLeaderInfo(); !isLeader {
			return
		}

		currentTerm := rf.getCurrentTerm()
		majorityIndex := rf.getMajorityIndex()

		if majorityIndex >= rf.getLogSize() {
			time.Sleep(10 * time.Millisecond)
			continue
		}

		entry := rf.getLogEntry(majorityIndex)
		if entry.Term != currentTerm {
			time.Sleep(10 * time.Millisecond)
			continue
		}

		Debug(dLeader, "Server %d replicated log %d to majority, update leader commit\n", rf.me, majorityIndex)
		rf.setServerCommitIndex(majorityIndex)

		time.Sleep(10 * time.Millisecond)
	}
}

func (rf *Raft) runLogReplicator(server int) {
	Debug(dLeader, "Server %d started log replicator for server %d\n", rf.me, server)
	for !rf.killed() {
		Debug(dLeader, "Server %d try to replicate log to server %d", rf.me, server)
		// Stop log replicator is current peer is not leader anymore
		if _, isLeader := rf.getLeaderInfo(); !isLeader {
			return
		}

		currentTerm := rf.getCurrentTerm()
		leaderCommitIndex := rf.getServerCommitIndex()
		nextIndex := rf.getNextIndexForPeer(server)
		prevLogIndex := nextIndex - 1
		prevLog := rf.getLogEntry(prevLogIndex)
		prevLogTerm := prevLog.Term

		if nextIndex >= rf.getLogSize() {
			Debug(dLeader, "Server %d has no logs to replicate for server %d", rf.me, server)
			time.Sleep(100 * time.Millisecond)
			continue
		}

		logEntry := rf.getLogEntry(nextIndex)
		appendEntriesArgs := AppendEntriesArgs{currentTerm, rf.me, []LogEntry{logEntry}, prevLogIndex, prevLogTerm, leaderCommitIndex}
		appendEntriesReply := AppendEntriesReply{}

		ok := rf.sendAppendEntries(server, &appendEntriesArgs, &appendEntriesReply)
		if !ok {
			Debug(dLog, "Server %d rpc call to server %d failed", rf.me, server)
			continue
		}

		// Leader is not with the highest term, step down to follower
		if appendEntriesReply.Term > currentTerm {
			Debug(dWarn, "Server %d is not leader anymore cuz there is a peer has a higher term", rf.me)
			rf.setState(FOLLOWER, appendEntriesReply.Term, -1)
			continue
		}

		// Couldn't replicate message, log mismatch found from peer, reduce nextIndex and retry
		if !appendEntriesReply.Success {
			Debug(dWarn, "Server %d couldn't replicate log to server %d, reduce nextIndex to %d", rf.me, server, nextIndex-1)
			rf.setNextIndexForPeer(server, nextIndex-1)
			continue
		}

		// Message replication succeeded, update nextIndex and matchIndex
		Debug(dCommit, "Server %d replicated log %d to server %d, update next index to %d", rf.me, nextIndex, server, nextIndex+1)
		rf.setMatchIndexForPeer(server, nextIndex)
		rf.setNextIndexForPeer(server, nextIndex+1)
	}
}

func (rf *Raft) runApplier() {
	Debug(dInfo, "Server %d started applier", rf.me)
	for !rf.killed() {
		commitIndex := rf.getServerCommitIndex()
		lastAppliedIndex := rf.getServerAppliedIndex()
		Debug(dCommit, "Server %d commitIndex: %d, lastAppliedIndex: %d", rf.me, commitIndex, lastAppliedIndex)
		for curIndex := lastAppliedIndex + 1; curIndex <= commitIndex; curIndex += 1 {
			Debug(dCommit, "Server %d get index %d command, log count %d", rf.me, curIndex, rf.getLogSize())
			cmd := rf.getLogEntry(curIndex).Command
			applyMsg := ApplyMsg{true, cmd, curIndex, false, nil, -1, -1}
			rf.applyCh <- applyMsg
			rf.setServerAppliedIndex(curIndex)
		}
		time.Sleep(100 * time.Millisecond)
		//Debug(dInfo, "Server %d applied a message, lastAppliedIndex %d", rf.me, rf.getServerAppliedIndex())
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	rf.lastCommTime = time.Now()
	rf.state = &State{FOLLOWER, 0, -1}
	rf.logs = []LogEntry{
		{Term: 0, Command: 0},
	}
	rf.logState = &LogState{len(rf.logs) - 1, 0}
	rf.applyCh = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	// start heatbeat goroutine to start elections
	go rf.sendHeartbeat()

	// Apply committed message on each peer
	go rf.runApplier()

	Debug(dInfo, "Server %d started with term %d", rf.me, rf.getCurrentTerm())

	return rf
}
