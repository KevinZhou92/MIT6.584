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

	"bytes"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labgob"
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
	Role        Role
	CurrentTerm int
	VotedFor    int // index of the candidate that this peer voted for
}

func (state *State) String() string {
	return fmt.Sprintf("State: role: %s, CurrentTerm: %d, VotedFor: %d", roleMap[state.Role], state.CurrentTerm, state.VotedFor)
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
	logState       *LogState       // the commited log index and the applied log index, here applied means the result has been returned to client
	peerIndexState *PeerIndexState // the next index and match index for each peer
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
	term = rf.state.CurrentTerm
	isleader = rf.state.Role == LEADER

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
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.state)
	e.Encode(rf.logs)
	raftstate := w.Bytes()

	rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var state State
	var logs []LogEntry
	if d.Decode(&state) != nil || d.Decode(&logs) != nil {
		Debug(dError, "Server %d is unable to restore persist state", rf.me)
		return
	}

	rf.state = &state
	rf.logs = logs
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

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
	rf.persist()

	rf.peerIndexState.nextIndex[rf.me] = len(rf.logs)
	rf.peerIndexState.matchIndex[rf.me] = len(rf.logs) - 1
	Debug(dLog, "Server %d appended log %v and has log size %d", rf.me, logEntry, len(rf.logs))

	return len(rf.logs) - 1
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
		timeout := (300 + time.Duration(rand.Int63()%300)) * time.Millisecond
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

func (rf *Raft) runApplier() {
	Debug(dInfo, "Server %d started applier", rf.me)
	for !rf.killed() {
		serverCommitIndex := rf.getServerCommitIndex()
		lastAppliedIndex := rf.getServerAppliedIndex()
		Debug(dCommit, "Server %d commitIndex: %d, lastAppliedIndex: %d", rf.me, serverCommitIndex, lastAppliedIndex)
		for curIndex := lastAppliedIndex + 1; curIndex <= serverCommitIndex; curIndex += 1 {
			Debug(dCommit, "Server %d get index %d command, log size %d", rf.me, curIndex, rf.getLogSize())
			logEntry, err := rf.getLogEntry(curIndex)
			if err != nil {
				Debug(dError, "Server %d can't get index %d from log, log size %d", rf.me, curIndex, rf.getLogSize())
				continue
			}
			cmd := logEntry.Command
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
	Debug(dInfo, "Server %d started with state %v, logs: %v", rf.me, rf.state, rf.logs)

	rf.initializePeerIndexState()
	Debug(dInfo, "Server %d started with PeerIndexState %v", rf.me, rf.peerIndexState)

	// start ticker goroutine to start elections
	go rf.ticker()

	// start heatbeat goroutine to start elections
	go rf.sendHeartbeat()

	// start log replicator if raft peer is a leader
	if rf.isLeader() {
		rf.startLeaderProcesses()
	}

	// Apply committed message on each peer
	go rf.runApplier()

	Debug(dInfo, "Server %d started with term %d", rf.me, rf.getCurrentTerm())

	return rf
}
