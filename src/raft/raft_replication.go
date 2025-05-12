package raft

import (
	"fmt"
	"time"
)

type LogEntry struct {
	Term    int
	Command interface{}
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

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)

	return ok
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.lastCommTime = time.Now()

	// could be a heartbeat message from a leader
	if len(args.Entries) == 0 {
		Debug(dInfo, "Server %d received heartbeat from leader id %d with args %v\n", rf.me, args.LeaderId, args)
	}

	// not a heart beat message, processing logs from append entries request
	reply.Success = false
	reqTerm := args.Term
	reply.Term = reqTerm
	reply.ConflictEntryIndex = -1
	reply.ConflictEntryTerm = -1

	// leaderId := args.LeaderId
	entries := args.Entries
	prevLogIndex := args.PrevLogIndex
	prevLogTerm := args.PrevLogTerm
	leaderCommitIndex := args.LeaderCommit

	if reqTerm >= rf.state.CurrentTerm {
		rf.state.VotedFor = -1
		rf.state.Role = FOLLOWER
		rf.state.CurrentTerm = args.Term
	}

	// reply false if term < current term
	if reqTerm < rf.state.CurrentTerm {
		Debug(dWarn, "Server %d's term is greater than %d's term %d\n", rf.me, args.LeaderId, reqTerm)
		reply.Term = rf.state.CurrentTerm
		return
	}

	// reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm
	if prevLogIndex >= len(rf.logs) || (prevLogIndex > 0 && rf.logs[prevLogIndex].Term != prevLogTerm) {
		Debug(dWarn, "Server %d's term at prevLogIndex %d doesn't match prevLogTerm %d\n", rf.me, prevLogIndex, prevLogTerm)
		index := prevLogIndex
		for index < len(rf.logs) && index > 0 && rf.logs[index].Term != prevLogTerm {
			index -= 1
		}

		if 0 < index && index < len(rf.logs) {
			reply.ConflictEntryIndex = index + 1
			reply.ConflictEntryTerm = rf.logs[index+1].Term
		}

		return
	}

	Debug(dLog, "Server %d received a log %s\n", rf.me, args)

	// Truncate log if prevLogIndex is not the last log in current peer's log.This could happen during a split brain scenario.
	// Note that we should not truncte committed log as once log is committed it it durable
	if len(entries) != 0 {
		if prevLogIndex >= rf.logState.commitIndex && prevLogIndex+1 <= len(rf.logs) {
			rf.logs = rf.logs[:prevLogIndex+1]
			Debug(dLog, "Server %d truncated its log to index %d", rf.me, prevLogIndex)
		}

		rf.logs = append(rf.logs, entries...)
		Debug(dLog, "Server %d append entries in log\n", rf.me)
	}

	// This check should happen after any potential log truncate that will happen during an appentry behavior
	// Otherwise we could end up mistakenly updating commit index on current peer
	if leaderCommitIndex > rf.logState.commitIndex {
		Debug(dWarn, "Server %d's commitIndex %d is behind leader %d's commitIndex %d, server log count: %d\n", rf.me, rf.logState.commitIndex, args.LeaderId, leaderCommitIndex, len(rf.logs))
		newCommitIndex := rf.logState.commitIndex
		// Find last new entry from current term, we should only commit the entry that belongs to current term
		// For example,
		// The original leader (Term 2) appends a log entry at index 4, but it fails to replicate it to a majority of followers.
		// The original leader crashes or gets partitioned.
		// The remaining two followers elect a new leader (Term 3).
		// The new leader writes a new log entry at index 4 (with Term 3), effectively overwriting the uncommitted entry from the old leader.
		// The new leader commits up to index 4 after successfully replicating it to a majority.
		// The old leader rejoins the cluster and steps down as a follower. It receives an AppendEntries RPC from the new leader with commitIndex = 4.
		// The old leader blindly sets its commit index to 4, without checking whether its log at index 4 matches the leader’s log term (Term 3).
		// As a result, the old leader incorrectly considers its own log at index 4 (Term 2) as committed, violating Raft’s safety property.
		// So we should just commit entry from current term
		for i := rf.logState.commitIndex + 1; i <= min(leaderCommitIndex, len(rf.logs)-1); i++ {
			if rf.logs[i].Term != rf.state.CurrentTerm {
				continue
			}
			newCommitIndex = i
		}
		rf.logState.commitIndex = newCommitIndex
	}

	Debug(dLog, "Server %d logs: %v\n", rf.me, rf.logs)
	reply.Term = rf.state.CurrentTerm
	reply.Success = true
}

func (rf *Raft) runReplicaCounter() {
	for !rf.killed() {
		if _, isLeader := rf.getLeaderInfo(); !isLeader {
			return
		}

		currentTerm := rf.getCurrentTerm()
		lastLogIndex := rf.getLogSize() - 1

		for curIdx := rf.getServerCommitIndex() + 1; curIdx <= lastLogIndex; curIdx++ {
			entry := rf.getLogEntry(curIdx)
			if entry.Term != currentTerm {
				continue // Raft can't commit logs from previous term
			}

			count := 1 // self
			for peer := range rf.peers {
				if peer != rf.me && rf.getMatchIndexForPeer(peer) >= curIdx {
					count++
				}
			}

			if count > len(rf.peers)/2 {
				Debug(dLeader, "Server %d commits log %d (term=%d)", rf.me, curIdx, entry.Term)
				rf.setServerCommitIndex(curIdx)
			}
		}

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
			if appendEntriesReply.ConflictEntryTerm != -1 {
				conflictEntryIndex, conflictEntryTerm := appendEntriesReply.ConflictEntryIndex, appendEntriesReply.ConflictEntryTerm
				curIdx := rf.getLogSize() - 1
				for curIdx >= 0 && rf.getLogEntry(curIdx).Term != conflictEntryTerm {
					curIdx -= 1
				}

				if curIdx >= 0 {
					rf.setNextIndexForPeer(server, curIdx+1)
				} else {
					rf.setNextIndexForPeer(server, conflictEntryIndex)
				}

			} else {
				rf.setNextIndexForPeer(server, nextIndex-1)
			}
			continue
		}

		// Message replication succeeded, update nextIndex and matchIndex
		Debug(dCommit, "Server %d replicated log %d to server %d, update next index to %d", rf.me, nextIndex, server, nextIndex+1)
		rf.setMatchIndexForPeer(server, nextIndex)
		rf.setNextIndexForPeer(server, nextIndex+1)
	}
}
