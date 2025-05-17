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
	PeerLogSize        int
}

func (args AppendEntriesReply) String() string {
	return fmt.Sprintf(
		"AppendEntriesReply{Term: %d, Success: %t, ConflictEntryIndex: %d, ConflictEntryTerm: %d, PeerLogSize: %d}",
		args.Term, args.Success, args.ConflictEntryIndex, args.ConflictEntryTerm, args.PeerLogSize)
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)

	return ok
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// could be a heartbeat message from a leader
	if len(args.Entries) == 0 {
		Debug(dInfo, "Server %d(term: %d) received heartbeat from leader id %d with args %v\n", rf.me, rf.electionState.CurrentTerm, args.LeaderId, args)
	} else {
		Debug(dInfo, "Server %d(term: %d) received logs from leader id %d with args %v\n", rf.me, rf.electionState.CurrentTerm, args.LeaderId, args)
	}

	// not a heart beat message, processing logs from append entries request
	reply.Success = false
	reqTerm := args.Term
	reply.Term = reqTerm
	reply.ConflictEntryIndex = -1
	reply.ConflictEntryTerm = -1
	reply.PeerLogSize = len(rf.logs)

	// leaderId := args.LeaderId
	entries := args.Entries
	prevLogIndex := args.PrevLogIndex
	prevLogTerm := args.PrevLogTerm
	leaderCommitIndex := args.LeaderCommit

	if reqTerm > rf.electionState.CurrentTerm {
		rf.electionState.VotedFor = -1
		rf.electionState.Role = FOLLOWER
		rf.electionState.CurrentTerm = args.Term
		rf.persist()
	}

	// reply false if term < current term
	if reqTerm < rf.electionState.CurrentTerm {
		Debug(dWarn, "Server %d's term %d is greater than %d's term %d\n", rf.me, rf.electionState.CurrentTerm, args.LeaderId, reqTerm)
		reply.Term = rf.electionState.CurrentTerm

		return
	}

	// only update the last communication time if we received a heartbeat from a current leader(which carries a term which is at least larger than peer's)
	rf.lastCommTime = time.Now()

	// reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm
	if prevLogIndex >= len(rf.logs) || (prevLogIndex > 0 && rf.logs[prevLogIndex].Term != prevLogTerm) {
		Debug(dWarn, "Server %d's term at prevLogIndex %d doesn't match prevLogTerm %d\n", rf.me, prevLogIndex, prevLogTerm)
		index := prevLogIndex
		for index < len(rf.logs) && index > 0 && rf.logs[index].Term != prevLogTerm {
			index -= 1
		}

		if 0 <= index && index < len(rf.logs) {
			reply.ConflictEntryIndex = index + 1
			reply.ConflictEntryTerm = rf.logs[index+1].Term
		}

		return
	}

	// Truncate log if prevLogIndex is not the last log in current peer's log.This could happen during a split brain scenario.
	// Note that we should not truncte committed log as once log is committed it it durable
	if len(entries) != 0 {
		Debug(dLog, "Server %d received log entries %s\n", rf.me, args)
		if prevLogIndex >= rf.logState.commitIndex && prevLogIndex+1 <= len(rf.logs) {
			Debug(dPersist, "Server %d log length before append: %d", rf.me, len(rf.logs))
			rf.logs = rf.logs[:prevLogIndex+1]
			Debug(dLog, "Server %d truncated its log to index %d", rf.me, prevLogIndex)
		}

		// If there were retries for a request, we just do an idempotenet operation here, we are put the entry
		// at the same index it was supposed to be at. For example, if prevLogIndex was 5, then the first entry in the
		// input should be at index 6. Note that we already made sure the prevLogTerm matches with the log entry's term
		// at prevLogIndex
		idx := prevLogIndex + 1
		for idx < len(rf.logs) && idx-prevLogIndex-1 < len(entries) {
			rf.logs[idx] = entries[idx-prevLogIndex-1]
			idx += 1
		}
		// append new logs
		if idx-prevLogIndex-1 < len(entries) {
			rf.logs = append(rf.logs, entries[idx-prevLogIndex-1:]...)
		}

		Debug(dPersist, "Server %d log length after append: %d", rf.me, len(rf.logs))
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
			if rf.logs[i].Term != rf.electionState.CurrentTerm {
				continue
			}
			newCommitIndex = i
		}
		rf.logState.commitIndex = newCommitIndex
	}

	Debug(dLog, "Server %d log size: %d lastLogTerm: %d, logs: %v\n", rf.me, len(rf.logs), rf.logs[len(rf.logs)-1].Term, rf.logs)
	rf.persist()

	reply.Term = rf.electionState.CurrentTerm
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
			entry, err := rf.getLogEntry(curIdx)
			if err != nil || entry.Term != currentTerm {
				continue // Raft can't commit logs from previous term
			}

			count := 1 // self
			for peer := range rf.peers {
				if peer != rf.me && rf.getMatchIndexForPeer(peer) >= curIdx {
					count++
				}
			}

			if count > len(rf.peers)/2 {
				Debug(dLeader, "Server %d replicate log to majority, commits log %d (term=%d)", rf.me, curIdx, entry.Term)
				rf.setServerCommitIndex(curIdx)
			}
		}

		time.Sleep(10 * time.Millisecond)
	}
}

func (rf *Raft) runLogReplicator(server int) {
	Debug(dLeader, "Server %d started log replicator for server %d\n", rf.me, server)
	// This boolean indicates if the prev append entry call is successful, if yes, we will
	// send log entries in batch in the following request, this case we can avoid always sending
	// all the remaining logs thus save some bandwidth
	prevSuccess := false
	for !rf.killed() {
		// Stop log replicator is current peer is not leader anymore
		if _, isLeader := rf.getLeaderInfo(); !isLeader {
			return
		}
		Debug(dLeader, "Server %d has %d logs with lastLogTerm: %d", rf.me, rf.getLogSize(), rf.getLastLogTerm())

		currentTerm := rf.getCurrentTerm()
		leaderCommitIndex := rf.getServerCommitIndex()
		nextIndex := min(rf.getLogSize(), rf.getNextIndexForPeer(server))
		prevLogIndex := nextIndex - 1
		prevLog, err := rf.getLogEntry(prevLogIndex)
		if err != nil {
			Debug(dError, "Server %d has less logs for server %d, log size: %d, reduce nextIndex to logsize", rf.me, server, rf.getLogSize())
			rf.setNextIndexForPeer(server, rf.getLogSize())
			continue
		}
		prevLogTerm := prevLog.Term

		if nextIndex >= rf.getLogSize() {
			Debug(dLeader, "Server %d has no logs to replicate for server %d", rf.me, server)
			time.Sleep(100 * time.Millisecond)
			continue
		}

		logEntries, err := rf.getLogEntriesFromIndex(nextIndex)
		if err != nil {
			Debug(dError, "Server %d has less logs for server %d, log size: %d, reduce nextIndex to logsize", rf.me, server, rf.getLogSize())
			rf.setNextIndexForPeer(server, rf.getLogSize())
			continue
		}

		if !prevSuccess {
			logEntries = []LogEntry{logEntries[0]}
		}

		appendEntriesArgs := AppendEntriesArgs{currentTerm, rf.me, logEntries, prevLogIndex, prevLogTerm, leaderCommitIndex}
		appendEntriesReply := AppendEntriesReply{}

		Debug(dLeader, "Server %d try to replicate log %v to server %d", rf.me, appendEntriesArgs, server)
		ok := rf.sendAppendEntries(server, &appendEntriesArgs, &appendEntriesReply)
		if !ok {
			Debug(dLog, "Server %d rpc call to server %d failed, current server role is %s", rf.me, server, roleMap[rf.GetRole()])
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
			Debug(dWarn, "Server %d couldn't replicate log to server %d, response %v", rf.me, server, appendEntriesReply)
			if appendEntriesReply.ConflictEntryTerm != -1 {
				conflictEntryIndex, conflictEntryTerm := appendEntriesReply.ConflictEntryIndex, appendEntriesReply.ConflictEntryTerm
				curIdx := rf.getLogSize() - 1
				for curIdx >= 0 {
					entry, err := rf.getLogEntry(curIdx)
					if err != nil {
						// Defensive fallback: log entry unexpectedly not found
						Debug(dError, "Server %d failed to get log entry at index %d: %v", rf.me, curIdx, err)
						break
					}
					if entry.Term == conflictEntryTerm {
						break
					}
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

			if rf.getNextIndexForPeer(server) >= appendEntriesReply.PeerLogSize {
				rf.setNextIndexForPeer(server, appendEntriesReply.PeerLogSize)
			}
			Debug(dWarn, "Server %d couldn't replicate log to server %d, reduce nextIndex to %d", rf.me, server, rf.getNextIndexForPeer(server))
			prevSuccess = false
			continue
		}

		// Message replication succeeded, update nextIndex and matchIndex
		// note that we are sending message in batch so we need to update nextIndex and matchIndex correspondingly
		Debug(dCommit, "Server %d replicated log %d to server %d, update next index to %d", rf.me, nextIndex+len(logEntries), server, nextIndex+1)
		rf.setMatchIndexForPeer(server, nextIndex+len(logEntries)-1)
		rf.setNextIndexForPeer(server, nextIndex+len(logEntries))
		prevSuccess = true
	}
}

// Apply log if the log has been replicated to majority of server
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
			Debug(dError, "Server %d sent a message!!", rf.me)
			rf.applyCh <- applyMsg
			rf.setServerAppliedIndex(curIndex)
		}
		time.Sleep(100 * time.Millisecond)
		Debug(dInfo, "Server %d applied a message, lastAppliedIndex %d", rf.me, rf.getServerAppliedIndex())
	}
}
