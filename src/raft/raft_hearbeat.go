package raft

import "time"

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
