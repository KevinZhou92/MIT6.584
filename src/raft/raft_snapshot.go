package raft

import "fmt"

type InstallSnapShotArgs struct {
	Term                  int
	LeaderId              int
	LastIncludedIndex     int
	LastIncludedIndexTerm int
	Offset                int
	Data                  []byte
	Done                  bool
}

func (args InstallSnapShotArgs) String() string {
	return fmt.Sprintf(
		"InstallSnapShotArgs{Term: %d, LeaderId: %d, LastIncludedIndex: %d, LastIncludedIndexTerm: %d, Offset: %d, Done: %t}",
		args.Term, args.LeaderId, args.LastIncludedIndex, args.LastIncludedIndexTerm, args.Offset, args.Done,
	)
}

type InstallSnapShotReply struct {
	Term int
}
