# Edge Case Doc
## Only allow one leader for any given term
```
# This works
if reqTerm > rf.state.CurrentTerm {
    rf.state.VotedFor = -1
    rf.state.Role = FOLLOWER
    rf.state.CurrentTerm = args.Term
    rf.persist()
}
# This won't work
if reqTerm >= rf.state.CurrentTerm {
    rf.state.VotedFor = -1
    rf.state.Role = FOLLOWER
    rf.state.CurrentTerm = args.Term
    rf.persist()
}
```
If we allow two leaders in a given term, it will result in incorrectly committing entry that is not replicated to majorities.
- Two servers get elected as Leader both in Term X, they could write different commands A,B at the same given index on differet peers
- Command A was replicated on majorities while command B is not
- A new server gets elected as Leader, it appends a new command in log and start to replicate to all peers
- When we replicate entry to peers, we are only checking the term at a given index, in the above case, all peers will have identical term at given index
- This will result in data inconsistency. 
- Therefore, we need to make sure at any given term, only one leader can write logs and replicate to peers
