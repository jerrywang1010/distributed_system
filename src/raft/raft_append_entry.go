package raft

func (rf *Raft) sendHearbeatToPeers() {
	args := AppendEntryArgs{}
	reply := AppendEntryReply{}
	args.Term = -1
	for i := range rf.peers {
		if i != rf.me {
			go rf.sendAppendEntries(i, &args, &reply)
		}
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntryArgs, reply *AppendEntryReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}


func (rf *Raft) AppendEntries(args *AppendEntryArgs, reply *AppendEntryReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("node %v recerived AppendEntries RPC from node %v\n", rf.me, args.LeaderID)
	
	// 2A
	rf.changeRole(Follower)
	rf.resetElectionTimer()
	if args.Term == -1 {
		DPrintf("node %v received heartbeat from node %v\n", rf.me, args.LeaderID)
		return
	}

	// 2B
	// reply false if term < currentTerm
	// if rf.term > args.Term {
	// 	reply.Success = false
	// 	reply.Term = rf.term
	// 	return
	// }	
	// // reply false if log doesn't contain a log entry at prevLogIndex whose term matches preLogTerm
	// if len(rf.log) > args.PrevLogIndex && rf.log[args.PrevLogIndex].term != args.PrevLogTerm {
	// 	reply.Success = false
	// 	reply.Term = rf.term
	// 	return
	// }
}