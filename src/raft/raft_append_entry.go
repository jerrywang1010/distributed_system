package raft

// spawn a go routine for each peer and wait for append entry timeout and send append entry
func (rf *Raft) waitForAppendEntryTimeout() {
	for !rf.killed() {
		for i := range rf.peers {
			if i == rf.me {
				continue
			}
			select {
			case <-rf.appendEntryTimers[i].C:
				go rf.sendHearbeatToPeer(i)
			}
		}
	}
}

func (rf *Raft) sendHearbeatToPeer(i int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.resetAppendEntryTimerForPeer(i)
	if rf.role != Leader {
		return
	}
	args := AppendEntryArgs{}
	reply := AppendEntryReply{}
	args.Term = rf.term
	args.IsHeartBeat = true
	rf.sendAppendEntries(i, &args, &reply)
}

func (rf *Raft) resetAppendEntryTimerForPeer(i int) {
	if !rf.appendEntryTimers[i].Stop() {
		select {
			case <- rf.appendEntryTimers[i].C:
			default:
		}
	}
	rf.appendEntryTimers[i].Reset(AppendEntryTimeout)
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntryArgs, reply *AppendEntryReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}


func (rf *Raft) AppendEntries(args *AppendEntryArgs, reply *AppendEntryReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.term {
		reply.Success = false
		reply.Term = rf.term
		return
	}
	
	rf.term = args.Term
	if rf.role != Follower {
		rf.changeRole(Follower)
	}
	rf.resetElectionTimer()
	// heartbeat
	if args.IsHeartBeat == true {
		DPrintf("node %v received heartbeat from node %v\n", rf.me, args.LeaderID)
		reply.Success = true
		reply.Term = rf.term
		return
	}
	DPrintf("node %v recerived AppendEntries RPC from node %v, term=%v\n", 
			rf.me, args.LeaderID, args.Term)
	
	reply.Success = true
	reply.Term = rf.term		
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