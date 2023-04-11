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
	args.LeaderID = rf.me

	ok := rf.sendAppendEntries(i, &args, &reply)

	if !ok {
		DPrintf("leader %v can not send heartbeat to peer %v", rf.me, i)
		return
	}

	if reply.Term > rf.currentTerm {
		DPrintf("leader %v received a heartbeat reply with higher term=%v than current term=%v from node %v",
			rf.me, reply.Term, rf.currentTerm, i)
		rf.currentTerm = reply.Term
		rf.changeRole(Follower)
		rf.resetElectionTimer()
	}
}

func (rf *Raft) resetAppendEntryTimerForPeer(i int) {
	if !rf.appendEntryTimers[i].Stop() {
		select {
		case <-rf.appendEntryTimers[i].C:
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
	DPrintf("====================node %v received AppendEntries PRC from %v====================", rf.me, args.LeaderID)

	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer DPrintf("================================================================================")
	reply.Term = rf.term

	if args.Term < rf.term {
		reply.Success = false
		return
	}

	rf.term = args.Term
	if rf.role != Follower {
		rf.changeRole(Follower)
	}
	rf.resetElectionTimer()
	// heartbeat
	if args.IsHeartBeat == true {
		reply.Success = true
		return
	}

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
