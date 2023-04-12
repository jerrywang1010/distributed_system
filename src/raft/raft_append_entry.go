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
				if !rf.killed() {
					go rf.sendHearbeatToPeer(i)
				} else {
					break
				}
			}
		}
	}
}

func (rf *Raft) sendHearbeatToPeer(i int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.role != Leader {
		return
	}
	DPrintf("====================leader %v sending heartbeats to node %v, role=%v====================",
		rf.me, i, rf.currentTerm)
	defer DPrintf("================================================================================")
	rf.resetAppendEntryTimerForPeer(i)
	args := AppendEntryArgs{}
	reply := AppendEntryReply{}
	args.Term = rf.currentTerm
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("====================node %v received AppendEntries PRC from %v====================", rf.me, args.LeaderID)
	defer DPrintf("================================================================================")
	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		reply.Success = false
		return
	}

	rf.currentTerm = args.Term
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
	reply.Term = rf.currentTerm
	// 2B
	// reply false if term < currentTerm
	// if rf.currentTerm > args.Term {
	// 	reply.Success = false
	// 	reply.Term = rf.currentTerm
	// 	return
	// }
	// // reply false if log doesn't contain a log entry at prevLogIndex whose term matches preLogTerm
	// if len(rf.log) > args.PrevLogIndex && rf.log[args.PrevLogIndex].term != args.PrevLogTerm {
	// 	reply.Success = false
	// 	reply.Term = rf.currentTerm
	// 	return
	// }
}
