package raft

import (
	"time"
)

// spawn a go routine for each peer and wait for append entry timeout and send append entry, run forever until nodeis killed
// one go routine for each peer only, don't spawn inf threads
func (rf *Raft) waitForAppendEntryTimeout() {
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(i int) {
			DPrintf("spawning a go routine for node=%v to listen for appendEntry timeout for peer %v", rf.me, i)
			for {
				select {
				case <-rf.stopCh:
					// DPrintf("node=%v is killed, exiting go routing to listen for appendEntry timeout for peer %v", rf.me, i)
					return
				case <-rf.appendEntryTimers[i].C:
					// DPrintf("appendEntry time out for node %v to node %v", rf.me, i)
					// rf.sendHearbeatToPeer(i)
					rf.sendAppendEntriesToPeer(i)
				}
			}
		}(i)
	}
}

func (rf *Raft) getAppendEntriesArgsForPeer(i int) AppendEntryArgs {
	args := AppendEntryArgs{
		Term:              rf.currentTerm,
		LeaderID:          rf.me,
		LeaderCommitIndex: rf.commitIndex,
	}
	// if there are new stuff to commit
	if len(rf.log) > rf.nextIndex[i] {
		args.IsHeartBeat = false
		args.PrevLogIndex = rf.nextIndex[i] - 1
		args.PrevLogTerm = rf.log[args.PrevLogIndex].Term
		args.Entries = append(args.Entries, rf.log[rf.nextIndex[i]:]...)
	} else { // just an heartbeat
		args.IsHeartBeat = true
		args.PrevLogIndex = len(rf.log) - 1
		args.PrevLogTerm = rf.log[len(rf.log)-1].Term
	}
	return args
}

// after an appendEntry, update commitIndex on the leader
// If there exists an N such that N > commitIndex, a majority
// of matchIndex[i] ≥ N, and log[N].term == currentTerm:
// set commitIndex = N
func (rf *Raft) updateCommitIndex() {
	for i := rf.commitIndex + 1; ; i++ {
		matchedFollowerCount := 0
		for _, m := range rf.matchIndex {
			if m >= i {
				matchedFollowerCount++
				// already majority, go to next index
				if matchedFollowerCount >= len(rf.matchIndex)+1 {
					break
				}
			}
		}
		if matchedFollowerCount >= len(rf.matchIndex)+1 {
			if rf.log[i].Term == rf.currentTerm {
				rf.commitIndex = i
				DPrintf("updating commitIndex for leader %v to %v", rf.me, rf.commitIndex)
			}
		} else { // don't have majority
			return
		}
	}
}

func (rf *Raft) sendAppendEntriesToPeer(i int) {
	needUnlock := true
	defer func() {
		if needUnlock {
			rf.mu.Unlock()
		}
		DPrintf("================================================================================")
	}()

	for !rf.killed() {
		rf.mu.Lock()
		needUnlock = true

		rf.resetAppendEntryTimerForPeer(i)
		if rf.role != Leader {
			return
		}
		args := rf.getAppendEntriesArgsForPeer(i)
		DPrintf("====================leader %v sending AppendEntry to node %v, current term=%v====================",
			rf.me, i, rf.currentTerm)

		rf.mu.Unlock()
		needUnlock = false
		reply := AppendEntryReply{}

		ch := make(chan bool, 1)
		RPCtimer := time.NewTimer(RPCTimeout)
		defer RPCtimer.Stop()

		go func() {
			ch <- rf.sendAppendEntries(i, &args, &reply)
		}()

		select {
		case <-RPCtimer.C:
			DPrintf("leader %v can't reach peer %v to send AppendEntry rpc, rpc timeout", rf.me, i)
			reply.Success = false
			reply.Term = 0
			return
		case ok := <-ch:
			if ok {
				break
			} else {
				DPrintf("leader %v can't reach peer %v to send AppendEntry rpc, retrying", rf.me, i)
				continue
			}
		}
		rf.mu.Lock()
		needUnlock = true
		DPrintf("leader %v recerived AppendEntry reply from %v, reply.success=%v, reply.Term=%v", rf.me, i, reply.Success, reply.Term)

		if rf.role != Leader {
			return
		}
		if reply.Term > rf.currentTerm {
			DPrintf("leader %v received a AppendEntry reply with higher term=%v than current term=%v from node %v",
				rf.me, reply.Term, rf.currentTerm, i)
			rf.currentTerm = reply.Term
			rf.changeRole(Follower)
			rf.resetElectionTimer()
			return
		}

		rf.nextIndex[i] = reply.NextIndex
		if reply.Success {
			rf.matchIndex[i] = reply.NextIndex - 1
			// if heartbeat, commit index won't change
			if !args.IsHeartBeat {
				rf.updateCommitIndex()
			}
			return
		} else { // append entry failed
			// retry with updated nextIndex
			continue
		}
	}
}

func (rf *Raft) resetAppendEntryTimerForPeer(i int) {
	// if !rf.appendEntryTimers[i].Stop() {
	// 	select {
	// 	case <-rf.appendEntryTimers[i].C:
	// 	default:
	// 	}
	// }
	rf.appendEntryTimers[i].Stop()
	rf.appendEntryTimers[i].Reset(AppendEntryTimeout)
}

func (rf *Raft) resetAppendEntryTimerForPeerToZero(i int) {
	rf.appendEntryTimers[i].Stop()
	rf.appendEntryTimers[i].Reset(0)
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntryArgs, reply *AppendEntryReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) AppendEntries(args *AppendEntryArgs, reply *AppendEntryReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("====================node %v received AppendEntries PRC from %v, args=%v====================", rf.me, args.LeaderID, args)
	defer DPrintf("================================================================================")
	reply.Term = rf.currentTerm
	reply.NextIndex = -1

	if args.Term < rf.currentTerm {
		DPrintf("AppendEntry args term %v < current term %v, returning false", args.Term, rf.currentTerm)
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
		reply.NextIndex = len(rf.log)
		reply.Success = true
		return
	}

	// 2B
	// reply false if log doesn't contain a log entry at prevLogIndex whose term matches preLogTerm
	if len(rf.log) <= args.PrevLogIndex || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
		reply.Term = rf.currentTerm

		if args.PrevLogIndex < len(rf.log) {
			term := rf.log[args.PrevLogIndex]
			nextIndex := args.PrevLogIndex
			// go to the start of last term, and hope it matches in the next rpc
			for nextIndex > rf.commitIndex && rf.log[nextIndex] == term {
				nextIndex--
			}
			// next index is the previous term, so should set reply.nextIndex to the start of this term
			reply.NextIndex = nextIndex + 1
			DPrintf("AppendEntry failed on node %v, args.PrevLogIndex=%v, args.PrevLogTerm=%v, node's term=%v, nextIndex=%v",
				rf.me, args.PrevLogIndex, args.PrevLogTerm, rf.log[args.PrevLogIndex], nextIndex)
		} else {
			DPrintf("AppendEntry failed on node %v, log is too short, len=%v, args.PrevLogIndex=%v",
				rf.me, len(rf.log), args.PrevLogIndex)
			reply.NextIndex = len(rf.log)
		}
		return
	}

	// my log must contain the same term as in args at prevLogIndex
	reply.Success = true
	rf.log = append(rf.log[:args.PrevLogIndex+1], args.Entries...)
	reply.NextIndex = len(rf.log)
	DPrintf("AppendEntry successed")
	if rf.commitIndex < args.LeaderCommitIndex {
		if args.LeaderCommitIndex < len(rf.log) {
			rf.commitIndex = args.LeaderCommitIndex
		} else {
			rf.commitIndex = len(rf.log) - 1
		}
		DPrintf("updating commitIndex to %v", rf.commitIndex)
	}
}

/*
func (rf *Raft) sendHearbeatToPeer(i int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.resetAppendEntryTimerForPeer(i)
	if rf.role != Leader {
		return
	}
	DPrintf("====================leader %v sending heartbeats to node %v, current term=%v====================",
		rf.me, i, rf.currentTerm)
	defer DPrintf("================================================================================")
	args := AppendEntryArgs{}
	reply := AppendEntryReply{}
	args.Term = rf.currentTerm
	args.IsHeartBeat = true
	args.LeaderID = rf.me

	rf.mu.Unlock()

	// wait for the go routine that sends append entry rpc to finish before continuing
	var wg sync.WaitGroup
	wg.Add(1)

	go func(peer int) {
		ch := make(chan bool, 1)
		RPCtimer := time.NewTimer(RPCTimeout)
		defer RPCtimer.Stop()
		defer wg.Done()
		for {
			go func() {
				ch <- rf.sendAppendEntries(peer, &args, &reply)
			}()

			select {
			case <-RPCtimer.C:
				DPrintf("leader %v can't reach peer %v to send heartbeat rpc, rpc timeout", rf.me, peer)
				reply.Success = false
				reply.Term = 0
				return
			case ok := <-ch:
				if ok {
					return
				} else {
					DPrintf("leader %v can't reach peer %v to send heartbeat rpc, retrying", rf.me, peer)
					continue
				}
			}
		}
	}(i)

	wg.Wait()
	rf.mu.Lock()
	DPrintf("leader %v recerived heartbeat reply from %v, reply.Term=%v", rf.me, i, reply.Term)
	if reply.Term > rf.currentTerm {
		DPrintf("leader %v received a heartbeat reply with higher term=%v than current term=%v from node %v",
			rf.me, reply.Term, rf.currentTerm, i)
		rf.currentTerm = reply.Term
		rf.changeRole(Follower)
		rf.resetElectionTimer()
	}
}
*/
