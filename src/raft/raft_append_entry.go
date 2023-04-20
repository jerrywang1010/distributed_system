package raft

import (
	"sync"
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
					DPrintf("node=%v is killed, exiting go routing to listen for appendEntry timeout for peer %v", rf.me, i)
					return
				case <-rf.appendEntryTimers[i].C:
					// DPrintf("appendEntry time out for node %v to node %v", rf.me, i)
					rf.sendHearbeatToPeer(i)
				}
			}
		}(i)
	}
}

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
	DPrintf("====================node %v received AppendEntries PRC from %v, term %v====================", rf.me, args.LeaderID, args.Term)
	defer DPrintf("================================================================================")
	reply.Term = rf.currentTerm

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
