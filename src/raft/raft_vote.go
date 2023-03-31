package raft

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//

import (
	"log"
	"math/rand"
	"time"
)

func (rf *Raft) isLogUpToDate(lastLogTerm int, lastLogIndex int) bool {
	myLastLogTerm := rf.log[len(rf.log)-1].term
	myLastLogIndex := len(rf.log) - 1
	return lastLogTerm > myLastLogTerm ||
		(lastLogTerm == myLastLogTerm && lastLogIndex >= myLastLogIndex)
}

func (rf *Raft) changeRole(role Role) {
	DPrintf("changing node %v from %v to %v", rf.me, rf.role, role)
	rf.role = role
	switch role {
	case Follower:
	case Candidate:
		rf.term++
		rf.votedFor = rf.me
	case Leader:
		// reset nextIndex and matchIndex
		rf.nextIndex = make([]int, len(rf.peers))
		rf.matchIndex = make([]int, len(rf.peers))
		for i := range rf.nextIndex {
			rf.nextIndex[i] = len(rf.log)
		}
	default:
		log.Fatalf("unknown role = %v\n", role)
	}
}

func (rf *Raft) resetElectionTimer() {
	if !rf.electionTimer.Stop() {
		select {
		case <-rf.electionTimer.C:
		default:
		}
	}
	// randomize election timeout
	rand.Seed(time.Now().UnixNano())
	// a random time interval between 0 and ElectionTimeOut(400ms)
	randomTimeout := time.Duration(rand.Int63()) % ElectionTimeout
	rf.electionTimer.Reset(ElectionTimeout + randomTimeout)
	DPrintf("reset election timer for node=%v, timeout=%v\n", rf.me, ElectionTimeout+randomTimeout)
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("node %v received RequestVote RPC from node %v\n", args.CanadidateId, rf.me)
	reply.Term = rf.term
	if rf.term > args.Term {
		reply.VoteGranted = false
		return
	} else if rf.votedFor == args.CanadidateId { // if already voted
		reply.VoteGranted = true
		return
	} else if rf.votedFor != -1 { // voted for someone else
		reply.VoteGranted = false
		return
	}
	// hasn't voted, check if log is up to date
	if args.Term > rf.term {
		rf.currentTerm = args.Term
		rf.changeRole(Follower)
		rf.resetElectionTimer()
	}
	if rf.isLogUpToDate(args.LastLogTerm, args.LastLogIndex) {
		rf.votedFor = args.CanadidateId
		rf.changeRole(Follower)
		rf.resetElectionTimer()
		reply.VoteGranted = true
		DPrintf("node=%v voted for %v for the first time\n", args.CanadidateId, rf.me)
		return
	}
	// no vote
	reply.VoteGranted = false
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("node %v started election\n", rf.me)
	if rf.role == Leader {
		DPrintf("node %v is a leader, skipping this election\n", rf.me)
		return
	}
	rf.changeRole(Candidate)
	rf.resetElectionTimer()
	args := RequestVoteArgs{
		Term:         rf.term,
		CanadidateId: rf.me,
		LastLogIndex: len(rf.log) - 1,
		LastLogTerm:  rf.log[len(rf.log)-1].term,
	}
	// vote for itself
	voteGranted := 1
	// TODO: put this into a go routine and
	// check if role is still candidiate before promote to leader
	for i := range rf.peers {
		if i != rf.me {
			var reply RequestVoteReply
			rf.sendRequestVote(i, &args, &reply)
			if reply.VoteGranted {
				voteGranted++
			} else if reply.Term > rf.term { // will not grant vote
				rf.term = reply.Term
				rf.changeRole(Follower)
				rf.resetElectionTimer()
				// no hope of becoming leader
				return
			}
			if voteGranted > len(rf.peers)/2 {
				rf.changeRole(Leader)
				rf.resetElectionTimer()
				DPrintf("node %v received %v votes, became leader\n", voteGranted, rf.me)
				break
			}
		}
	}

	// if me is a leader, the next time AppendEntryTimer fires, it will start heartbeat
	// if rf.role == Leader {
	// 	rf.mu.Unlock()
	// 	// start heartbeat
	// 	rf.sendHearbeatToPeers()
	// }
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) waitForElectionTimeout() {
	for !rf.killed() {
		<-rf.electionTimer.C
		rf.startElection()
	}
}
