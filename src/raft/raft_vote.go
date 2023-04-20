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
	myLastLogTerm := rf.log[len(rf.log)-1].Term
	myLastLogIndex := len(rf.log) - 1
	return lastLogTerm > myLastLogTerm ||
		(lastLogTerm == myLastLogTerm && lastLogIndex >= myLastLogIndex)
}

func (rf *Raft) changeRole(role Role) {
	DPrintf("changing roles for node %v from %v to %v", rf.me, rf.role, role)
	rf.role = role
	switch role {
	case Follower:
	case Candidate:
		rf.currentTerm++
		DPrintf("term=%v", rf.currentTerm)
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
	// a random time interval between 0 and ElectionTimeOut(400ms)RequestVote
	randomTimeout := time.Duration(rand.Int63()) % ElectionTimeout
	rf.electionTimer.Reset(ElectionTimeout + randomTimeout)
	DPrintf("reset election timer for node %v, timeout %v\n", rf.me, ElectionTimeout+randomTimeout)
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("====================node %v received RequestVote PRC from %v for term %v====================",
		rf.me, args.CanadidateId, args.Term)
	defer DPrintf("================================================================================")
	reply.Term = rf.currentTerm
	if rf.currentTerm > args.Term {
		reply.VoteGranted = false
		DPrintf("node %v rejected vote for node %v because of higher term\n", rf.me, args.CanadidateId)
		return
	} else if rf.currentTerm == args.Term {
		if rf.votedFor == args.CanadidateId { // if already voted
			DPrintf("node %v already voted for node %v\n", rf.me, args.CanadidateId)
			reply.VoteGranted = true
			return
		} else if rf.votedFor != -1 { // voted for someone else
			DPrintf("node %v already voted for node %v\n", rf.me, rf.votedFor)
			reply.VoteGranted = false
			return
		}
	}
	// electing a leader for a new term
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		if rf.role != Follower {
			rf.changeRole(Follower)
		}
		rf.votedFor = -1
		// rf.resetElectionTimer()
	}
	if rf.isLogUpToDate(args.LastLogTerm, args.LastLogIndex) {
		rf.votedFor = args.CanadidateId
		if rf.role != Follower {
			rf.changeRole(Follower)
		}

		reply.VoteGranted = true
		DPrintf("node %v voted for %v for the first time in this term", rf.me, args.CanadidateId)
		rf.resetElectionTimer()
		return
	}
	// no vote
	DPrintf("node %v rejected vote for node %v because of log not up to date\n", rf.me, args.CanadidateId)
	reply.VoteGranted = false
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("====================node %v started election, role=%v====================", rf.me, rf.role)
	defer DPrintf("================================================================================")
	if rf.role == Leader {
		DPrintf("node %v is a leader, skipping this election\n", rf.me)
		rf.resetElectionTimer()
		return
	}
	rf.changeRole(Candidate)
	rf.votedFor = rf.me
	// rf.resetElectionTimer()
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CanadidateId: rf.me,
		LastLogIndex: len(rf.log) - 1,
		LastLogTerm:  rf.log[len(rf.log)-1].Term,
	}
	// vote for itself
	replyCh := make(chan RequestVoteReply, len(rf.peers))
	replyCh <- RequestVoteReply{Term: rf.currentTerm, VoteGranted: true}
	// unlock as while sending request vote, it might receive other rpc and change state
	rf.mu.Unlock()

	for i := range rf.peers {
		if i != rf.me {
			go func(peer int) {
				// to store return val from rpc call, so that we know the other server has responded
				ch := make(chan bool, 1)
				var reply RequestVoteReply
				RPCTimer := time.NewTimer(RPCTimeout)
				defer RPCTimer.Stop()
				// keep sending rpc to this peer until respond or rpc timeout
				for {
					go func () {
						ok := rf.sendRequestVote(peer, &args, &reply)
						ch <- ok
					} ()
					select {
					case <- RPCTimer.C:
						DPrintf("node %v can't reach peer %v to send request vote rpc, rpc timeout",
							rf.me, peer)
						replyCh <- RequestVoteReply{Term: 0, VoteGranted: false}
						return
					case ok := <- ch:
						// a valid reply, can return too
						if ok {
							replyCh <- reply
							return
						} else { // rpc failed, rpc hasen't timed out, retry
							DPrintf("node %v can't reach peer %v to send request vote rpc, retrying",
								rf.me, peer)
							continue
						}
					}
				}
				
			}(i)
		}
	}

	rf.mu.Lock()
	// if other node hasn't become the leader first
	// check if role is still candidiate before promote to leader
	if rf.role == Candidate {
		votesReceived := 0
		votesGranted := 0
	CollectResponse:
		for {
			select {
			case <-rf.stopCh:
				DPrintf("node %v is killed, exiting", rf.me)
				return
			case reply := <-replyCh:
				votesReceived++
				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					DPrintf("node %v received reply with term %v, convert back to follower",
						rf.me, reply.Term)
					rf.changeRole(Follower)
					break CollectResponse
				}
				if reply.VoteGranted {
					votesGranted++
				}
				if votesGranted > len(rf.peers)/2 { // received majority of votes
					DPrintf("node %v received at least %v votes, became leader for term=%v",
						rf.me, votesGranted, rf.currentTerm)
					rf.changeRole(Leader)
					break CollectResponse
				} else if votesReceived == len(rf.peers) { // all peers responded, not enough votes
					DPrintf("node %v received %v votes, not enough", rf.me, votesGranted)
					rf.changeRole(Follower)
					break CollectResponse
				}
			}
		}
	}
	rf.resetElectionTimer()
	// if me is a leader, set the appendEntry Timer to fire immediately, it will start heartbeat
	if rf.role == Leader {
		for i := range rf.peers {
			if i == rf.me {
				continue
			}
			rf.resetAppendEntryTimerForPeerToZero(i)
		}
	}
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) waitForElectionTimeout() {
	for {
		select {
		// killed
		case <-rf.stopCh:
			DPrintf("node=%v is killed, exiting go routing to listen for election timeout", rf.me)
			return
		case <-rf.electionTimer.C:
			// DPrintf("election timeout for node %v", rf.me)
			rf.startElection()
		}
	}
}
