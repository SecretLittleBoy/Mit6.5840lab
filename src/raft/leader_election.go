package raft

import (
	"math/rand"
	"sync"
	"time"
)

func (rf *Raft) resetElectionTimer() {//reset election timer to 150-300ms later
	rf.electionTime = time.Now().Add(time.Duration(rand.Intn(150)+150)*time.Millisecond)
}

func (rf *Raft) leaderElection(){
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.state = Candidate
	rf.persist()
	rf.resetElectionTimer()
	voteCount := 1
	requestVoteArgs := RequestVoteArgs{
		Term: rf.currentTerm,
		CandidateId: rf.me,
		LastLogIndex: rf.log[len(rf.log)-1].Index,
		LastLogTerm: rf.log[len(rf.log)-1].Term,
	}
	var becomeLeader sync.Once
	for peer_id,_ := range rf.peers{
		if peer_id == rf.me{
			continue
		}
		go rf.candidateRequestVote(requestVoteArgs, peer_id, &voteCount,&becomeLeader)
	}
}

func (rf *Raft) candidateRequestVote(args RequestVoteArgs, peer int, voteCount *int,becomeLeader *sync.Once){
	reply := RequestVoteReply{}
	ok := rf.sendRequestVote(peer, &args, &reply)
	if ok{
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if reply.VoteGranted{
			*voteCount++
			if *voteCount > len(rf.peers)/2 && rf.state == Candidate && rf.currentTerm == args.Term{
				becomeLeader.Do(func() {
					rf.state = Leader
					lastLogIndex := len(rf.log) - 1
					for i, _ := range rf.peers {
						rf.nextIndex[i] = lastLogIndex + 1
						rf.matchIndex[i] = 0
					}
					rf.distributeEntries(true)
				})
				return
			}
		}else{
			if reply.Term > rf.currentTerm{
				rf.currentTerm = reply.Term
				rf.votedFor = -1
				rf.state = Follower
				rf.persist()
				return
			}
		}
	}
}

//RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term <= rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	} else {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.state = Follower
		if args.LastLogTerm > rf.log[len(rf.log)-1].Term || (args.LastLogTerm == rf.log[len(rf.log)-1].Term && args.LastLogIndex >= rf.log[len(rf.log)-1].Index) {
			rf.votedFor = args.CandidateId
			rf.resetElectionTimer()
			reply.Term = rf.currentTerm
			reply.VoteGranted = true
			rf.persist()
			return
		} else {
			reply.Term = rf.currentTerm
			reply.VoteGranted = false
			rf.persist()
			return
		}
	}
}
