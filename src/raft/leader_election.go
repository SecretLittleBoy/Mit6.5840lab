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
				return
			}
		}
	}
}

func (rf *Raft) distributeEntries(isHeartBeat bool) {//以leader的log为准，其他server如果有更新的log，直接删除新log
	for peer_id,_ := range rf.peers{
		if peer_id == rf.me{
			rf.resetElectionTimer()
			continue
		}
		if rf.log[len(rf.log)-1].Index >= rf.nextIndex[peer_id] || isHeartBeat{
			peerNextIndex := rf.nextIndex[peer_id]
			myLastLogIndex := rf.log[len(rf.log)-1].Index
			if peerNextIndex <= 0 {
				peerNextIndex = 1
			}
			if peerNextIndex > myLastLogIndex + 1{
				peerNextIndex = myLastLogIndex + 1 //todo:different from example code
			}
			entries := rf.log[peerNextIndex:]
			preLog := rf.log[peerNextIndex-1]
			args := AppendEntriesArgs{
				Term: rf.currentTerm,
				LeaderId: rf.me,
				PrevLogIndex: preLog.Index,
				PrevLogTerm: preLog.Term,
				Entries: entries,
				LeaderCommit: rf.commitIndex,
			}
			go rf.leaderSendEntries(peer_id, &args)
		}
	}
}

func (rf *Raft) leaderSendEntries(peer int, args *AppendEntriesArgs){
	reply := AppendEntriesReply{}
	ok := rf.sendAppendEntries(peer, args, &reply)
	if ok{
		if reply.Success{
			rf.nextIndex[peer] = args.PrevLogIndex + len(args.Entries) + 1
			rf.matchIndex[peer] = args.PrevLogIndex + len(args.Entries)
		}else{
			rf.nextIndex[peer]--
		}
	}
}

func (rf *Raft) sendAppendEntries(peer int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool{
	ok := rf.peers[peer].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply){
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm{
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	} else if args.Term > rf.currentTerm{
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.state = Follower
	}
	if rf.state == Candidate{
		rf.state = Follower
	}
	reply.Term = rf.currentTerm
	//DPrintf("server %d receive append entries from %d, args: %v", rf.me, args.LeaderId, args)
	//DPrintf("server %d 's len(rf.log): %d",rf.me,len(rf.log));
	if args.PrevLogIndex > rf.log[len(rf.log)-1].Index{
		reply.Success = false
		return
	}
	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm{
		reply.Success = false
		return
	}

	rf.log = rf.log[:args.PrevLogIndex+1]
	rf.log = append(rf.log, args.Entries...)

	if args.LeaderCommit > rf.commitIndex{
		rf.commitIndex = min(args.LeaderCommit, rf.log[len(rf.log)-1].Index)
	}
	reply.Success = true
	rf.resetElectionTimer()
}