package raft

import (
// "math/rand"
// "sync"
// "time"
)

func (rf *Raft) distributeEntries(isHeartBeat bool) { //以leader的log为准，其他server如果有更新的log，直接删除新log
	for peer_id, _ := range rf.peers {
		if peer_id == rf.me {
			rf.resetElectionTimer()
			continue
		}
		if rf.log[len(rf.log)-1].Index >= rf.nextIndex[peer_id] || isHeartBeat {
			peerNextIndex := rf.nextIndex[peer_id]
			myLastLogIndex := rf.log[len(rf.log)-1].Index
			if peerNextIndex <= 0 {
				peerNextIndex = 1
			}
			if peerNextIndex > myLastLogIndex+1 {
				peerNextIndex = myLastLogIndex + 1
			}
			entries := rf.log[peerNextIndex:min(peerNextIndex+100, myLastLogIndex+1)]
			preLog := rf.log[peerNextIndex-1]
			args := AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: preLog.Index,
				PrevLogTerm:  preLog.Term,
				Entries:      entries,
				LeaderCommit: rf.commitIndex,
			}
			go rf.leaderSendEntries(peer_id, &args)
		}
	}
}

func (rf *Raft) leaderSendEntries(peer int, args *AppendEntriesArgs) {
	reply := AppendEntriesReply{}
	ok := rf.sendAppendEntries(peer, args, &reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if ok {
		if reply.Success {
			rf.nextIndex[peer] = args.PrevLogIndex + len(args.Entries) + 1
			rf.matchIndex[peer] = args.PrevLogIndex + len(args.Entries)
		} else {
			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.votedFor = -1
				rf.state = Follower
				rf.persist()
				rf.resetElectionTimer()
				return
			}
			if reply.IsConflict {
				if reply.Xterm == -1 {
					rf.nextIndex[peer] = reply.Xlen
				} else {
					for i := args.PrevLogIndex; i >= 0; i-- {
						if rf.log[i].Term <= reply.Xterm {
							rf.nextIndex[peer] = i
							break
						}
					}
				}
			} else {
				rf.nextIndex[peer]--
			}
		}
	}
	rf.leaderCommitRule()
}

func (rf *Raft) leaderCommitRule() {
	for N := len(rf.log) - 1; N > rf.commitIndex; N-- {
		if rf.log[N].Term == rf.currentTerm {
			count := 1
			for i := range rf.peers {
				if i == rf.me {
					continue
				}
				if rf.matchIndex[i] >= N {
					count++
				}
			}
			if count > len(rf.peers)/2 {
				rf.commitIndex = N
				rf.apply()
				break
			}
		}
	}
}

func (rf *Raft) sendAppendEntries(peer int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[peer].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm { //对方term落后
		reply.Term = rf.currentTerm
		reply.Success = false
		reply.IsConflict = false
		return
	} else if args.Term > rf.currentTerm { //对方term领先
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.state = Follower
		rf.persist()
	}
	if rf.state == Candidate {
		rf.state = Follower
	}
	reply.Term = rf.currentTerm
	if args.PrevLogIndex > rf.log[len(rf.log)-1].Index {
		reply.Success = false
		reply.IsConflict = true
		reply.Xlen = len(rf.log)
		reply.Xterm = -1
		rf.resetElectionTimer()
		return
	}
	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
		reply.IsConflict = true
		reply.Xlen = len(rf.log)
		reply.Xterm = rf.log[args.PrevLogIndex].Term
		rf.resetElectionTimer()
		return
	}

	for idx, entry := range args.Entries {
		// append entries rpc 3
		if entry.Index <= rf.log[len(rf.log)-1].Index && rf.log[entry.Index].Term != entry.Term {
			rf.log= rf.log[:entry.Index]
			rf.persist()
		}
		// append entries rpc 4
		if entry.Index > rf.log[len(rf.log)-1].Index {
			rf.log = append(rf.log, args.Entries[idx:]...)
			rf.persist()
			break
		}
	}

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.log[len(rf.log)-1].Index)
	}

	reply.Success = true
	rf.resetElectionTimer()
	rf.persist()
	rf.apply()
	DPrintf("server %d receive append entries from %d, args: %v", rf.me, args.LeaderId, args)
	DPrintf("server %d log: %v", rf.me, rf.log)
	DPrintf("server %d commitIndex: %d,lastApplied: %d", rf.me, rf.commitIndex, rf.lastApplied)
}
