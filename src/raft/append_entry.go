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
		if (len(rf.log) == 0 && rf.lastIncludeIndex >= rf.nextIndex[peer_id]) ||
			(len(rf.log) > 0 && rf.log[len(rf.log)-1].Index >= rf.nextIndex[peer_id]) ||
			isHeartBeat {

			peerNextIndex := rf.nextIndex[peer_id]

			if peerNextIndex <= rf.lastIncludeIndex {
				args := InstallSnapshotArgs{
					Snapshot:         rf.persister.ReadSnapshot(),
					LastIncludeTerm:  rf.lastIncludeTerm,
					LastIncludeIndex: rf.lastIncludeIndex,
				}
				DPrintf("[%d] leader send snapshot %d to [%d]", rf.me, args.LastIncludeIndex, peer_id)
				go rf.leaderSendInstallSnapshot(peer_id, &args)
				continue
			}

			if len(rf.log) == 0 {
				args := AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeaderId:     rf.me,
					PrevLogIndex: rf.lastIncludeIndex,
					PrevLogTerm:  rf.lastIncludeTerm,
					Entries:      []LogEntry{},
					LeaderCommit: rf.commitIndex,
				}
				DPrintf("[%d] leader send entries to [%d], Entries: %v", rf.me, peer_id, args.Entries)
				go rf.leaderSendEntries(peer_id, &args)
			} else { //len(rf.log) > 0
				myLastLogIndex := rf.log[len(rf.log)-1].Index
				if peerNextIndex <= 0 {
					peerNextIndex = 1
				}
				if peerNextIndex > myLastLogIndex+1 {
					peerNextIndex = myLastLogIndex + 1
				}
				entries := rf.log[rf.Index2index(peerNextIndex):rf.Index2index(min(peerNextIndex+100, myLastLogIndex+1))]

				var PrevLogIndex int
				var PrevLogTerm int
				if peerNextIndex > rf.lastIncludeIndex+1 {
					preLog := rf.log[rf.Index2index(peerNextIndex-1)]
					PrevLogIndex = preLog.Index
					PrevLogTerm = preLog.Term
				} else {
					PrevLogIndex = rf.lastIncludeIndex
					PrevLogTerm = rf.lastIncludeTerm
				}
				args := AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeaderId:     rf.me,
					PrevLogIndex: PrevLogIndex,
					PrevLogTerm:  PrevLogTerm,
					Entries:      entries,
					LeaderCommit: rf.commitIndex,
				}
				DPrintf("[%d] leader send entries to [%d], Entries: %v", rf.me, peer_id, entries)
				go rf.leaderSendEntries(peer_id, &args)
			}
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
			rf.nextIndex[peer] = max(args.PrevLogIndex+len(args.Entries)+1, rf.nextIndex[peer])
			rf.matchIndex[peer] = max(args.PrevLogIndex+len(args.Entries), rf.matchIndex[peer])
		} else {
			if len(rf.log) == 0 { //没有log了，下次只能发snapshot
				rf.nextIndex[peer] = rf.lastIncludeIndex
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
					if reply.Xterm == -1 { //nextIndex太大，缩小至对方的最后一个log的index+1
						rf.nextIndex[peer] = reply.Xindex + 1
					} else {
						var i int
						//i=max(...)是因为乱序到达的reply中PrevLogIndex可能比len(rf.log)-1大。比如：figue8的S1
						for i = max(rf.Index2index(args.PrevLogIndex), len(rf.log)-1); i >= 0; i-- {
							if rf.log[i].Term == reply.Xterm {
								rf.nextIndex[peer] = min(i, rf.nextIndex[peer]) //找到term等于xterm的最后一个index
								break
							} else if rf.log[i].Term < reply.Xterm {
								rf.nextIndex[peer] = min(reply.Xindex, rf.nextIndex[peer]) //如果term等于xterm没有log，找到term小于xterm的最后一个index
							}
						}
						if i < 0 {
							rf.nextIndex[peer] = min(rf.lastIncludeIndex, rf.nextIndex[peer]) //如果term小于xterm的最后一个index没有log，缩小至snapshot的index
						}
					}
				} else {
					rf.nextIndex[peer]--
				}
			}
		}
	}
	rf.leaderCommitRule()
}

func (rf *Raft) leaderCommitRule() {
	if len(rf.log) == 0 {
		if rf.commitIndex < rf.lastIncludeIndex && rf.lastIncludeTerm == rf.currentTerm {
			rf.commitIndex = rf.lastIncludeIndex //TODO:应该不会运行到这里
			rf.apply()
			DPrintf("[%d] leader commit index: %d. nextIndex: %v. matchIndex: %v.", rf.me, rf.commitIndex, rf.nextIndex, rf.matchIndex)
		}
		return
	}
	for N := rf.log[(len(rf.log) - 1)].Index; N > rf.commitIndex; N-- {
		if rf.log[rf.Index2index(N)].Term == rf.currentTerm {
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
				DPrintf("[%d] leader commit index: %d. nextIndex: %v. matchIndex: %v.", rf.me, rf.commitIndex, rf.nextIndex, rf.matchIndex)
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
	if len(rf.log) > 0 { //Entries的index太大,超出了本raft的最后一个log
		if args.PrevLogIndex > rf.log[len(rf.log)-1].Index {
			reply.Success = false
			reply.IsConflict = true
			reply.Xindex = rf.log[len(rf.log)-1].Index
			reply.Xterm = -1
			rf.resetElectionTimer()
			return
		}
	} else {
		if args.PrevLogIndex > rf.lastIncludeIndex {
			reply.Success = false
			reply.IsConflict = true
			reply.Xindex = rf.lastIncludeIndex
			reply.Xterm = -1
			rf.resetElectionTimer()
			return
		}
	}

	if len(rf.log) > 0 { //不匹配，冲突
		if rf.Index2index(args.PrevLogIndex) >= 0 {
			if rf.log[rf.Index2index(args.PrevLogIndex)].Term != args.PrevLogTerm {
				reply.Success = false
				reply.IsConflict = true
				reply.Xterm = rf.log[rf.Index2index(args.PrevLogIndex)].Term
				for i := args.PrevLogIndex - 1; i >= rf.log[0].Index; i-- {
					if rf.log[rf.Index2index(i)].Term != reply.Xterm {
						reply.Xindex = i //index是term小于xterm的最后一个index
						break
					}
				}
				rf.resetElectionTimer()
				return
			}
		} else { //这里假定snapshot压缩的log都是正确的，不会出现冲突
			// reply.Success = false
			// reply.IsConflict = true
			// reply.Xterm = rf.lastIncludeTerm
			// //reply.Xindex = rf.lastIncludeIndex
			// rf.resetElectionTimer()
			// return
		}
	} else {
		if rf.lastIncludeTerm != args.PrevLogTerm {
			reply.Success = false
			reply.IsConflict = true
			reply.Xterm = rf.lastIncludeTerm //leader一定存在term==rf.lastIncludeTerm的log，此后leader更新nextIndex为lastIncludeTerm的最后一个log的index
			reply.Xindex = rf.lastIncludeIndex
			rf.resetElectionTimer()
			return
		}
	}

	if len(rf.log) > 0 { //PrevLogTerm在上条if语句已经判断过了，说明现在LogEntry可以被接受了
		for idx, entry := range args.Entries {
			if rf.Index2index(entry.Index) < 0 {
				continue
			}
			// append entries rpc 3
			if entry.Index <= rf.log[len(rf.log)-1].Index && rf.log[rf.Index2index(entry.Index)].Term != entry.Term { //如果这里还冲突，直接截断后面的log
				rf.log = rf.log[:rf.Index2index(entry.Index)]
				rf.persist()
			}
			// append entries rpc 4
			if len(rf.log) == 0 || entry.Index > rf.log[len(rf.log)-1].Index {
				rf.log = append(rf.log, args.Entries[idx:]...)
				rf.persist()
				break
			}
		}
	} else {
		rf.log = append(rf.log, args.Entries...)
		rf.persist()
	}

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.log[len(rf.log)-1].Index)
	}

	reply.Success = true
	rf.resetElectionTimer()
	rf.persist()
	rf.apply()
	DPrintf("[%d] receive append entries from %d, args: %v", rf.me, args.LeaderId, args)
	DPrintf("[%d] log: %v", rf.me, rf.log)
	DPrintf("[%d] commitIndex: %d,lastApplied: %d", rf.me, rf.commitIndex, rf.lastApplied)
	if len(rf.log) > 0 {
		DPrintf("[%d] leaderCommit: %d, rf.lastlog.Index: %d", rf.me, args.LeaderCommit, rf.log[len(rf.log)-1].Index)
	} else {
		DPrintf("[%d] leaderCommit: %d, rf.lastIncludeIndex: %d", rf.me, args.LeaderCommit, rf.lastIncludeIndex)
	}
}
