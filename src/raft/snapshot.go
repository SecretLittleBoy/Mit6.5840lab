package raft

import (
	//"log"
)

type InstallSnapshotArgs struct {
	Snapshot         []byte
	LastIncludeTerm  int
	LastIncludeIndex int
}
type InstallSnapshotReply struct {
	Success bool
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.

func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if index >= rf.commitIndex && index >= rf.log[len(rf.log)-1].Index { //rf.log must at least have one log
		return
	}
	DPrintf("[%d] state %v called Snapshot(%d)", rf.me, rf.state, index)
	DPrintf("[%d] before snapshot log len: %d", rf.me, len(rf.log))
	rf.lastIncludeIndex = index
	rf.lastIncludeTerm = rf.log[rf.Index2index(index)].Term
	tempLog := make([]LogEntry, len(rf.log)-(rf.Index2index(index)+1))
	copy(tempLog, rf.log[rf.Index2index(index)+1:])
	rf.log = tempLog
	snapshotCopy := make([]byte, len(snapshot))
	copy(snapshotCopy, snapshot)
	rf.snapshot = snapshotCopy
	rf.persist()
	DPrintf("[%d] after snapshot log len: %d", rf.me, len(rf.log))
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.LastIncludeIndex < rf.log[0].Index {//本rf的snapshot更新
		reply.Success = false
		return
	} else if args.LastIncludeIndex >= rf.log[len(rf.log)-1].Index{//snapshot包含当前所有log的内容
		reply.Success = true
		snapshotCopy := make([]byte, len(args.Snapshot))
		copy(snapshotCopy, args.Snapshot)
		rf.snapshot = snapshotCopy
		rf.log = make([]LogEntry, 0)
		rf.lastIncludeIndex = args.LastIncludeIndex
		rf.lastIncludeTerm = args.LastIncludeTerm
		rf.persist()
		DPrintf("[%d] install snapshot LastIncludeTerm:%d, lastIncludeIndex:%d. Now, log:%v", rf.me, rf.lastIncludeTerm, rf.lastIncludeIndex, rf.log)
		return
	} else {
		snapshotCopy := make([]byte, len(args.Snapshot))
		copy(snapshotCopy, args.Snapshot)
		rf.snapshot = snapshotCopy

		tempLog := make([]LogEntry, len(rf.log)-(rf.Index2index(args.LastIncludeIndex)+1))
		copy(tempLog, rf.log[rf.Index2index(args.LastIncludeIndex)+1:])
		rf.log = tempLog
		rf.lastIncludeIndex = args.LastIncludeIndex
		rf.lastIncludeTerm = args.LastIncludeTerm

		reply.Success=true
		rf.persist()
		DPrintf("[%d] install snapshot LastIncludeTerm:%d, lastIncludeIndex:%d. Now, log:%v", rf.me, rf.lastIncludeTerm, rf.lastIncludeIndex, rf.log)
		return
	}
}

func (rf *Raft) leaderSendInstallSnapshot(peer_id int, args *InstallSnapshotArgs) {
	reply := InstallSnapshotReply{}
	ok := rf.sendInstallSnapshot(peer_id, args, &reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if ok {
		if reply.Success {
			rf.nextIndex[peer_id] = args.LastIncludeIndex + 1
			rf.matchIndex[peer_id] = args.LastIncludeIndex
		}
	}
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}