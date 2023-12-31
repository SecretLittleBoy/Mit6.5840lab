package raft

//import "fmt"

// "log"

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
	if index <= rf.lastIncludeIndex || len(rf.log) == 0 ||
		index > rf.commitIndex || index > rf.log[len(rf.log)-1].Index {
		//DPrintf("snapshot error. index:", index, "lastIncludeIndex:", rf.lastIncludeIndex, "commitIndex:", rf.commitIndex, "log len:", len(rf.log))
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
	if args.LastIncludeIndex <= rf.lastIncludeIndex { //本rf的snapshot更新
		reply.Success = true
		return
	} else if (len(rf.log) == 0 && args.LastIncludeIndex > rf.lastIncludeIndex) ||
		(len(rf.log) > 0 && args.LastIncludeIndex >= rf.log[len(rf.log)-1].Index) { //snapshot包含当前所有log的内容
		reply.Success = true
		snapshotCopy := make([]byte, len(args.Snapshot))
		copy(snapshotCopy, args.Snapshot)
		rf.snapshot = snapshotCopy
		rf.log = make([]LogEntry, 0)
		rf.lastIncludeIndex = args.LastIncludeIndex
		rf.lastIncludeTerm = args.LastIncludeTerm
		rf.commitIndex = args.LastIncludeIndex
		rf.persist()
		rf.apply()
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

		reply.Success = true
		if rf.commitIndex < rf.lastIncludeIndex {
			rf.commitIndex = rf.lastIncludeIndex
		}
		rf.persist()
		rf.apply()
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
			rf.nextIndex[peer_id] = max(args.LastIncludeIndex+1, rf.nextIndex[peer_id])
			rf.matchIndex[peer_id] = max(args.LastIncludeIndex, rf.matchIndex[peer_id])
			DPrintf("[%d] leader send snapshot to [%d] success", rf.me, peer_id)
		}
	}
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}
