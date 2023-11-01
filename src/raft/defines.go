package raft

type RaftState int

const (
	Follower RaftState = iota
	Candidate
	Leader
)

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term       int
	Success    bool
	IsConflict bool //是否有冲突
	Xterm      int  //-1:表示nextIndex太大；其他值：表示rf.log[args.PrevLogIndex].Term
	Xindex     int  //Xterm==-1时，表示最后一个log的index;Xterm!=-1时，表示term小于Xterm的最后一个index
}

func min(a int, b int) int {
	if a > b {
		return b
	}
	return a
}

func max(a int, b int) int {
	if a > b {
		return a
	}
	return b
}