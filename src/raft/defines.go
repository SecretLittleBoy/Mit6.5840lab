package raft

type RaftState int
const (
	Follower RaftState = iota
	Candidate
	Leader
)

type AppendEntriesArgs struct {
	Term int
	LeaderId int
	PrevLogIndex int
	PrevLogTerm int
	Entries []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term int
	Success bool
	//xterm int //与AppendEntriesArgs冲突的本raft的index的term
	//xindex int //与AppendEntriesArgs冲突的本raft的index
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