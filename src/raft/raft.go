package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"log"
	//"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	state             RaftState     //current state of the server
	electionTime      time.Time     //elect when current time is electionTime
	applyCh           chan ApplyMsg //applyCh is a channel on which the tester or service expects Raft to send ApplyMsg messages.
	applyCond         *sync.Cond
	heartBeatInterval time.Duration //the interval of heart beat

	//Persistent state on all servers:>>>>>>>>begin
	//latest term server has seen (initialized to 0 on first boot, increases monotonically)
	//服务器看到的最新任期（首次启动时初始化为0，单调增加）
	currentTerm int
	//candidateId that received vote in current term (or null if none)
	//当前任期内获得选票的候选人ID（如果没有则为空）
	votedFor int
	//log entries; each entry contains command for state machine, and term when entry was received by leader (first index is 1)
	//日志条目；每个条目包含状态机的命令，以及领导者收到条目的时间（第一个索引是1）
	log              []LogEntry
	snapshot         []byte
	lastIncludeIndex int
	lastIncludeTerm  int
	//Persistent state on all servers:>>>>>>>>end

	//Volatile state on all servers:>>>>>>>>begin
	//index of highest log entry known to be committed (initialized to 0, increases monotonically)
	//已知被提交的最高日志条目的索引（初始化为0，单调增加）
	commitIndex int
	//index of highest log entry applied to state machine (initialized to 0, increases monotonically)
	//已应用于状态机的最高日志条目的索引（初始化为0，单调增加）
	lastApplied int
	//Volatile state on all servers:>>>>>>>>end

	//Volatile state on leaders:>>>>>>>>begin
	//for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	//对于每个服务器，要发送给该服务器的下一个日志条目的索引（初始化为领导者的最后一个日志索引+1）
	nextIndex []int
	//for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)
	//对于每个服务器，已知在服务器上复制的最高日志条目的索引（初始化为0，单调增加）
	matchIndex []int
	//Volatile state on leaders:>>>>>>>>end
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == Leader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.lastIncludeTerm)
	e.Encode(rf.lastIncludeIndex)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, rf.snapshot)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var decodedCurrentTerm int
	var decodedVotedFor int
	var decodedLog []LogEntry
	var decodedLastIncludeTerm int
	var decodedLastIncludeIndex int
	if d.Decode(&decodedCurrentTerm) != nil ||
		d.Decode(&decodedVotedFor) != nil ||
		d.Decode(&decodedLog) != nil ||
		d.Decode(&decodedLastIncludeTerm) != nil ||
		d.Decode(&decodedLastIncludeIndex) != nil {
		log.Fatal("readPersist error")
	} else {
		rf.currentTerm = decodedCurrentTerm
		rf.votedFor = decodedVotedFor
		rf.log = decodedLog
		rf.lastIncludeTerm = decodedLastIncludeTerm
		rf.lastIncludeIndex = decodedLastIncludeIndex
		rf.commitIndex = rf.lastIncludeIndex
		rf.lastApplied = rf.lastIncludeIndex
	}
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int //candidate’s term,选举成功后的任期
	CandidateId  int //candidate requesting vote
	LastLogIndex int //index of candidate’s last log entry
	LastLogTerm  int //term of candidate’s last log entry
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  //currentTerm, for candidate to update itself
	VoteGranted bool //true means candidate received vote
}

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != Leader {
		return -1, rf.currentTerm, false
	} else {
		if len(rf.log) == 0 {
			rf.log = append(rf.log, LogEntry{command, rf.currentTerm, rf.lastIncludeIndex + 1})
		} else {
			rf.log = append(rf.log, LogEntry{command, rf.currentTerm, rf.log[len(rf.log)-1].Index + 1})
		}
		rf.persist()
		rf.distributeEntries(false)
		DPrintf("[%v]: term %v Start %v", rf.me, rf.currentTerm, command)
		return rf.log[(len(rf.log) - 1)].Index, rf.currentTerm, true
	}
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}
func (rf *Raft) Killed() bool {
	return rf.killed()
}
func (rf *Raft) ticker() {
	for !rf.killed() {

		// Your code here (2A)
		// Check if a leader election should be started.
		rf.mu.Lock()
		if rf.state == Leader {
			rf.distributeEntries(true)
		} else if time.Now().After(rf.electionTime) {
			//println("server", rf.me, "start election")
			rf.leaderElection()
		}
		rf.mu.Unlock()
		// pause for a random amount of time between 50 and 350
		// milliseconds.
		//ms := 50 + (rand.Int63() % 300)
		//time.Sleep(time.Duration(ms) * time.Millisecond)
		time.Sleep(rf.heartBeatInterval)
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.applyCh = applyCh
	rf.state = Follower
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.heartBeatInterval = 50 * time.Millisecond
	rf.resetElectionTimer()

	rf.log = make([]LogEntry, 0)
	rf.log = append(rf.log, LogEntry{-1, 0, 0})
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	rf.applyCond = sync.NewCond(&rf.mu)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.snapshot = persister.ReadSnapshot()
	// start ticker goroutine to start elections
	go rf.ticker()

	go rf.applier()
	return rf
}

func (rf *Raft) applier() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for !rf.killed() {
		if rf.commitIndex > rf.lastApplied {
			if rf.lastApplied < rf.lastIncludeIndex {
				rf.lastApplied = rf.lastIncludeIndex
				applyMsg := ApplyMsg{
					CommandValid:  false,
					SnapshotValid: true,
					Snapshot:      rf.snapshot,
					SnapshotTerm:  rf.lastIncludeTerm,
					SnapshotIndex: rf.lastIncludeIndex,
				}
				rf.mu.Unlock()
				rf.applyCh <- applyMsg
				rf.mu.Lock()
			} else {
				rf.lastApplied++
				applyMsg := ApplyMsg{
					CommandValid: true,
					Command:      rf.log[rf.Index2index(rf.lastApplied)].Command,
					CommandIndex: rf.lastApplied,
				}
				rf.mu.Unlock()
				rf.applyCh <- applyMsg
				rf.mu.Lock()
			}
		} else {
			rf.applyCond.Wait()
		}
	}
}

func (rf *Raft) apply() {
	rf.applyCond.Broadcast()
}

/*
rf.log[index].Index==Index
make sure len(rf.log)>0
*/
func (rf *Raft) Index2index(Index int) int {
	return Index - rf.log[0].Index
}

func (rf *Raft) GetRaftStateSize() int {
	return rf.persister.RaftStateSize()
}