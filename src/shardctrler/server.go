package shardctrler

import (
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"sort"
	"sync"
	"time"
)

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	cond                  sync.Cond
	maxAppliedOpIdOfClerk map[int64]int

	configs []Config // indexed by config num
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	op := Op{OpType: JoinOp, ClerkId: args.ClerkId, OpId: args.OpId, Servers: args.Servers}
	_, _, isLeader := sc.rf.Start(op)
	if !isLeader {
		reply.WrongLeader = true
		return
	} else {
		DPrintf("[%d] start join op %v", sc.me, op)
		reply.WrongLeader = false
		reply.Err = OK
		sc.mu.Lock()
		for sc.maxAppliedOpIdOfClerk[args.ClerkId] < args.OpId {
			sc.mu.Unlock()
			appliedOneOp := make(chan struct{})
			go func() {
				sc.mu.Lock()
				sc.cond.Wait()
				close(appliedOneOp)
				sc.mu.Unlock()
			}()
			select {
			case <-appliedOneOp:
			case <-time.After(100 * time.Millisecond):
				reply.WrongLeader = true
			}
			if reply.WrongLeader {
				break
			}
			sc.mu.Lock()
		}
		if !reply.WrongLeader {
			sc.mu.Unlock()
		}
		return
	}

}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	op := Op{OpType: LeaveOp, ClerkId: args.ClerkId, OpId: args.OpId, GIDs: args.GIDs}
	_, _, isLeader := sc.rf.Start(op)
	if !isLeader {
		reply.WrongLeader = true
		return
	} else {
		DPrintf("[%d] start leave op %v", sc.me, op)
		reply.WrongLeader = false
		reply.Err = OK
		sc.mu.Lock()
		for sc.maxAppliedOpIdOfClerk[args.ClerkId] < args.OpId {
			sc.mu.Unlock()
			appliedOneOp := make(chan struct{})
			go func() {
				sc.mu.Lock()
				sc.cond.Wait()
				close(appliedOneOp)
				sc.mu.Unlock()
			}()
			select {
			case <-appliedOneOp:
			case <-time.After(100 * time.Millisecond):
				reply.WrongLeader = true
			}
			if reply.WrongLeader {
				break
			}
			sc.mu.Lock()
		}
		if !reply.WrongLeader {
			sc.mu.Unlock()
		}
		return
	}
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	op := Op{OpType: MoveOp, ClerkId: args.ClerkId, OpId: args.OpId, GID: args.GID, Shard: args.Shard}
	_, _, isLeader := sc.rf.Start(op)
	if !isLeader {
		reply.WrongLeader = true
		return
	} else {
		DPrintf("[%d] start move op %v", sc.me, op)
		reply.WrongLeader = false
		reply.Err = OK
		sc.mu.Lock()
		for sc.maxAppliedOpIdOfClerk[args.ClerkId] < args.OpId {
			sc.mu.Unlock()
			appliedOneOp := make(chan struct{})
			go func() {
				sc.mu.Lock()
				sc.cond.Wait()
				close(appliedOneOp)
				sc.mu.Unlock()
			}()
			select {
			case <-appliedOneOp:
			case <-time.After(100 * time.Millisecond):
				reply.WrongLeader = true
			}
			if reply.WrongLeader {
				break
			}
			sc.mu.Lock()
		}
		if !reply.WrongLeader {
			sc.mu.Unlock()
		}
		return
	}
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	op := Op{OpType: QueryOp, ClerkId: args.ClerkId, OpId: args.OpId, Num: args.Num}
	_, _, isLeader := sc.rf.Start(op)
	if !isLeader {
		reply.WrongLeader = true
		return
	} else {
		DPrintf("[%d] start query op %v", sc.me, op)
		reply.WrongLeader = false
		reply.Err = OK
		sc.mu.Lock()
		for sc.maxAppliedOpIdOfClerk[args.ClerkId] < args.OpId {
			sc.mu.Unlock()
			appliedOneOp := make(chan struct{})
			go func() {
				sc.mu.Lock()
				sc.cond.Wait()
				close(appliedOneOp)
				sc.mu.Unlock()
			}()
			select {
			case <-appliedOneOp:
			case <-time.After(100 * time.Millisecond):
				reply.WrongLeader = true
			}
			if reply.WrongLeader {
				break
			}
			sc.mu.Lock()
		}
		if !reply.WrongLeader {
			if args.Num == -1 {
				reply.Config = sc.configs[len(sc.configs)-1]
			} else {
				reply.Config = sc.configs[args.Num]
			}
			DPrintf("[%d] leader query op %v reply %v", sc.me, op, reply)
			sc.mu.Unlock()
		}
		return
	}
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}
	sc.configs[0].Num = 0

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.maxAppliedOpIdOfClerk = make(map[int64]int)
	sc.cond = sync.Cond{L: &sc.mu}
	go sc.apply()

	return sc
}

func (sc *ShardCtrler) apply() {
	for {
		if sc.rf.Killed() {
			return
		}
		DPrintf("[%d] blocking on applyCh", sc.me)
		msg := <-sc.applyCh
		DPrintf("[%d] unblocked on applyCh", sc.me)
		op := msg.Command.(Op)
		sc.mu.Lock()
		if op.OpId <= sc.maxAppliedOpIdOfClerk[op.ClerkId] {
			sc.mu.Unlock()
			continue
		}
		sc.maxAppliedOpIdOfClerk[op.ClerkId] = op.OpId
		switch op.OpType {
		case JoinOp:
			DPrintf("[%d] apply join op %v", sc.me, op)
			sc.applyJoin(op)
		case LeaveOp:
			DPrintf("[%d] apply leave op %v", sc.me, op)
			sc.applyLeave(op)
		case MoveOp:
			DPrintf("[%d] apply move op %v", sc.me, op)
			sc.applyMove(op)
		case QueryOp:
			DPrintf("[%d] apply query op %v", sc.me, op)
			//do nothing
		default:
			panic("unknown op type")
		}
		sc.mu.Unlock()
		DPrintf("[%d] blocking on Broadcast op %v", sc.me, op)
		sc.cond.Broadcast()
		DPrintf("[%d] unblocked on Broadcast op %v", sc.me, op)
	}
}

func (sc *ShardCtrler) applyJoin(op Op) {
	newConfig := sc.configs[len(sc.configs)-1]
	newConfig.Groups = make(map[int][]string)
	for gid, servers := range sc.configs[len(sc.configs)-1].Groups {
		newConfig.Groups[gid] = servers
	}
	for gid, servers := range op.Servers {
		newConfig.Groups[gid] = servers
	}
	newConfig.Num++
	newConfig = balance(newConfig)
	sc.configs = append(sc.configs, newConfig)
}
func (sc *ShardCtrler) applyLeave(op Op) {
	newConfig := sc.configs[len(sc.configs)-1]
	newConfig.Groups = make(map[int][]string)
	for gid, servers := range sc.configs[len(sc.configs)-1].Groups {
		newConfig.Groups[gid] = servers
	}

	for _, gid := range op.GIDs {
		delete(newConfig.Groups, gid)
	}
	newConfig.Num++
	newConfig = balance(newConfig)
	sc.configs = append(sc.configs, newConfig)
}
func (sc *ShardCtrler) applyMove(op Op) {
	newConfig := sc.configs[len(sc.configs)-1]
	newConfig.Groups = make(map[int][]string)
	for gid, servers := range sc.configs[len(sc.configs)-1].Groups {
		newConfig.Groups[gid] = servers
	}

	newConfig.Shards[op.Shard] = op.GID
	newConfig.Num++
	newConfig = balance(newConfig)
	sc.configs = append(sc.configs, newConfig)
}

func balance(config Config) Config {
	DPrintf("before balance %v", config)
	NumOfGroups := len(config.Groups)
	if NumOfGroups == 0 {
		for i := 0; i < NShards; i++ {
			config.Shards[i] = 0
		}
		DPrintf("balanced config %v", config)
		return config
	} else {
		maxShardsOfGroup := NShards / NumOfGroups
		if NShards%NumOfGroups != 0 {
			maxShardsOfGroup++
		}
		maxTimesOfMaxShardsOfGroup := NShards % NumOfGroups
		shardsToReassign := make([]int, 0)
		graupAbleToReceiveShard := make([]int, 0)
		numShardsOfGroups := make(map[int]int)
		for i := 0; i < NShards; i++ {
			if _, exist := config.Groups[config.Shards[i]]; !exist {
				shardsToReassign = append(shardsToReassign, i)
			}
		}

		gids := make([]int, 0)
		for gid, _ := range config.Groups {
			gids = append(gids, gid)
		}
		sort.Ints(gids)

		for _, gid := range gids {
			numShardsOfGroup := 0
			for i := 0; i < NShards; i++ {
				if config.Shards[i] == gid {
					numShardsOfGroup++
					if numShardsOfGroup > maxShardsOfGroup {
						shardsToReassign = append(shardsToReassign, i)
					}
				}
			}
			if numShardsOfGroup < maxShardsOfGroup {
				graupAbleToReceiveShard = append(graupAbleToReceiveShard, gid)
			} else {
				if maxTimesOfMaxShardsOfGroup != 0 {
					maxTimesOfMaxShardsOfGroup--
				}
				if maxTimesOfMaxShardsOfGroup == 0 {
					maxShardsOfGroup = NShards / NumOfGroups
				}
			}
			numShardsOfGroups[gid] = numShardsOfGroup
		}
		for len(shardsToReassign) > 0 {
			shard := shardsToReassign[0]
			shardsToReassign = shardsToReassign[1:]
			gid := graupAbleToReceiveShard[0]
			config.Shards[shard] = gid
			numShardsOfGroups[gid]++
			if numShardsOfGroups[gid] >= maxShardsOfGroup {
				graupAbleToReceiveShard = graupAbleToReceiveShard[1:]
				if maxTimesOfMaxShardsOfGroup != 0 {
					maxTimesOfMaxShardsOfGroup--
				}
				if maxTimesOfMaxShardsOfGroup == 0 {
					maxShardsOfGroup = NShards / NumOfGroups
				}
			}
		}
		DPrintf("balanced config %v", config)
		return config
	}
}
