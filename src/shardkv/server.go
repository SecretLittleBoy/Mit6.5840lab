package shardkv

import (
	"bytes"
	"log"
	"sync"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"6.5840/shardctrler"
)

//Each shardkv server operates as part of a replica group.
//Each replica group serves Get, Put, and Append operations for some of the key-space shards.

type OpType int

const (
	GetOp OpType = iota
	PutOp
	AppendOp
	GetShardOp
	ReplaceShardOp
	NopOp
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	OpType  OpType
	ClerkId int64
	OpId    int
	Key     string
	Value   string

	// GetShard
	Shard int
	// ReplaceShard
	KvMap map[string]string
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	cond                  sync.Cond
	kvMaps                [shardctrler.NShards]map[string]string
	maxAppliedOpIdOfClerk map[int64]int
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	op := Op{OpType: GetOp, Key: args.Key, ClerkId: args.ClerkId, OpId: args.OpId}
	_, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	} else {
		DPrintf("[%d] start get %s", kv.me, args.Key)
		reply.Err = OK
		kv.mu.Lock()
		for kv.maxAppliedOpIdOfClerk[args.ClerkId] < args.OpId {
			kv.mu.Unlock()
			appliedOneOp := make(chan struct{})
			go func() {
				kv.mu.Lock()
				kv.cond.Wait()
				close(appliedOneOp)
				kv.mu.Unlock()
			}()
			select {
			case <-appliedOneOp:
			case <-time.After(100 * time.Millisecond):
				reply.Err = ErrWrongLeader
			}
			if reply.Err == ErrWrongLeader {
				break
			}
			kv.mu.Lock()
		}
		if reply.Err != ErrWrongLeader {
			reply.Value = kv.kvMaps[key2shard(args.Key)][args.Key]
			kv.mu.Unlock()
		}
		return
	}
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	op := Op{OpType: PutOp, Key: args.Key, Value: args.Value, ClerkId: args.ClerkId, OpId: args.OpId}
	if args.Op == "Append" {
		op.OpType = AppendOp
	}
	_, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	} else {
		DPrintf("[%d] start %s %s:%s", kv.me, args.Op, args.Key, args.Value)
		reply.Err = OK
		kv.mu.Lock()
		for kv.maxAppliedOpIdOfClerk[args.ClerkId] < args.OpId {
			kv.mu.Unlock()
			appliedOneOp := make(chan struct{})
			go func() {
				kv.mu.Lock()
				kv.cond.Wait()
				close(appliedOneOp)
				kv.mu.Unlock()
			}()
			select {
			case <-appliedOneOp:
			case <-time.After(100 * time.Millisecond):
				reply.Err = ErrWrongLeader
			}
			if reply.Err == ErrWrongLeader {
				break
			}
			kv.mu.Lock()
		}
		if reply.Err == OK {
			kv.mu.Unlock()
		}
		return
	}
}

func (kv *ShardKV) GetShard(args *GetShardArgs, reply *GetShardReply) {
	op := Op{OpType: GetShardOp, Shard: args.Shard}
	_, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	} else {
		reply.Err = OK
		kv.mu.Lock()
		for kv.maxAppliedOpIdOfClerk[args.ClerkId] < args.OpId {
			kv.mu.Unlock()
			appliedOneOp := make(chan struct{})
			go func() {
				kv.mu.Lock()
				kv.cond.Wait()
				close(appliedOneOp)
				kv.mu.Unlock()
			}()
			select {
			case <-appliedOneOp:
			case <-time.After(100 * time.Millisecond):
				reply.Err = ErrWrongLeader
			}
			if reply.Err == ErrWrongLeader {
				break
			}
			kv.mu.Lock()
		}
		if reply.Err != ErrWrongLeader {
			reply.KvMap = make(map[string]string)
			for k, v := range kv.kvMaps[args.Shard] {
				reply.KvMap[k] = v
			}
			kv.mu.Unlock()
		}
		return
	}
}

func (kv *ShardKV) ReplaceShard(args *ReplaceShardArgs, reply *ReplaceShardReply) {
	op := Op{OpType: ReplaceShardOp, Shard: args.Shard, KvMap: args.KvMap}
	_, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	} else {
		reply.Err = OK
		kv.mu.Lock()
		for kv.maxAppliedOpIdOfClerk[args.ClerkId] < args.OpId {
			kv.mu.Unlock()
			appliedOneOp := make(chan struct{})
			go func() {
				kv.mu.Lock()
				kv.cond.Wait()
				close(appliedOneOp)
				kv.mu.Unlock()
			}()
			select {
			case <-appliedOneOp:
			case <-time.After(100 * time.Millisecond):
				reply.Err = ErrWrongLeader
			}
			if reply.Err == ErrWrongLeader {
				break
			}
			kv.mu.Lock()
		}
		if reply.Err == OK {
			kv.mu.Unlock()
		}
		return
	}
}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.
	for i := 0; i < shardctrler.NShards; i++ {
		kv.kvMaps[i] = make(map[string]string)
	}
	kv.maxAppliedOpIdOfClerk = make(map[int64]int)
	kv.cond = sync.Cond{L: &kv.mu}

	// Use something like this to talk to the shardctrler:
	// kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.readSnapshot(persister.ReadSnapshot())

	go kv.apply()
	return kv
}

func (kv *ShardKV) apply() {
	for {
		if kv.rf.Killed() {
			return
		}
		applyMsg := <-kv.applyCh
		if !applyMsg.CommandValid {
			kv.readSnapshot(applyMsg.Snapshot)
			continue
		}
		op := applyMsg.Command.(Op)
		kv.mu.Lock()
		if op.OpId <= kv.maxAppliedOpIdOfClerk[op.ClerkId] {
			kv.mu.Unlock()
			continue
		}
		if op.OpType == GetOp {
			DPrintf("[%d] get %s:%s", kv.me, op.Key, kv.kvMaps[key2shard(op.Key)][op.Key])
		} else if op.OpType == PutOp {
			kv.kvMaps[key2shard(op.Key)][op.Key] = op.Value
			DPrintf("[%d] put %s:%s", kv.me, op.Key, op.Value)
		} else if op.OpType == AppendOp {
			kv.kvMaps[key2shard(op.Key)][op.Key] += op.Value
			DPrintf("[%d] append %s:%s,kvmap[%s]:%s", kv.me, op.Key, op.Value, op.Key, kv.kvMaps[key2shard(op.Key)][op.Key])
		} else if op.OpType == NopOp {
			// do nothing
			DPrintf("[%d] nop", kv.me)
		} else if op.OpType == GetShardOp {
			DPrintf("[%d] getshard %d", kv.me, op.Shard)
		} else if op.OpType == ReplaceShardOp {
			DPrintf("[%d] replaceshard %d", kv.me, op.Shard)
			kv.kvMaps[op.Shard] = make(map[string]string)
			for k, v := range op.KvMap {
				kv.kvMaps[op.Shard][k] = v
			}
		} else {
			log.Fatal("Unknown OpType")
		}
		if kv.maxAppliedOpIdOfClerk[op.ClerkId] < op.OpId {
			kv.maxAppliedOpIdOfClerk[op.ClerkId] = op.OpId
		}
		kv.cond.Broadcast()
		if kv.maxraftstate != -1 && kv.rf.GetRaftStateSize() > kv.maxraftstate {
			w := new(bytes.Buffer)
			e := labgob.NewEncoder(w)
			e.Encode(kv.kvMaps)
			e.Encode(kv.maxAppliedOpIdOfClerk)
			data := w.Bytes()
			kv.rf.Snapshot(applyMsg.CommandIndex, data)
		}
		kv.mu.Unlock()
	}
}

func (kv *ShardKV) GetRaftStateSize() int {
	return kv.rf.GetRaftStateSize()
}
