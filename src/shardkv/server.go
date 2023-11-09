package shardkv

import (
	"bytes"
	"fmt"
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
	sm           *shardctrler.Clerk
	config       shardctrler.Config
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	cond sync.Cond

	//snapshot data begin
	kvMaps                [shardctrler.NShards]map[string]string
	shardIhave            [shardctrler.NShards]bool
	maxAppliedOpIdOfClerk map[int64]int
	//snapshot data end

	waitForConfig sync.WaitGroup
	waitForOp     sync.WaitGroup
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	DPrintf("[%d] get %s. waitForConfig", kv.me, args.Key)
	kv.waitForConfig.Wait()
	kv.waitForOp.Add(1)
	defer kv.waitForOp.Done()
	if _, isleader := kv.rf.GetState(); isleader && !kv.shardIhave[key2shard(args.Key)] {
		reply.Err = ErrWrongGroup
		return
	}
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
	DPrintf("[%d] %s %s:%s. waitForConfig", kv.me, args.Op, args.Key, args.Value)
	kv.waitForConfig.Wait()
	kv.waitForOp.Add(1)
	defer kv.waitForOp.Done()
	if _, isleader := kv.rf.GetState(); isleader && !kv.shardIhave[key2shard(args.Key)] {
		reply.Err = ErrWrongGroup
		return
	}
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
	kv.sm = shardctrler.MakeClerk(ctrlers)

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
	go kv.detectConfigChange()
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
			DPrintf("[%d] apply get %s:%s", kv.me, op.Key, kv.kvMaps[key2shard(op.Key)][op.Key])
		} else if op.OpType == PutOp {
			kv.kvMaps[key2shard(op.Key)][op.Key] = op.Value
			DPrintf("[%d] apply put %s:%s", kv.me, op.Key, op.Value)
		} else if op.OpType == AppendOp {
			kv.kvMaps[key2shard(op.Key)][op.Key] += op.Value
			DPrintf("[%d] apply append %s:%s,kvmap[%s]:%s", kv.me, op.Key, op.Value, op.Key, kv.kvMaps[key2shard(op.Key)][op.Key])
		} else if op.OpType == NopOp {
			// do nothing
			DPrintf("[%d] apply nop", kv.me)
		} else {
			log.Fatal("apply Unknown OpType")
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
			e.Encode(kv.shardIhave)
			data := w.Bytes()
			kv.rf.Snapshot(applyMsg.CommandIndex, data)
		}
		kv.mu.Unlock()
	}
}

func (kv *ShardKV) GetRaftStateSize() int {
	return kv.rf.GetRaftStateSize()
}

func (kv *ShardKV) detectConfigChange() {
	for {
		if kv.rf.Killed() {
			return
		}
		var isLeader bool
		if _, isLeader = kv.rf.GetState(); !isLeader {
			DPrintf("[%d] isLeader:%v", kv.me, isLeader)
			time.Sleep(100 * time.Millisecond)
			continue
		}
		DPrintf("[%d] isLeader:%v", kv.me, isLeader)
		newConfig := kv.sm.Query(-1)
		if newConfig.Num != kv.config.Num {
			fmt.Println(newConfig)
			kv.waitForConfig.Add(1)
			DPrintf("[%d] waitForOp", kv.me)
			kv.waitForOp.Wait()
			//todo
			kv.config = newConfig
			for i := 0; i < shardctrler.NShards; i++ {
				if kv.config.Shards[i] == kv.gid {
					kv.shardIhave[i] = true
				} else {
					//kv.shardIhave[i] = false
				}
			}
			fmt.Printf("[%d] shardIhave:%v\n", kv.me, kv.shardIhave)
			//todo
			kv.waitForConfig.Done()
			DPrintf("[%d] done waitForConfig", kv.me)
		}
		time.Sleep(100 * time.Millisecond)
	}
}
