package kvraft

import (
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	OpType  string
	Key     string
	Value   string
	ClerkId int64
	OpId    int
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	cond sync.Cond

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	kvMap                 map[string]string
	maxAppliedOpIdOfClerk map[int64]int
}

/*
如果键/值服务将操作提交到其Raft日志（并因此将操作应用于键/值状态机），则领导者通过响应其RPC将结果报告给 Clerk 。
如果操作未能提交（例如，如果领导者被替换），服务器会报告错误，并且 Clerk 会使用不同的服务器重试。
After calling Start(), your kvservers will need to wait for Raft to complete agreement.
Commands that have been agreed upon arrive on the applyCh.
Your code will need to keep reading applyCh
while PutAppend() and Get() handlers submit commands to the Raft log using Start().
*/
func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	op := Op{OpType: GET, Key: args.Key, ClerkId: args.ClerkId, OpId: args.OpId}
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
			reply.Value = kv.kvMap[args.Key]
			kv.mu.Unlock()
		}
		return
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	op := Op{OpType: args.Op, Key: args.Key, Value: args.Value, ClerkId: args.ClerkId, OpId: args.OpId}
	_, _, isLeader := kv.rf.Start(op)
	DPrintf("[%d] start %s %s:%s", kv.me, args.Op, args.Key, args.Value)
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

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.cond = sync.Cond{L: &kv.mu}

	// You may need initialization code here.
	kv.kvMap = make(map[string]string)
	kv.maxAppliedOpIdOfClerk = make(map[int64]int)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.readSnapshot(persister.ReadSnapshot())

	go kv.apply()
	return kv
}

func (kv *KVServer) apply() {
	for {
		if kv.killed() {
			return
		}
		applyMsg := <-kv.applyCh
		if !applyMsg.CommandValid {
			kv.readSnapshot(applyMsg.Snapshot)
			continue
		}
		op := applyMsg.Command.(Op)
		DPrintf("[%d] applying %s %s:%s. ClerkId:%d,OpId:%d", kv.me, op.OpType, op.Key, op.Value, op.ClerkId, op.OpId)
		DPrintf("[%d] maxAppliedOpIdOfClerk[%d]:%d", kv.me, op.ClerkId, kv.maxAppliedOpIdOfClerk[op.ClerkId])
		kv.mu.Lock()
		if op.OpId <= kv.maxAppliedOpIdOfClerk[op.ClerkId] {
			kv.mu.Unlock()
			continue
		}
		if op.OpType == GET {
			DPrintf("[%d] get %s:%s", kv.me, op.Key, kv.kvMap[op.Key])
		} else if op.OpType == PUT {
			kv.kvMap[op.Key] = op.Value
			DPrintf("[%d] put %s:%s", kv.me, op.Key, op.Value)
		} else if op.OpType == APPEND {
			kv.kvMap[op.Key] += op.Value
			DPrintf("[%d] append %s:%s,kvmap[%s]:%s", kv.me, op.Key, op.Value, op.Key, kv.kvMap[op.Key])
		} else if op.OpType == NOP {
			// do nothing
			DPrintf("[%d] nop", kv.me)
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
			e.Encode(kv.kvMap)
			e.Encode(kv.maxAppliedOpIdOfClerk)
			data := w.Bytes()
			kv.rf.Snapshot(applyMsg.CommandIndex, data)
		}
		kv.mu.Unlock()
	}
}

func (kv *KVServer) GetRaftStateSize() int {
	return kv.rf.GetRaftStateSize()
}