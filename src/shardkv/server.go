package shardkv

import (
	"bytes"
	//"fmt"
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
	DeleteShardOp
	ReplaceShardOp
	UpdateConfigOp
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
	Gid   int
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
	lastGetShardGid       [shardctrler.NShards]int
	//snapshot data end

	waitForConfig sync.WaitGroup
	waitForOp     sync.WaitGroup

	ClerkId  int64
	nextOpId int
}

func (kv *ShardKV) getNextOpId() int {
	nextOpIdBackup := kv.nextOpId
	kv.nextOpId++
	return nextOpIdBackup
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	DPrintf("[%d:%d] get %s. waitForConfig", kv.gid, kv.me, args.Key)
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
		DPrintf("[%d:%d] start get %s", kv.gid, kv.me, args.Key)
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
	DPrintf("[%d:%d] %s %s:%s. waitForConfig", kv.gid, kv.me, args.Op, args.Key, args.Value)
	kv.waitForConfig.Wait()
	kv.waitForOp.Add(1)
	defer kv.waitForOp.Done()
	if _, isleader := kv.rf.GetState(); isleader && !kv.shardIhave[key2shard(args.Key)] {
		reply.Err = ErrWrongGroup
		DPrintf("[%d:%d] %s %s:%s. ErrWrongGroup", kv.gid, kv.me, args.Op, args.Key, args.Value)
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
		DPrintf("[%d:%d] start %s %s:%s", kv.gid, kv.me, args.Op, args.Key, args.Value)
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

	kv.nextOpId = 1
	kv.ClerkId = nrand()

	// Use something like this to talk to the shardctrler:
	// kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.readSnapshot(persister.ReadSnapshot())
	DPrintf("[%d:%d] Clerkid %d init, kvMaps:%v, maxAppliedOpIdOfClerk:%v, shardIhave:%v", kv.gid, kv.me, kv.ClerkId, kv.kvMaps, kv.maxAppliedOpIdOfClerk, kv.shardIhave)
	go kv.apply()

	go func() {
		NopOpArgs := NopOpArgs{ClerkId: kv.ClerkId, OpId: kv.getNextOpId()}
		NopOpReply := NopOpReply{}
		kv.NopOp(&NopOpArgs, &NopOpReply)
		for NopOpReply.Err == ErrWrongLeader {
			time.Sleep(100 * time.Millisecond)
			kv.NopOp(&NopOpArgs, &NopOpReply)
		}
		go kv.detectConfigChange()
	}()

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
			DPrintf("[%d:%d] apply get %s:%s", kv.gid, kv.me, op.Key, kv.kvMaps[key2shard(op.Key)][op.Key])
		} else if op.OpType == PutOp {
			kv.kvMaps[key2shard(op.Key)][op.Key] = op.Value
			DPrintf("[%d:%d] apply put %s:%s", kv.gid, kv.me, op.Key, op.Value)
			DPrintf("[%d:%d] apply put kvmap[%s]:%s", kv.gid, kv.me, op.Key, kv.kvMaps[key2shard(op.Key)][op.Key])
		} else if op.OpType == AppendOp {
			kv.kvMaps[key2shard(op.Key)][op.Key] += op.Value
			DPrintf("[%d:%d] apply append %s:%s,kvmap[%s]:%s", kv.gid, kv.me, op.Key, op.Value, op.Key, kv.kvMaps[key2shard(op.Key)][op.Key])
		} else if op.OpType == NopOp {
			// do nothing
			DPrintf("[%d:%d] apply nop", kv.gid, kv.me)
		} else if op.OpType == GetShardOp {
			DPrintf("[%d:%d] apply getShard %d", kv.gid, kv.me, op.Shard)
			kv.shardIhave[op.Shard] = false
			kv.lastGetShardGid[op.Shard] = op.Gid
		} else if op.OpType == DeleteShardOp {
			DPrintf("[%d:%d] apply deleteShard %d", kv.gid, kv.me, op.Shard)
			kv.kvMaps[op.Shard] = make(map[string]string)
			kv.lastGetShardGid[op.Shard] = 0
		} else if op.OpType == ReplaceShardOp {
			DPrintf("[%d:%d] apply Clerkid %d Opid %d replaceShard %d", kv.gid, kv.me, op.ClerkId, op.OpId, op.Shard)
			kv.kvMaps[op.Shard] = op.KvMap
			kv.shardIhave[op.Shard] = true
		} else if op.OpType == UpdateConfigOp {
			DPrintf("[%d:%d] apply updateConfig %d", kv.gid, kv.me, op.Shard)
			kv.config = kv.sm.Query(op.Shard)
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
			DPrintf("[%d:%d] isLeader:%v", kv.gid, kv.me, isLeader)
			time.Sleep(100 * time.Millisecond)
			continue
		}
		DPrintf("[%d:%d] isLeader:%v", kv.gid, kv.me, isLeader)
		newConfig := kv.sm.Query(-1)
		if newConfig.Num > kv.config.Num {
			DPrintf("[%d:%d] newConfig:%v", kv.gid, kv.me, newConfig)
			kv.waitForConfig.Add(1)
			DPrintf("[%d:%d] waitForOp", kv.gid, kv.me)
			kv.waitForOp.Wait()
			var updateConfigReply UpdateConfigReply
			var updateConfigArgs UpdateConfigArgs
			for i := 0; i < shardctrler.NShards; i++ { //遍历每个shard
				if newConfig.Shards[i] == kv.gid && kv.config.Shards[i] != kv.gid { //get new shard
					gid := kv.config.Shards[i]
					if gid == 0 {
						emptyKvMap := make(map[string]string)
						replaceSharkArg := replaceSharkArgs{Shard: i, KvMap: emptyKvMap, ClerkId: kv.ClerkId, OpId: kv.getNextOpId()}
						var replaceShardReply replaceSharkReply
						kv.ReplaceShark(&replaceSharkArg, &replaceShardReply)
						if replaceShardReply.Err == ErrWrongLeader {
							goto END //如果不是leader了，退出,config不变
						}
						continue
					}
					servers, ok := kv.config.Groups[gid]
					if !ok {
						servers, ok = newConfig.Groups[gid]
						if !ok {
							log.Fatal("detectConfigChange: can't find servers")
						}
					}
					si := -1
					var reply GetShardReply
					for {
						si = (si + 1) % len(servers)
						srv := kv.make_end(servers[si])
						args := GetShardArgs{Shard: i, ClerkId: kv.ClerkId, OpId: kv.getNextOpId(), Gid: kv.gid}
						ok := srv.Call("ShardKV.GetShard", &args, &reply) //先取回shard
						if ok && reply.Err == OK {
							replaceSharkArg := replaceSharkArgs{Shard: i, KvMap: reply.KvMap, ClerkId: kv.ClerkId, OpId: kv.getNextOpId()}
							var replaceShardReply replaceSharkReply
							kv.ReplaceShark(&replaceSharkArg, &replaceShardReply)
							if replaceShardReply.Err == ErrWrongLeader {
								goto END //如果不是leader了，退出,config不变
							}
							deleteShardArg := DeleteShardArgs{Shard: i, ClerkId: kv.ClerkId, OpId: kv.getNextOpId()}
							var deleteShardReply DeleteShardReply
							deleteShardOK := false
							for !deleteShardOK { //要求对方删除shard
								deleteShardOK = srv.Call("ShardKV.DeleteShard", &deleteShardArg, &deleteShardReply)
								si = (si + 1) % len(servers)
								srv = kv.make_end(servers[si])
							}
							break //一个shard处理完毕
						} else if ok && reply.Err == ErrWrongGroup {
							break
						}
					}
					if reply.Err == ErrWrongGroup {
						for gid := range kv.config.Groups {//遍历每个gid
							if gid != kv.gid {
								servers, ok := kv.config.Groups[gid]
								if !ok {
									servers, ok = newConfig.Groups[gid]
									if !ok {
										continue
									}
								}
								si := -1
								var reply GetShardReply
								for {
									si = (si + 1) % len(servers)
									srv := kv.make_end(servers[si])
									args := GetShardArgs{Shard: i, ClerkId: kv.ClerkId, OpId: kv.getNextOpId(), Gid: kv.gid}
									ok := srv.Call("ShardKV.GetShard", &args, &reply) //先取回shard
									if ok && reply.Err == OK {
										replaceSharkArg := replaceSharkArgs{Shard: i, KvMap: reply.KvMap, ClerkId: kv.ClerkId, OpId: kv.getNextOpId()}
										var replaceShardReply replaceSharkReply
										kv.ReplaceShark(&replaceSharkArg, &replaceShardReply)
										if replaceShardReply.Err == ErrWrongLeader {
											goto END //如果不是leader了，退出,config不变
										}
										deleteShardArg := DeleteShardArgs{Shard: i, ClerkId: kv.ClerkId, OpId: kv.getNextOpId()}
										var deleteShardReply DeleteShardReply
										deleteShardOK := false
										for !deleteShardOK { //要求对方删除shard
											deleteShardOK = srv.Call("ShardKV.DeleteShard", &deleteShardArg, &deleteShardReply)
											si = (si + 1) % len(servers)
											srv = kv.make_end(servers[si])
										}
										//break //一个shard处理完毕
										goto NEXT_SHARD
									} else if ok && reply.Err == ErrWrongGroup {
										break
									}
								}
							}
						}
					}
				}
				NEXT_SHARD:
			}
			updateConfigArgs = UpdateConfigArgs{ConfigNum: newConfig.Num, ClerkId: kv.ClerkId, OpId: kv.getNextOpId()}
			kv.UpdateConfig(&updateConfigArgs, &updateConfigReply) //更新config
			if updateConfigReply.Err == ErrWrongLeader {
				goto END //如果不是leader了，退出,config不变
			}
			kv.config = newConfig
			DPrintf("[%d:%d] shardIhave:%v", kv.gid, kv.me, kv.shardIhave)
			//todo
		END:
			kv.waitForConfig.Done()
			DPrintf("[%d:%d] done waitForConfig", kv.gid, kv.me)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// shard操作时不能对外服务，getShard需要立即设置shardIhave为false，停止对外服务。
// 但是不能立即删除shard，因为getShard的回复可能丢包
func (kv *ShardKV) GetShard(args *GetShardArgs, reply *GetShardReply) {
	if kv.lastGetShardGid[args.Shard] != 0 && kv.lastGetShardGid[args.Shard] != args.Gid {
		reply.Err = ErrWrongGroup
		return
	}
	if kvmap := kv.kvMaps[args.Shard]; len(kvmap) == 0 {
		reply.Err = ErrWrongGroup
	}
	op := Op{OpType: GetShardOp, Shard: args.Shard, ClerkId: args.ClerkId, OpId: args.OpId, Gid: args.Gid}
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
			reply.KvMap = kv.kvMaps[args.Shard]
			kv.mu.Unlock()
		}
		return
	}
}

func (kv *ShardKV) DeleteShard(args *DeleteShardArgs, reply *DeleteShardReply) {
	op := Op{OpType: DeleteShardOp, Shard: args.Shard, ClerkId: args.ClerkId, OpId: args.OpId}
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

func (kv *ShardKV) ReplaceShark(args *replaceSharkArgs, reply *replaceSharkReply) {
	op := Op{OpType: ReplaceShardOp, Shard: args.Shard, KvMap: args.KvMap, ClerkId: args.ClerkId, OpId: args.OpId}
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

func (kv *ShardKV) UpdateConfig(args *UpdateConfigArgs, reply *UpdateConfigReply) {
	DPrintf("[%d:%d] start updateConfig %d", kv.gid, kv.me, args.ConfigNum)
	op := Op{OpType: UpdateConfigOp, ClerkId: args.ClerkId, OpId: args.OpId, Shard: args.ConfigNum}
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

func (kv *ShardKV) NopOp(args *NopOpArgs, reply *NopOpReply) {
	op := Op{OpType: NopOp, ClerkId: args.ClerkId, OpId: args.OpId}
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
