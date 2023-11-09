package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client first talks to the shardctrler to find out
// the assignment of shards (keys) to groups, and then
// talks to the group that holds the key's shard.
//

import (
	"crypto/rand"
	"math/big"
	"sync"
	"time"

	"6.5840/labrpc"
	"6.5840/shardctrler"
)

// which shard is a key in?
// please use this function,
// and please do not change it.
func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardctrler.NShards
	return shard
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

type Clerk struct {
	sm       *shardctrler.Clerk
	config   shardctrler.Config
	make_end func(string) *labrpc.ClientEnd
	// You will have to modify this struct.
	clerkId       int64
	nextOpId      int
	waitForOp     sync.WaitGroup
	waitForConfig sync.WaitGroup
}

func (ck *Clerk) getNextOpId() int {
	nextOpIdBackup := ck.nextOpId
	ck.nextOpId++
	return nextOpIdBackup
}

// the tester calls MakeClerk.
//
// ctrlers[] is needed to call shardctrler.MakeClerk().
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs.
func MakeClerk(ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.sm = shardctrler.MakeClerk(ctrlers)
	ck.make_end = make_end
	// You'll have to add code here.
	ck.clerkId = nrand()
	ck.nextOpId = 1
	go ck.detectConfigChange()
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
// You will have to modify this function.
func (ck *Clerk) Get(key string) string {
	DPrintf("[%d] Clerk Get %s", ck.clerkId, key)
	ck.waitForConfig.Wait()
	ck.waitForOp.Add(1)
	defer ck.waitForOp.Done()
	args := GetArgs{}
	args.Key = key
	args.ClerkId = ck.clerkId
	args.OpId = ck.getNextOpId()

	for {
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			// try each server for the shard.
			for si := 0; si < len(servers); si++ {
				srv := ck.make_end(servers[si])
				var reply GetReply
				DPrintf("[%d] Clerk call Get %s to %s", ck.clerkId, key, servers[si])
				ok := srv.Call("ShardKV.Get", &args, &reply)
				if ok && (reply.Err == OK || reply.Err == ErrNoKey) {
					return reply.Value
				}
				if ok && (reply.Err == ErrWrongGroup) {
					break
				}
				// ... not ok, or ErrWrongLeader
			}
		}
		ck.waitForOp.Done()
		time.Sleep(100 * time.Millisecond)
		ck.waitForConfig.Wait()
		ck.waitForOp.Add(1)
		// ask controler for the latest configuration.
		//ck.config = ck.sm.Query(-1)
	}
	return ""
}

// shared by Put and Append.
// You will have to modify this function.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	DPrintf("[%d] Clerk %s %s:%s", ck.clerkId, op, key, value)
	ck.waitForConfig.Wait()
	ck.waitForOp.Add(1)
	defer ck.waitForOp.Done()
	args := PutAppendArgs{}
	args.Key = key
	args.Value = value
	args.Op = op
	args.ClerkId = ck.clerkId
	args.OpId = ck.getNextOpId()

	for {
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			for si := 0; si < len(servers); si++ {
				srv := ck.make_end(servers[si])
				var reply PutAppendReply
				DPrintf("[%d] Clerk call %s %s:%s to %s", ck.clerkId, op, key, value, servers[si])
				ok := srv.Call("ShardKV.PutAppend", &args, &reply)
				if ok && reply.Err == OK {
					return
				}
				if ok && reply.Err == ErrWrongGroup {
					break
				}
				// ... not ok, or ErrWrongLeader
			}
		}
		ck.waitForOp.Done()
		time.Sleep(100 * time.Millisecond)
		ck.waitForConfig.Wait()
		ck.waitForOp.Add(1)
		// ask controler for the latest configuration.
		//ck.config = ck.sm.Query(-1)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}

func (ck *Clerk) GetShard(config *shardctrler.Config, shard int) map[string]string {
	DPrintf("[%d] Clerk GetShard %d", ck.clerkId, shard)
	args := GetShardArgs{}
	args.Shard = shard
	args.ClerkId = ck.clerkId
	args.OpId = ck.getNextOpId()
	for {
		gid := config.Shards[shard]
		if servers, ok := config.Groups[gid]; ok {
			for si := 0; si < len(servers); si++ {
				srv := ck.make_end(servers[si])
				var reply GetShardReply
				ok := srv.Call("ShardKV.GetShard", &args, &reply)
				if ok && reply.Err == OK {
					return reply.KvMap
				}
				if ok && reply.Err == ErrWrongGroup {
					break
				}
				// ... not ok, or ErrWrongLeader
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask controler for the latest configuration.
		//ck.config = ck.sm.Query(-1)
	}
	return nil
}
func (ck *Clerk) ReplaceShard(config *shardctrler.Config, shard int, kv map[string]string) {
	DPrintf("[%d] Clerk ReplaceShard %d", ck.clerkId, shard)
	args := ReplaceShardArgs{}
	args.Shard = shard
	args.KvMap = kv
	args.ClerkId = ck.clerkId
	args.OpId = ck.getNextOpId()
	for {
		gid := config.Shards[shard]
		if servers, ok := config.Groups[gid]; ok {
			for si := 0; si < len(servers); si++ {
				srv := ck.make_end(servers[si])
				var reply ReplaceShardReply
				ok := srv.Call("ShardKV.ReplaceShard", &args, &reply)
				if ok && reply.Err == OK {
					return
				}
				if ok && reply.Err == ErrWrongGroup {
					break
				}
				// ... not ok, or ErrWrongLeader
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask controler for the latest configuration.
		//ck.config = ck.sm.Query(-1)
	}
}
func (ck *Clerk) detectConfigChange() {
	for {
		newConfig := ck.sm.Query(-1)
		if newConfig.Num != ck.config.Num {
			ck.waitForConfig.Add(1)
			ck.waitForOp.Wait()
			for i := 0; i < shardctrler.NShards; i++ {
				if ck.config.Shards[i] != newConfig.Shards[i] && ck.config.Shards[i] != 0 && newConfig.Shards[i] != 0 {
					kv:=ck.GetShard(&ck.config, i)
					ck.ReplaceShard(&newConfig, i, kv)
					ck.ReplaceShard(&ck.config, i, nil)
				}
			}
			ck.config = newConfig
			ck.waitForConfig.Done()
		}
		time.Sleep(100 * time.Millisecond)
	}
}
