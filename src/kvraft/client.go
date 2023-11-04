package kvraft

import (
	"crypto/rand"
	"math/big"
	"time"

	"6.5840/labrpc"
)
const retryInterval = 100 * time.Millisecond
type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	clerkId  int64
	nextOpId int
	leader   int
}

func (ck *Clerk) getNextOpId() int {
	nextOpIdBackup := ck.nextOpId
	ck.nextOpId++
	return nextOpIdBackup
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.clerkId = nrand()
	ck.nextOpId = 1
	ck.leader = 0
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	args := GetArgs{key, ck.clerkId, ck.getNextOpId()}
	for {
		for i := 0; i < len(ck.servers); i++ {
			sendToWho := (ck.leader + i) % len(ck.servers)
			reply := GetReply{}
			ok := ck.servers[sendToWho].Call("KVServer.Get", &args, &reply)
			if ok && reply.Err == OK {
				ck.leader = sendToWho
				return reply.Value
			}
		}
		time.Sleep(retryInterval)
	}
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	args := PutAppendArgs{key, value, op, ck.clerkId, ck.getNextOpId()}
	for {
		for i := 0; i < len(ck.servers); i++ {
			sendToWho := (ck.leader + i) % len(ck.servers)
			reply := PutAppendReply{}
			ok := ck.servers[sendToWho].Call("KVServer.PutAppend", &args, &reply)
			if ok && reply.Err == OK {
				ck.leader = sendToWho
				return
			}
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
