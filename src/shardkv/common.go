package shardkv

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
	ErrConfigTooOld = "ErrConfigTooOld"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClerkId int64
	OpId    int
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClerkId int64
	OpId    int
}

type GetReply struct {
	Err   Err
	Value string
}

type GetShardArgs struct {
	Shard   int
	Gid     int
	ClerkId int64
	OpId    int
}

type GetShardReply struct {
	Err   Err
	KvMap map[string]string
}

type DeleteShardArgs struct {
	Shard   int
	ClerkId int64
	OpId    int
}

type DeleteShardReply struct {
	Err Err
}

type replaceSharkArgs struct {
	Shard   int
	KvMap   map[string]string
	ClerkId int64
	OpId    int
}

type replaceSharkReply struct {
	Err Err
}

type UpdateConfigArgs struct {
	ConfigNum int
	ClerkId   int64
	OpId      int
}

type UpdateConfigReply struct {
	Err Err
}

type NopOpArgs struct {
	ClerkId int64
	OpId    int
}

type NopOpReply struct {
	Err Err
}