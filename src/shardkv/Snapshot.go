package shardkv

import (
	"6.5840/labgob"
	"bytes"
	"log"
	//"6.5840/labrpc"
	//"6.5840/raft"
	"6.5840/shardctrler"
)

func (kv *ShardKV) readSnapshot(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var kvMaps [shardctrler.NShards]map[string]string
	var maxAppliedOpIdOfClerk map[int64]int
	var shardIhave [shardctrler.NShards]bool
	if d.Decode(&kvMaps) != nil ||
		d.Decode(&maxAppliedOpIdOfClerk) != nil ||
		d.Decode(&shardIhave) != nil {
		log.Fatal("Error when decoding snapshot")
	} else {
		kv.kvMaps = kvMaps
		kv.maxAppliedOpIdOfClerk = maxAppliedOpIdOfClerk
		kv.shardIhave = shardIhave
	}
}
