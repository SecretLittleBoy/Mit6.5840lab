package shardkv
import (
	"bytes"
	"6.5840/labgob"
	"log"
	//"6.5840/labrpc"
	//"6.5840/raft"
)

func (kv *ShardKV) readSnapshot(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var kvMap map[string]string
	var maxAppliedOpIdOfClerk map[int64]int
	if d.Decode(&kvMap) != nil ||
		d.Decode(&maxAppliedOpIdOfClerk) != nil {
		log.Fatal("Error when decoding snapshot")
	} else {
		kv.kvMap = kvMap
		kv.maxAppliedOpIdOfClerk = maxAppliedOpIdOfClerk
	}
}