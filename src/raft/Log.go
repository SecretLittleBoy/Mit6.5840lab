package raft
import (
)

type LogEntry struct {
	Command interface{}
	Term    int
	Index   int //新的term不会使index重新开始编号。
}

