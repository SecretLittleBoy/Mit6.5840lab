package raft
import (
)

type LogEntry struct {
	Command interface{}
	Term    int
	Index   int
}

