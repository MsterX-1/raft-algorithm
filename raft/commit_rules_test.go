package raft

import (
	"lab2raft/labrpc"
	"sync"
	"testing"
	"time"
)

// Exercise commitment independently of the replication transport.
func TestCommitAndApplyRules(t *testing.T) {
	ch := make(chan ApplyMsg)
	rf := &Raft{role: leader, currentTerm: 2, peers: make([]*labrpc.ClientEnd, 3),
		log:        []LogEntry{{}, {Term: 1, Command: "old"}, {Term: 2, Command: "new"}, {Term: 2, Command: "tail"}},
		matchIndex: []int{0, 1, 1}, applyCh: ch, done: make(chan struct{})}
	rf.applyReady = sync.NewCond(&rf.mu)
	rf.advanceLeaderCommitLocked()
	if rf.commitIndex != 0 {
		t.Fatal("counted an old-term entry directly")
	}
	rf.matchIndex[1] = 2
	rf.advanceLeaderCommitLocked()
	if rf.commitIndex != 2 {
		t.Fatalf("majority committed through %d, want 2", rf.commitIndex)
	}
	rf.role = follower
	rf.advanceFollowerCommitLocked(3, 2)
	if rf.commitIndex != 2 {
		t.Fatal("committed an unmatched suffix")
	}
	go rf.applyCommitted()
	defer rf.Kill()
	for i, want := range []string{"old", "new"} {
		select {
		case msg := <-ch:
			if msg.Index != i+1 || msg.Command != want {
				t.Fatalf("unexpected apply: %+v", msg)
			}
		case <-time.After(time.Second):
			t.Fatal("apply timed out")
		}
	}
	rf.mu.Lock()
	rf.advanceFollowerCommitLocked(4, 3)
	rf.mu.Unlock()
	select {
	case msg := <-ch:
		if msg.Index != 3 || msg.Command != "tail" {
			t.Fatalf("unexpected apply: %+v", msg)
		}
	case <-time.After(time.Second):
		t.Fatal("follower apply timed out")
	}
}
