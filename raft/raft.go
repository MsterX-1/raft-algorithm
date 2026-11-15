package raft

import (
	"lab2raft/labrpc"
	"math/rand"
	"sync"
	"time"
)

type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool
	Snapshot    []byte
}
type LogEntry struct {
	Command interface{}
	Term    int
}
type role int

const (
	follower role = iota
	candidate
	leader
)

type Raft struct {
	mu                              sync.Mutex
	peers                           []*labrpc.ClientEnd
	me                              int
	role                            role
	currentTerm, votedFor           int
	log                             []LogEntry // Index zero is a sentinel.
	commitIndex, lastApplied        int
	matchIndex                      []int
	electionDeadline, nextHeartbeat time.Time
	applyReady                      *sync.Cond
	applyCh                         chan ApplyMsg
	done                            chan struct{}
	stopped                         bool
}

func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.role == leader && !rf.stopped
}

type RequestVoteArgs struct{ Term, CandidateId, LastLogIndex, LastLogTerm int }
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}
type AppendEntriesArgs struct {
	Term, LeaderId, PrevLogIndex, PrevLogTerm int
	Entries                                   []LogEntry
	LeaderCommit                              int
}
type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) resetElectionLocked() {
	rf.electionDeadline = time.Now().Add(time.Duration(150+rand.Intn(151)) * time.Millisecond)
}
func (rf *Raft) observeTermLocked(term int) {
	if term > rf.currentTerm {
		rf.currentTerm, rf.votedFor, rf.role = term, -1, follower
	}
}
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.observeTermLocked(args.Term)
	reply.Term = rf.currentTerm
	last := len(rf.log) - 1
	lastTerm := rf.log[last].Term
	fresh := args.LastLogTerm > lastTerm || (args.LastLogTerm == lastTerm && args.LastLogIndex >= last)
	reply.VoteGranted = !rf.stopped && args.Term == rf.currentTerm && fresh && (rf.votedFor == -1 || rf.votedFor == args.CandidateId)
	if reply.VoteGranted {
		rf.votedFor = args.CandidateId
		rf.resetElectionLocked()
	}
}
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.observeTermLocked(args.Term)
	reply.Term = rf.currentTerm
	reply.Success = false
	if rf.stopped || args.Term < rf.currentTerm {
		return
	}
	rf.role = follower
	rf.resetElectionLocked()
	// Member 2: check the prefix and reconcile entries here. On success call
	// advanceFollowerCommitLocked(args.LeaderCommit, args.PrevLogIndex+len(args.Entries)).
}
func (rf *Raft) startElectionLocked() {
	rf.role = candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.resetElectionLocked()
	last := len(rf.log) - 1
	args := RequestVoteArgs{Term: rf.currentTerm, CandidateId: rf.me, LastLogIndex: last, LastLogTerm: rf.log[last].Term}
	votes := 1
	if len(rf.peers) == 1 {
		rf.becomeLeaderLocked()
		return
	}
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		go func(peer int) {
			var reply RequestVoteReply
			if !rf.sendRequestVote(peer, &args, &reply) {
				return
			}
			rf.mu.Lock()
			defer rf.mu.Unlock()
			rf.observeTermLocked(reply.Term)
			if rf.stopped || rf.role != candidate || rf.currentTerm != args.Term || !reply.VoteGranted {
				return
			}
			votes++
			if votes > len(rf.peers)/2 {
				rf.becomeLeaderLocked()
			}
		}(peer)
	}
}
func (rf *Raft) becomeLeaderLocked() {
	rf.role = leader
	rf.matchIndex = make([]int, len(rf.peers))
	rf.heartbeatLocked()
}
func (rf *Raft) heartbeatLocked() {
	rf.nextHeartbeat = time.Now().Add(110 * time.Millisecond)
	last := len(rf.log) - 1
	args := AppendEntriesArgs{Term: rf.currentTerm, LeaderId: rf.me, PrevLogIndex: last, PrevLogTerm: rf.log[last].Term, LeaderCommit: rf.commitIndex}
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		go func(peer int) {
			var reply AppendEntriesReply
			if rf.sendAppendEntries(peer, &args, &reply) {
				rf.mu.Lock()
				rf.observeTermLocked(reply.Term)
				rf.mu.Unlock()
			}
		}(peer)
	}
}
func (rf *Raft) tick() {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-rf.done:
			return
		case now := <-ticker.C:
			rf.mu.Lock()
			if !rf.stopped {
				if rf.role == leader {
					if !now.Before(rf.nextHeartbeat) {
						rf.heartbeatLocked()
					}
				} else if !now.Before(rf.electionDeadline) {
					rf.startElectionLocked()
				}
			}
			rf.mu.Unlock()
		}
	}
}

// Member 2 calls this under rf.mu after a local append or matchIndex update.
func (rf *Raft) advanceLeaderCommitLocked() {
	if rf.role != leader {
		return
	}
	for n := len(rf.log) - 1; n > rf.commitIndex && rf.log[n].Term == rf.currentTerm; n-- {
		replicas := 1
		for peer, matched := range rf.matchIndex {
			if peer != rf.me && matched >= n {
				replicas++
			}
		}
		if replicas > len(rf.peers)/2 {
			rf.setCommitLocked(n)
			return
		}
	}
}
func (rf *Raft) advanceFollowerCommitLocked(leaderCommit, lastMatched int) {
	rf.setCommitLocked(min(leaderCommit, lastMatched, len(rf.log)-1))
}
func (rf *Raft) setCommitLocked(index int) {
	if index > rf.commitIndex {
		rf.commitIndex = index
		rf.applyReady.Signal()
	}
}
func (rf *Raft) applyCommitted() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for {
		for !rf.stopped && rf.lastApplied == rf.commitIndex {
			rf.applyReady.Wait()
		}
		if rf.stopped {
			return
		}
		index := rf.lastApplied + 1
		msg := ApplyMsg{Index: index, Command: rf.log[index].Command}
		rf.mu.Unlock()
		select {
		case rf.applyCh <- msg:
			rf.mu.Lock()
			rf.lastApplied = index
		case <-rf.done:
			rf.mu.Lock()
			return
		}
	}
}
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	return rf.peers[server].Call("Raft.RequestVote", args, reply)
}
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	return rf.peers[server].Call("Raft.AppendEntries", args, reply)
}

// Start is the Member 2 replication entry point.
func (rf *Raft) Start(command interface{}) (int, int, bool) { return -1, -1, false }
func (rf *Raft) Kill() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if !rf.stopped {
		rf.stopped = true
		close(rf.done)
		rf.applyReady.Broadcast()
	}
}
func Make(peers []*labrpc.ClientEnd, me int, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{peers: peers, me: me, votedFor: -1, log: []LogEntry{{}}, applyCh: applyCh, done: make(chan struct{})}
	rf.applyReady = sync.NewCond(&rf.mu)
	rf.resetElectionLocked()
	go rf.tick()
	go rf.applyCommitted()
	return rf
}
