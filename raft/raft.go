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
	nextIndex                       []int
	replicating                     []bool // One in-flight AppendEntries per follower in this term.
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
	Term          int
	Success       bool
	ConflictIndex int
}

func (rf *Raft) resetElectionLocked() {
	rf.electionDeadline = time.Now().Add(time.Duration(150+rand.Intn(151)) * time.Millisecond)
}//150,151
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
	if args.PrevLogIndex >= len(rf.log) {
		reply.ConflictIndex = len(rf.log)
		return
	}
	if term := rf.log[args.PrevLogIndex].Term; term != args.PrevLogTerm {
		first := args.PrevLogIndex
		for first > 0 && rf.log[first-1].Term == term {
			first--
		}
		reply.ConflictIndex = first
		return
	}
	for offset, entry := range args.Entries {
		index := args.PrevLogIndex + 1 + offset
		if index == len(rf.log) || rf.log[index].Term != entry.Term {
			rf.log = append(rf.log[:index], args.Entries[offset:]...)
			break
		}
	}
	rf.advanceFollowerCommitLocked(args.LeaderCommit, args.PrevLogIndex+len(args.Entries))
	reply.Success = true
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
	rf.nextIndex = make([]int, len(rf.peers))
	rf.replicating = make([]bool, len(rf.peers))
	for peer := range rf.peers {
		rf.nextIndex[peer] = len(rf.log)
	}
	rf.matchIndex[rf.me] = len(rf.log) - 1
	rf.heartbeatLocked()
}
func (rf *Raft) heartbeatLocked() {
    rf.nextHeartbeat = time.Now().Add(110 * time.Millisecond)

    for peer := range rf.peers {
        if peer == rf.me || !rf.replicating[peer] {
            continue
        }

        // A replication RPC is still pending. Send this follower
        // a heartbeat using the last prefix it confirmed.
        prev := rf.matchIndex[peer]
        args := AppendEntriesArgs{
            Term:         rf.currentTerm,
            LeaderId:     rf.me,
            PrevLogIndex: prev,
            PrevLogTerm:  rf.log[prev].Term,
            LeaderCommit: rf.commitIndex,
        }

        go func(peer int, args AppendEntriesArgs) {
            var reply AppendEntriesReply
            if !rf.sendAppendEntries(peer, &args, &reply) {
                return
            }

            rf.mu.Lock()
            defer rf.mu.Unlock()
            rf.observeTermLocked(reply.Term)
        }(peer, args)
    }

    // Followers with no pending RPC use the existing replication path.
    rf.scheduleReplicationLocked()
}

func (rf *Raft) scheduleReplicationLocked() {
	for peer := range rf.peers {
		if peer == rf.me || rf.replicating[peer] {
			continue
		}
		rf.replicating[peer] = true
		go rf.replicate(peer, rf.currentTerm)
	}
}

func (rf *Raft) replicate(peer, term int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for !rf.stopped && rf.role == leader && rf.currentTerm == term {
		next := rf.nextIndex[peer]
		args := AppendEntriesArgs{
			Term: term, LeaderId: rf.me, PrevLogIndex: next - 1,
			PrevLogTerm: rf.log[next-1].Term, LeaderCommit: rf.commitIndex,
			Entries: append([]LogEntry(nil), rf.log[next:]...),
		}
		rf.mu.Unlock()
		var reply AppendEntriesReply
		ok := rf.sendAppendEntries(peer, &args, &reply)
		rf.mu.Lock()
		if ok {
			rf.observeTermLocked(reply.Term)
		}
		// A new leader term owns a fresh set of progress and in-flight flags.
		if rf.stopped || rf.role != leader || rf.currentTerm != term {
			return
		}
		if !ok {
			break // The next heartbeat or command retries this follower.
		}
		if !reply.Success {
			rf.nextIndex[peer] = max(1, reply.ConflictIndex)
			continue
		}
		matched := args.PrevLogIndex + len(args.Entries)
		rf.matchIndex[peer] = matched
		rf.nextIndex[peer] = matched + 1
		rf.advanceLeaderCommitLocked()
		if rf.nextIndex[peer] == len(rf.log) {
			break
		}
	}
	if rf.currentTerm == term {
		rf.replicating[peer] = false
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

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.stopped || rf.role != leader {
		return -1, rf.currentTerm, false
	}
	index := len(rf.log)
	rf.log = append(rf.log, LogEntry{Command: command, Term: rf.currentTerm})
	rf.matchIndex[rf.me] = index
	rf.advanceLeaderCommitLocked()
	rf.scheduleReplicationLocked()
	return index, rf.currentTerm, true
}
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
