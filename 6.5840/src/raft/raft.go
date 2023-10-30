package raft

import (
	"bytes"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
)

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	CommandTerm  int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// 状态常量
const (
	LEADER    int = 0
	CANDIDATE int = 1
	FOLLOWER  int = 2
)

type LogEntry struct {
	Term    int
	Index   int
	Command interface{}
}

type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	applyCh  chan ApplyMsg // 应用日志的 channel
	state    int           // 记录当前状态
	timeout  time.Time     // 选举计时器
	snapshot []byte        // 记录快照

	currentTerm int
	votedFor    int
	log         []LogEntry

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int
}

func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	var term int = rf.currentTerm
	var isleader bool = (rf.state == LEADER)
	return term, isleader
}

func (rf *Raft) encodeState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.log)
	e.Encode(rf.votedFor)
	e.Encode(rf.currentTerm)
	return w.Bytes()
}

func (rf *Raft) persist() {
	rf.persister.Save(rf.encodeState(), rf.persister.ReadSnapshot())
}

func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var log []LogEntry
	var votedFor int
	var currentTerm int

	if d.Decode(&log) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&currentTerm) != nil {
		return
	}

	rf.log = log
	rf.votedFor = votedFor
	rf.currentTerm = currentTerm
}

func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	firstIndex := rf.log[0].Index

	if index <= firstIndex || index > rf.lastLog().Index {
		return
	}

	var tmp []LogEntry
	rf.log = append(tmp, rf.log[index-firstIndex:]...)
	rf.snapshot = make([]byte, len(snapshot))
	copy(rf.snapshot, snapshot)
	rf.log[0].Command = nil

	rf.persister.Save(rf.encodeState(), snapshot)
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Snapshot          []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		reply.Term = args.Term
	}

	rf.setElectionTime()
	rf.toState(FOLLOWER)

	if rf.commitIndex >= args.LastIncludedIndex {
		return
	}

	if rf.lastLog().Index <= args.LastIncludedIndex {
		rf.log = make([]LogEntry, 1)
	} else {
		rf.log = append([]LogEntry{}, rf.log[args.LastIncludedIndex-rf.log[0].Index:]...)
	}

	rf.log[0] = LogEntry{
		Term:    args.LastIncludedTerm,
		Index:   args.LastIncludedIndex,
		Command: nil,
	}
	rf.snapshot = make([]byte, len(args.Snapshot))
	copy(rf.snapshot, args.Snapshot)
	rf.persister.Save(rf.encodeState(), args.Snapshot)
	rf.lastApplied, rf.commitIndex = 0, args.LastIncludedIndex
}

type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	if args.Term < rf.currentTerm {

		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}

	if args.Term > rf.currentTerm {
		reply.Term = args.Term
		rf.state = FOLLOWER
		rf.votedFor = -1
	}

	// assert args.Term >= rf.currentTerm

	if rf.votedFor != -1 && rf.votedFor != args.CandidateId {
		reply.VoteGranted = false
		return
	}

	term, index := rf.lastLog().Term, rf.lastLog().Index

	if args.LastLogTerm > term ||
		(args.LastLogTerm == term && args.LastLogIndex >= index) {

		// vote for args.CandidateId
		rf.setElectionTime()
		rf.toState(FOLLOWER)
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
	}

	rf.currentTerm = args.Term
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool

	XTerm  int
	XIndex int
	XLen   int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	reply.Success = false
	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		reply.Term = args.Term
		rf.votedFor = -1
	}

	// assert args.Term >= rf.currentTerm
	rf.setElectionTime()
	rf.toState(FOLLOWER)

	// 2D
	if args.PrevLogIndex < rf.log[0].Index {
		reply.XTerm = -1
		reply.XIndex = -1
		return
	}
	// end 2D

	if args.PrevLogIndex > rf.lastLog().Index {
		reply.XIndex = rf.lastLog().Index + 1
		reply.XTerm = -1
		return
	}

	if rf.Log(args.PrevLogIndex).Term != args.PrevLogTerm {

		reply.XTerm = rf.Log(args.PrevLogIndex).Term

		prevIndex := args.PrevLogIndex - 1
		for prevIndex >= rf.log[0].Index && rf.Log(prevIndex).Term == reply.XTerm {
			prevIndex -= 1
		}

		reply.XIndex = prevIndex + 1

		return
	}

	for _, entry := range args.Entries {
		if entry.Index >= len(rf.log)+rf.log[0].Index {
			rf.log = append(rf.log, entry)
		} else if entry.Term != rf.Log(entry.Index).Term {
			rf.log = rf.log[:entry.Index-rf.log[0].Index]
			rf.log = append(rf.log, entry)
		}
	}

	rf.commitIndex = max(rf.commitIndex, min(args.LeaderCommit, rf.lastLog().Index))
	reply.Success = true
}

func (rf *Raft) Start(command interface{}) (int, int, bool) {

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != LEADER {
		return -1, -1, false
	}

	newEntry := LogEntry{
		Command: command,
		Term:    rf.currentTerm,
		Index:   rf.lastLog().Index + 1,
	}
	rf.log = append(rf.log, newEntry)
	rf.persist()

	index := newEntry.Index
	term := newEntry.Term
	isLeader := rf.state == LEADER

	rf.BroadCast()
	return index, term, isLeader
}

func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) toState(state int) {

	rf.state = state
	switch state {
	case LEADER:
		rf.nextIndex = make([]int, len(rf.peers))
		rf.matchIndex = make([]int, len(rf.peers))

		for i := range rf.nextIndex {
			rf.nextIndex[i] = rf.lastLog().Index + 1
		}

	case CANDIDATE:
		rf.currentTerm += 1
		rf.votedFor = rf.me
	}
}

func (rf *Raft) BroadCast() {

	for i := range rf.peers {

		if i == rf.me {
			continue
		}
		go func(server int) {

			rf.mu.Lock()

			if rf.state != LEADER {
				rf.mu.Unlock()
				return
			}

			if rf.nextIndex[server] <= rf.log[0].Index {
				args := &InstallSnapshotArgs{
					Term:              rf.currentTerm,
					LeaderId:          rf.me,
					LastIncludedIndex: rf.log[0].Index,
					LastIncludedTerm:  rf.log[0].Term,
					Snapshot:          rf.persister.ReadSnapshot(),
				}
				rf.mu.Unlock()

				reply := &InstallSnapshotReply{}

				if !rf.peers[server].Call("Raft.InstallSnapshot", args, reply) {
					return
				}

				rf.mu.Lock()
				defer rf.mu.Unlock()

				if rf.state != LEADER || rf.currentTerm != args.Term {
					return
				}

				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.toState(FOLLOWER)
					rf.votedFor = -1
					rf.persist()
					return
				}

				rf.matchIndex[server] = max(rf.matchIndex[server], args.LastIncludedIndex)
				rf.nextIndex[server] = rf.matchIndex[server] + 1
				return
			}

			prevLogIndex := rf.nextIndex[server] - 1
			prevLogTerm := rf.Log(prevLogIndex).Term

			entries := []LogEntry{}
			for i := prevLogIndex + 1; i < len(rf.log)+rf.log[0].Index; i++ {
				entries = append(entries, *rf.Log(i))
			}

			args := AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				Entries:      entries,
				LeaderCommit: rf.commitIndex,
			}

			reply := AppendEntriesReply{}

			rf.mu.Unlock()

			if !rf.peers[server].Call("Raft.AppendEntries", &args, &reply) {
				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()

			if rf.state != LEADER || rf.currentTerm != args.Term {
				return
			}

			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.toState(FOLLOWER)
				rf.votedFor = -1
				rf.persist()
				return
			}

			if reply.Success {

				rf.matchIndex[server] = max(rf.matchIndex[server], args.PrevLogIndex+len(args.Entries))

				rf.nextIndex[server] = rf.matchIndex[server] + 1

				n := len(rf.matchIndex)
				tmp := make([]int, n)
				copy(tmp, rf.matchIndex)

				tmp[rf.me] = rf.lastLog().Index

				sort.Ints(tmp)
				newCommitIndex := tmp[n/2]

				if newCommitIndex > rf.commitIndex && newCommitIndex <= rf.lastLog().Index &&
					rf.Log(newCommitIndex).Term == rf.currentTerm {

					rf.commitIndex = newCommitIndex
				}

				return
			}

			rf.nextIndex[server] = max(rf.matchIndex[server]+1, reply.XIndex)

			boundary := max(reply.XIndex, rf.log[0].Index)
			for boundary <= rf.lastLog().Index && rf.Log(boundary).Term == reply.XTerm {
				boundary += 1
				rf.nextIndex[server] = boundary
			}
			// rf.nextIndex[server] = max(rf.matchIndex[server]+1, rf.nextIndex[server]-1)

		}(i)
	}
}

func (rf *Raft) StartElection() {
	total := len(rf.peers)

	rf.setElectionTime()

	rf.state = CANDIDATE
	rf.currentTerm += 1
	rf.votedFor = rf.me
	rf.persist()

	ticketCount := 1

	args := RequestVoteArgs{rf.currentTerm, rf.me, rf.lastLog().Index, rf.lastLog().Term}

	for i := range rf.peers {

		if i == rf.me {
			continue
		}

		go func(server int) {

			reply := RequestVoteReply{}

			if !rf.peers[server].Call("Raft.RequestVote", &args, &reply) {
				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()

			if reply.Term != rf.currentTerm || rf.state != CANDIDATE {
				return
			}

			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.toState(FOLLOWER)
				rf.votedFor = -1
				rf.persist()
				return
			}

			if !reply.VoteGranted {
				return
			}

			ticketCount += 1

			if ticketCount <= total/2 {
				return
			}

			rf.toState(LEADER)
			rf.BroadCast()

		}(i)
	}
}

func (rf *Raft) ElectClock() {

	for !rf.killed() {

		rf.mu.Lock()
		if rf.state != LEADER && time.Now().After(rf.timeout) {
			// new election
			rf.StartElection()
		}
		rf.mu.Unlock()

		time.Sleep(20 * time.Millisecond)
	}
}

func (rf *Raft) HeartClock() {
	for !rf.killed() {

		rf.mu.Lock()

		if rf.state == LEADER {

			rf.BroadCast()
		}
		rf.mu.Unlock()

		time.Sleep(100 * time.Millisecond)
	}
}

func (rf *Raft) setElectionTime() {

	ms := rand.Intn(200) + 200
	rf.timeout = time.Now().Add(time.Duration(ms) * time.Millisecond)
}

func (rf *Raft) applyMsg() {

	for !rf.killed() {
		time.Sleep(10 * time.Millisecond)

		rf.mu.Lock()

		if rf.lastApplied < rf.log[0].Index {
			tmp := ApplyMsg{

				SnapshotValid: true,
				Snapshot:      rf.snapshot,
				SnapshotTerm:  rf.log[0].Term,
				SnapshotIndex: rf.log[0].Index,
			}
			rf.mu.Unlock()

			rf.applyCh <- tmp

			rf.mu.Lock()
			rf.lastApplied = tmp.SnapshotIndex
		}

		if rf.lastApplied >= rf.commitIndex {
			rf.mu.Unlock()
			continue
		}

		commitIndex := rf.commitIndex
		applyEntries := make([]LogEntry, commitIndex-rf.lastApplied)

		firstIndex := rf.log[0].Index
		copy(applyEntries, rf.log[rf.lastApplied+1-firstIndex:commitIndex+1-firstIndex])

		rf.mu.Unlock()

		for _, entry := range applyEntries {
			rf.applyCh <- ApplyMsg{
				CommandValid: true,
				Command:      entry.Command,
				CommandIndex: entry.Index,
				CommandTerm:  entry.Term,
			}
		}

		rf.mu.Lock()
		rf.lastApplied = max(rf.lastApplied, commitIndex)
		rf.mu.Unlock()
	}
}

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash

	rf := &Raft{

		peers:     peers,
		persister: persister,
		me:        me,

		state:   FOLLOWER,
		applyCh: applyCh,

		currentTerm: 0,
		votedFor:    -1,
		log:         make([]LogEntry, 1),
	}

	// 初始化 timeout
	rf.setElectionTime()

	// 初始化之前持久化过的量
	rf.readPersist(persister.ReadRaftState())

	//
	rf.commitIndex = rf.log[0].Index
	rf.lastApplied = rf.log[0].Index

	go rf.HeartClock()
	go rf.ElectClock()
	go rf.applyMsg()

	return rf
}

func (rf *Raft) lastLog() LogEntry {
	return rf.log[len(rf.log)-1]
}

func (rf *Raft) Log(trueIndex int) *LogEntry {
	return &rf.log[trueIndex-rf.log[0].Index]
}

func min(a, b int) int {
	if a > b {
		return b
	}
	return a
}

func max(a, b int) int {
	if a < b {
		return b
	}
	return a
}
