package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"labrpc"
	"time"
	"math/rand"
	"bytes"
	"sync"
	"encoding/gob"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

type LogEntry struct {
	Term    int
	Command interface{}
}

type State byte

const (
	Follower  State = iota
	Candidate
	Leader
)

type EventType int

const (
	TIMEOUT        EventType = iota
	BECOMEFOLLOWER
	BECOMELEADER
)

const HeartbeatInterval = 100 * time.Millisecond

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu sync.Mutex
	//orderMutex sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]

	leaderId int
	// Persistent state on all servers
	currentTerm int
	votedFor    int
	logs        []LogEntry
	// Volatile state on all servers
	commitIndex int
	lastApplied int
	// Volatile state on leaders
	nextIndex  []int
	matchIndex []int

	timer   *time.Timer
	state   State
	applyCh chan ApplyMsg

	eventQueue    chan EventType
	chanNewCommit chan bool
	//chanToFollower chan bool
	//chanToLeader   chan bool

	waitVoteTerm int
	votes        []bool
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	term := rf.currentTerm
	isLeader := rf.state == Leader
	rf.mu.Unlock()
	return term, isLeader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.logs)
}

//
// example RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	Term         int // candidate's term
	CandidateId  int // candidate's requesting vote
	LastLogIndex int // index of candidate's last log entry
	LastLogTerm  int // term of candidate's last log entry
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	Term        int  // currentTerm,for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

type AppendEntriesArgs struct {
	Term         int // leader's term
	LeaderId     int // so follower's can redirect clients
	PrevLogIndex int // index of log entry immediately preceding new ones
	PrevLogTerm  int // term of prevLogIndex entry

	Entries      []LogEntry // log entries to
	LeaderCommit int        // leader's commitIndex
}

type AppendEntriesReply struct {
	Term    int  // currentTerm,for leader to update itself
	Success bool // true if follower contained entry
}

func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {

	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	toFollower := false
	reply.VoteGranted = false
	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		return
	} else if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.state = Follower
		toFollower = true
	}

	reply.Term = rf.currentTerm

	if rf.votedFor == -1 || args.CandidateId == rf.votedFor {
		lastLogIndex := len(rf.logs) - 1
		lastLogTerm := rf.logs[lastLogIndex].Term

		newLog := false
		if args.LastLogTerm > lastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex) {
			newLog = true
		}

		if newLog {
			reply.VoteGranted = true
			rf.state = Follower
			toFollower = true
			rf.leaderId = args.CandidateId
			rf.votedFor = args.CandidateId

			rf.resetTimer(rf.randomTimeout(Follower))
		}
	}

	if toFollower {
		rf.eventQueue <- BECOMEFOLLOWER
	}

	if reply.VoteGranted {
		DPrintf("[授权成功] %d 授权给了 %d,my term = %d,args term = %d", rf.me, args.CandidateId, rf.currentTerm, args.Term)
	} else {
		DPrintf("[授权失败] %d 没有授权给了 %d,my term = %d,args term = %d", rf.me, args.CandidateId, rf.currentTerm, args.Term)
	}
}

func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {

	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	} else if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}

	rf.leaderId = args.LeaderId
	rf.state = Follower

	if args.PrevLogIndex > len(rf.logs)-1 || rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm {

		reply.Term = rf.currentTerm
		reply.Success = false
		rf.resetTimer(rf.randomTimeout(Follower))
		return
	}

	DPrintf("Server %d 收到Apppend消息,state = %d", rf.me, rf.state)

	rf.logs = append(rf.logs[:args.PrevLogIndex+1], args.Entries...)

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = args.LeaderCommit
		if rf.commitIndex > len(rf.logs)-1 {
			rf.commitIndex = len(rf.logs) - 1
		}

		go func() {
			rf.chanNewCommit <- true
		}()
	}

	reply.Term = rf.currentTerm
	reply.Success = true

	DPrintf("[AppendEntries]Server %d,term = %d 认同了leader %d,Term = %d", rf.me, rf.currentTerm, args.LeaderId, args.Term)

	rf.resetTimer(rf.randomTimeout(Follower))
	rf.eventQueue <- BECOMEFOLLOWER
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != Candidate {
		return false
	}

	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.state = Follower
		rf.votedFor = -1
		rf.persist()
		rf.eventQueue <- BECOMEFOLLOWER
		return false
	} else if reply.Term < rf.currentTerm {
		return false
	}

	if rf.waitVoteTerm != reply.Term {
		return false
	}
	if ok && reply.VoteGranted {

		DPrintf("[Granted] Server %d get vote from server %d", rf.me, server)
		rf.votes[server] = true
		count := 0
		for i := 0; i < len(rf.votes); i++ {
			if rf.votes[i] {
				count++
			}
		}
		if count > len(rf.peers)/2 {
			DPrintf("[选举成功] Server %d 得到半数投票", rf.me)
			if rf.state == Candidate {
				rf.eventQueue <- BECOMELEADER
				rf.leaderId = rf.me
				for i := 0; i < len(rf.peers); i++ {
					rf.nextIndex[i] = len(rf.logs)
					rf.matchIndex[i] = 0
				}

			}
		}
	}
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != Leader {
		return false
	}

	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.state = Follower
		rf.votedFor = -1
		rf.eventQueue <- BECOMEFOLLOWER
		return false
	} else if reply.Term < rf.currentTerm {
		return false
	}

	if reply.Success {
		newNextIndex := args.PrevLogIndex + len(args.Entries) + 1
		newMatchIndex := newNextIndex - 1

		if newMatchIndex > rf.matchIndex[server] {
			rf.nextIndex[server] = newNextIndex
			rf.matchIndex[server] = newMatchIndex
			go rf.updateCommitIndex()
		}
	} else {

		rf.nextIndex[server]--

		entries := make([]LogEntry, 0)

		prevLogIndex := len(rf.logs) - 1
		prevLogTerm := rf.logs[prevLogIndex].Term

		if rf.nextIndex[server]-1 < len(rf.logs) {
			prevLogIndex = rf.nextIndex[server] - 1
			DPrintf("[--prevLogIndex:%d,len(rf.logs):%d",prevLogIndex,len(rf.logs))
			prevLogTerm = rf.logs[prevLogIndex].Term
			entries = rf.logs[prevLogIndex+1:]
		}

		DPrintf("[Retry %d]prevLogIndex = %d,current leader %d", rf.me, prevLogIndex, rf.leaderId)
		args := AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.leaderId,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
			Entries:      entries,
			LeaderCommit: rf.commitIndex,
		}
		var r AppendEntriesReply
		go rf.sendAppendEntries(server, args, &r)
	}
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {

	rf.mu.Lock()
	defer rf.mu.Unlock()

	index := -1
	term := rf.currentTerm
	isLeader := rf.state == Leader

	if isLeader {
		rf.logs = append(rf.logs, LogEntry{rf.currentTerm, command})
		index = len(rf.logs) - 1
		rf.persist()
	}

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

func (rf *Raft) randomTimeout(state State) time.Duration {
	var lower int
	switch state {
	case Follower:
		lower = 300
	case Leader:
		lower = 100
	}
	timeout := time.Duration(lower+(rand.Int()%lower)) * time.Millisecond
	return timeout
}

func (rf *Raft) resetTimer(duration time.Duration) {
	if !rf.timer.Stop() {
		select {
		case <-rf.timer.C:
		default:
		}
	}

	rf.timer.Reset(duration)
}

func (rf *Raft) commit() {

	for {
		<-rf.chanNewCommit

		rf.mu.Lock()
		commitIndex := rf.commitIndex
		lastApplied := rf.lastApplied
		logs := rf.logs[lastApplied+1:commitIndex+1]
		rf.mu.Unlock()

		for i := lastApplied + 1; i <= commitIndex; i++ {
			DPrintf("[DEBUG]server[%d] apply %d-th command %v,term = %d,commitIndex = %d\n", rf.me, i, logs[i-lastApplied-1].Command, logs[i-lastApplied-1].Term, rf.commitIndex)
			rf.applyCh <- ApplyMsg{i, logs[i-lastApplied-1].Command, false, nil}
		}

		rf.mu.Lock()
		rf.lastApplied = commitIndex
		rf.mu.Unlock()
	}
}

func (rf *Raft) updateCommitIndex() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != Leader {
		return
	}
	var n int = len(rf.logs) - 1

	for n > rf.commitIndex {
		// 只能直接提交当前任期内的日志
		if rf.logs[n].Term == rf.currentTerm {
			count := 1

			for i := 0; i < len(rf.peers); i++ {
				if i == rf.me {
					continue
				}
				if rf.matchIndex[i] >= n {
					count++
				}
				if count > len(rf.peers)/2 {
					// 发现了新的commit index
					rf.commitIndex = n
					go func() {
						rf.chanNewCommit <- true
					}()
					return
				}
			}
		}
		n--
	}
}

func (rf *Raft) joinElection() {
	/* NOTE: caller has already hold rf.mu.Lock */
	defer rf.persist()

	if rf.state != Follower {
		return
	}

	rf.state = Candidate
	lastLogIndex := len(rf.logs) - 1
	lastLogTerm := rf.logs[lastLogIndex].Term
	rf.currentTerm++
	rf.votedFor = rf.me
	candidateId := rf.me
	term := rf.currentTerm
	rf.waitVoteTerm = rf.currentTerm
	for i := 0; i < len(rf.votes); i++ {
		rf.votes[i] = false
	}
	rf.votes[rf.me] = true

	DPrintf("[joinElection] Server %d join election,term = %d", rf.me, rf.currentTerm)

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}

		var reply RequestVoteReply
		go rf.sendRequestVote(i,
			RequestVoteArgs{
				Term:         term,
				CandidateId:  candidateId,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			}, &reply)
	}
}

func (rf *Raft) broadcastHeartbeat() {
	/* NOTE: caller has already hold rf.mu.Lock */

	for i := 0; i < len(rf.peers); i++ {

		if i == rf.me {
			continue
		}

		prevLogIndex := rf.nextIndex[i] - 1
		prevLogTerm := rf.logs[prevLogIndex].Term
		entries := rf.logs[prevLogIndex+1:]

		args := AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.leaderId,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
			Entries:      entries,
			LeaderCommit: rf.commitIndex,
		}
		reply := AppendEntriesReply{}
		go rf.sendAppendEntries(i, args, &reply)
	}
}

func (rf *Raft) bgTask() {

	go func() {
		for {
			select {
			case <-rf.timer.C:
				rf.eventQueue <- TIMEOUT
			}
		}
	}()

	for event := range rf.eventQueue {
		rf.mu.Lock()
		switch rf.state {
		case Leader:
			switch event {
			case TIMEOUT:
				rf.broadcastHeartbeat()
				rf.resetTimer(HeartbeatInterval)
			case BECOMEFOLLOWER:
				rf.state = Follower
			}
		case Follower:
			switch event {
			case TIMEOUT:
				DPrintf("[DEBUG]Server %d发现超时,开始加入选举", rf.me)
				rf.state = Follower
				rf.joinElection()
				rf.resetTimer(rf.randomTimeout(Follower))
			case BECOMEFOLLOWER:
			}
		case Candidate:
			switch event {
			case TIMEOUT:
				DPrintf("[DEBUG]Server %d发现超时,开始加入选举", rf.me)
				rf.state = Follower
				rf.joinElection()
				rf.resetTimer(rf.randomTimeout(Follower))
			case BECOMEFOLLOWER:
				rf.state = Follower
			case BECOMELEADER:
				rf.state = Leader
				DPrintf("[DEBUG] New leader: %d", rf.me)
				rf.broadcastHeartbeat()
				rf.resetTimer(HeartbeatInterval)
			}

		}
		rf.mu.Unlock()
	}
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.leaderId = -1
	// Your initialization code here.
	rf.applyCh = applyCh
	rf.state = Follower
	rf.logs = make([]LogEntry, 1)
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	//rf.chanToFollower = make(chan bool)
	//rf.chanToLeader = make(chan bool)
	rf.chanNewCommit = make(chan bool)
	rf.eventQueue = make(chan EventType, 1024)
	rf.votes = make([]bool, len(rf.peers))
	rf.waitVoteTerm = -1
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	rf.timer = time.NewTimer(rf.randomTimeout(rf.state))

	go rf.bgTask()
	go rf.commit()

	return rf
}
