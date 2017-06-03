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

import "sync"
import (
	"labrpc"
	"time"
	"math/rand"
	"bytes"
	"encoding/gob"
)

// import "bytes"
// import "encoding/gob"



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

type LogEntry struct{
	Term int
	Command interface{}
}

type State byte
const (
	Follower State = iota
	Candidate
	Leader
)

type commitMsg struct {
	lastCommitIndex int
	newCommitIndex  int
}
//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex
	orderMutex sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]

	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	leaderId    	int
	// Persistent state on all servers
	currentTerm 	int
	votedFor    	int
	logs	    	[]LogEntry
	// Volatile state on all servers
	commitIndex 	int
	lastApplied	int
	// Volatile state on leaders
	nextIndex	[]int
	matchIndex	[]int

	timer 	    	*time.Timer
	state       	State
	applyCh		chan ApplyMsg
	commitMsgs	[]commitMsg
	commitCh	chan commitMsg
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	// Your code here.
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
	// Your code here.
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
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
	// Your code here.
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
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
	// Your data here.
	Term 		int  		// candidate's term
	CandidateId 	int 		// candidate's requesting vote
	LastLogIndex 	int		// index of candidate's last log entry
	LastLogTerm	int		// term of candidate's last log entry
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	// Your data here.
	Term		int		// currentTerm,for candidate to update itself
	VoteGranted	bool		// true means candidate received vote
}

type AppendEntriesArgs struct{
	Term		int		// leader's term
	LeaderId	int		// so follower's can redirect clients
	PrevLogIndex	int		// index of log entry immediately preceding new ones
	PrevLogTerm	int 		// term of prevLogIndex entry

	Entries         []LogEntry	// log entries to
	LeaderCommit	int		// leader's commitIndex
}

type AppendEntriesReply struct {
	Term 		int		// currentTerm,for leader to update itself
	Success		bool		// true if follower contained entry
}

func entryComparision(index1,term1,index2,term2 int)bool{
	if term1 > term2{
		return true
	}else if term1 < term2{
		return false
	}else{
		return index1 >= index2
	}
}


//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here.
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//DPrintf("Server (%d,%d) receive RequestVote from server (%d,%d),VoteFor = %d",
	//	rf.me,rf.currentTerm,args.CandidateId,args.Term,rf.votedFor)
	reply.VoteGranted = false

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.persist()

		//if rf.state == Leader {
		//	DPrintf("Leader %d become Follower",rf.leaderId)
		//}
		rf.state = Follower
	}

	if rf.votedFor == -1 || args.CandidateId == rf.votedFor {
		if entryComparision(args.LastLogIndex,args.LastLogTerm,len(rf.logs) - 1,rf.logs[len(rf.logs) - 1].Term) {
			reply.VoteGranted = true
			rf.state = Follower
			rf.leaderId = args.CandidateId
			rf.votedFor = args.CandidateId
			rf.persist()
			// DPrintf("VoteGranted")
			rf.resetElectionTimer(rf.state)
			return
		}
	}

	//DPrintf("Not granted,because (%d,%d,%d,%d) = %v",args.LastLogIndex,args.LastLogTerm,len(rf.logs) - 1,rf.currentTerm,
	//	entryComparision(args.LastLogIndex,args.LastLogTerm,len(rf.logs) - 1,rf.currentTerm))

	reply.Term = rf.currentTerm
}

func (rf *Raft)AppendEntries(args AppendEntriesArgs,reply *AppendEntriesReply){
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// DPrintf("Server (%d,%d) received AppendEntries RPC from leader (%d,%d)",
	//	rf.me,rf.currentTerm,args.LeaderId,args.Term)



	DPrintf("server %d recive commitIndex %d",rf.me,args.LeaderCommit)

	if args.Term < rf.currentTerm ||   // #1
		(args.PrevLogIndex > len(rf.logs) - 1 || rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm) { // #2
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	rf.leaderId = args.LeaderId
	rf.currentTerm = args.Term
	rf.votedFor = -1
	rf.persist()
	rf.state = Follower

	for i := 0;i < len(args.Entries);i++ {
		if i + args.PrevLogIndex + 1 > len(rf.logs) - 1 {
			rf.logs = append(rf.logs,args.Entries[i:]...)
			break
		} else if rf.logs[i + args.PrevLogIndex + 1].Term != args.Entries[i].Term {
			rf.logs = append(rf.logs[:i + args.PrevLogIndex + 1],args.Entries[i:]...)
			break
		}
	}

	if args.LeaderCommit > rf.commitIndex {
		lastCommitted := rf.commitIndex
		rf.commitIndex = args.LeaderCommit
		if args.LeaderCommit > len(rf.logs) - 1 {
			rf.commitIndex = len(rf.logs) - 1
		}
		commitIndex := rf.commitIndex
		//DPrintf("server %d set commitIndex %d",rf.me,rf.commitIndex)
		rf.orderMutex.Lock()
		rf.commitMsgs = append(rf.commitMsgs,commitMsg{lastCommitted,commitIndex})
		rf.orderMutex.Unlock()

		go func() {
			rf.orderMutex.Lock()
			defer rf.orderMutex.Unlock()

			minIdx,minCommited := 0,rf.commitMsgs[0].lastCommitIndex
			for i := 1;i < len(rf.commitMsgs);i++ {
				if rf.commitMsgs[i].lastCommitIndex < minCommited {
					minIdx = i
					minCommited = rf.commitMsgs[i].lastCommitIndex
				}
			}
			lastCommitted := rf.commitMsgs[minIdx].lastCommitIndex
			commitIndex := rf.commitMsgs[minIdx].newCommitIndex
			rf.commitMsgs = append(rf.commitMsgs[:minIdx],rf.commitMsgs[minIdx + 1:]...)

			for i := lastCommitted + 1;i <= commitIndex;i++ {
				rf.applyCh <- ApplyMsg{i,rf.logs[i].Command,false,nil}
			}
		}()
	}
	reply.Term = rf.currentTerm
	reply.Success = true

	rf.resetElectionTimer(rf.state)
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
	return ok
}

func (rf *Raft) sendAppendEntries(server int,args AppendEntriesArgs, reply *AppendEntriesReply) bool{
	ok := rf.peers[server].Call("Raft.AppendEntries",args,reply)
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
	index := -1
	term := -1

	rf.mu.Lock()
	defer rf.mu.Unlock()

	term,isLeader := rf.currentTerm,rf.state == Leader

	if !isLeader {
		return index,term,false
	}

	rf.logs = append(rf.logs,LogEntry{rf.currentTerm,command})
	rf.persist()

	//DPrintf("Leader (%d,%d) logs => %v,start command %v",rf.me,rf.currentTerm,rf.logs,command)
	index = len(rf.logs) - 1
	go rf.broadcastHeartbeat()

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

func (rf *Raft) randomTimeout(state State)time.Duration{
	var lower int
	switch state {
	case Follower:
		lower = 300
	case Leader:
		lower = 100
	}
	return time.Duration(lower + (rand.Int() % lower)) * time.Millisecond
}

func (rf *Raft) resetElectionTimer(state State) {
	if !rf.timer.Stop() {
		select {
		case <- rf.timer.C:
		default:
		}
	}

	timeout := rf.randomTimeout(state)
	//DPrintf("Server %d state:%v reset timeout %v",rf.me,rf.state,timeout)
	rf.timer.Reset(timeout)
}

func (rf *Raft) updateCommitIndex() {

	rf.mu.Lock()
	defer rf.mu.Unlock()

	var n int = len(rf.logs) - 1
	for n > rf.commitIndex {
		if rf.logs[n].Term == rf.currentTerm {
			count := 1
			for i := 0;i < len(rf.peers);i++ {
				if i == rf.me {
					continue
				}
				if rf.matchIndex[i] >= n {
					count++
				}
				if count > len(rf.peers) / 2 {
					//DPrintf("Leader set commitIndex %d",n)

					lastCommitted := rf.commitIndex
					rf.commitIndex = n

					rf.orderMutex.Lock()
					rf.commitMsgs = append(rf.commitMsgs,commitMsg{lastCommitted,rf.commitIndex})
					rf.orderMutex.Unlock()

					go func(){

						rf.orderMutex.Lock()
						defer rf.orderMutex.Unlock()

						minIdx,minCommitIdx := 0,rf.commitMsgs[0].lastCommitIndex
						for i := 1;i < len(rf.commitMsgs);i++ {
							if rf.commitMsgs[i].lastCommitIndex < minCommitIdx {
								minIdx = i
								minCommitIdx = rf.commitMsgs[i].lastCommitIndex
							}
						}
						lastCommitted := rf.commitMsgs[minIdx].lastCommitIndex
						commitIndex := rf.commitMsgs[minIdx].newCommitIndex

						rf.commitMsgs = append(rf.commitMsgs[:minIdx],rf.commitMsgs[minIdx + 1:]...)

						for i := lastCommitted + 1;i <= commitIndex;i++ {
							rf.applyCh <- ApplyMsg{
								Index:		i,
								Command:	rf.logs[i].Command,
							}
						}

					}()

					return
				}
			}
		}
		n--
	}
}

func (rf *Raft) joinElection() {
	const (
		Reject = -iota
		Timeout
		Agree
	)

	rf.mu.Lock()
	lastLogIndex := len(rf.logs) - 1
	lastLogTerm := rf.logs[lastLogIndex].Term
	rf.currentTerm++
	rf.persist()


	//DPrintf(fmt.Sprintf("Server (%d,%d) join in election",rf.me,rf.currentTerm))

	votes := make(chan int,len(rf.peers) - 1)

	duration := rf.randomTimeout(rf.state)

	for i := 0;i < len(rf.peers);i++ {
		if i == rf.me {
			continue
		}

		go func(server int){
			start := time.Now()
			for {
				var reply RequestVoteReply
				ok := rf.sendRequestVote(server,
					RequestVoteArgs {
						Term: rf.currentTerm,
						CandidateId:rf.me,
						LastLogIndex:lastLogIndex,
						LastLogTerm:lastLogTerm,
					},&reply)
				if ok {
					if reply.VoteGranted {
						votes <- Agree
					}else{
						if reply.Term > rf.currentTerm {
							votes <- Reject
						}else {
							votes <- reply.Term
						}
					}
					break
				} else {
					// RPC failed,retry until timeout
					if time.Now().After(start.Add(duration)) {
						votes <- Timeout
						break
					}
				}
			}

		}(i)
	}

	rf.mu.Unlock()

	voteCount := 1
	rejectCount := 0
	peerTerm := 0

	// three result: 1. win 2. lose 3. timeout
	for  {
		select{
		case vote := <-votes:
			switch vote {
			case Agree:
				voteCount += 1
			case Reject,Timeout:
				rejectCount += 1
			default:
				// current term < peer's term
				rejectCount += 1
				if vote > peerTerm {
					peerTerm = vote
				}
			}
		}

		if voteCount > len(rf.peers) / 2 {
			// 1. win
			rf.mu.Lock()
			rf.state = Leader
			rf.leaderId = rf.me
			go rf.broadcastHeartbeat()
			rf.resetElectionTimer(rf.state)
			rf.mu.Unlock()
			DPrintf("Server (%d,%d) becomes new leader",rf.me,rf.currentTerm)
			break
		} else if rejectCount > len(rf.peers) / 2 {
			// 2. lose
			rf.mu.Lock()
			rf.state = Follower
			rf.resetElectionTimer(rf.state)
			rf.mu.Unlock()
			// DPrintf("Server %d lose election",rf.me)
			break

		} else if voteCount + rejectCount == len(rf.peers) {
			// 3. timeout
			if peerTerm > rf.currentTerm {
				// discover new term
				rf.mu.Lock()
				rf.state = Follower
				rf.resetElectionTimer(rf.state)
				rf.mu.Unlock()
			}
			break
		}
	}
}


func (rf *Raft) heartbeat(server int,args *AppendEntriesArgs,reply *AppendEntriesReply) {
	ok := rf.sendAppendEntries(server,*args,reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state == Follower {
		return
	}
	if ok {
		if reply.Success {
			// 其他服务器匹配了从args.logs[args.PrevLogIndex]开始的len(args.Entries)条日志
			if reply.Term == rf.currentTerm {
				newNextIndex := args.PrevLogIndex + len(args.Entries) + 1
				newMatchIndex := newNextIndex - 1
				if newMatchIndex > rf.matchIndex[server] {
					rf.nextIndex[server] = newMatchIndex
					rf.matchIndex[server] = newMatchIndex
					go rf.updateCommitIndex()
				}
			}
		} else {
			if reply.Term > rf.currentTerm {		// 发现新term,转移到Follower状态
				// DPrintf("Find new term,leader (%d,%d) transfer to follower",rf.me,rf.currentTerm)
				rf.currentTerm = reply.Term
				rf.state = Follower
				rf.votedFor = -1
				rf.persist()
				rf.resetElectionTimer(Follower)
			} else {		// 其它Raft服务器日志与Leader日志不匹配　
				if rf.nextIndex[server] > 0 {
					rf.nextIndex[server]--
				}

				args := AppendEntriesArgs{
					Term:		rf.currentTerm,
					LeaderId:	rf.leaderId,
					PrevLogIndex:	rf.nextIndex[server],
					PrevLogTerm:	rf.logs[rf.nextIndex[server]].Term,
					Entries:	rf.logs[rf.nextIndex[server] + 1:],
					LeaderCommit:   rf.commitIndex,
				}
				var r AppendEntriesReply
				go rf.heartbeat(server,&args,&r)
			}
		}
	}
}

func (rf *Raft)broadcastHeartbeat() {

	rf.mu.Lock()
	defer rf.mu.Unlock()

	for i := 0;i < len(rf.peers);i++ {
		if i == rf.me {
			continue
		}
		args := AppendEntriesArgs{
			Term:		rf.currentTerm,
			LeaderId:	rf.leaderId,
			PrevLogIndex:	rf.nextIndex[i],
			PrevLogTerm:	rf.logs[rf.nextIndex[i]].Term,
			Entries:	rf.logs[rf.nextIndex[i] + 1:],
			LeaderCommit:   rf.commitIndex,
		}
		reply := AppendEntriesReply{}
		go rf.heartbeat(i,&args,&reply)
	}
	rf.resetElectionTimer(rf.state)
}

func (rf *Raft)backgroundTask() {
	for {
		select {
		case <-rf.timer.C:
			rf.mu.Lock()
			switch rf.state {
			case Leader:
				go rf.broadcastHeartbeat()
			case Follower:
				go rf.joinElection()
			}
			rf.mu.Unlock()
		}
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
	rf.logs = make([]LogEntry,1)
	rf.nextIndex = make([]int,len(rf.peers))
	rf.matchIndex = make([]int,len(rf.peers))
	rf.commitMsgs = make([]commitMsg,0,2)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// DPrintf("[Start %d]currentTerm:%d,votedFor:%d,logs:%v",rf.me,rf.currentTerm,rf.votedFor,rf.logs)

	rf.timer = time.NewTimer(rf.randomTimeout(rf.state))
	go rf.backgroundTask()

	return rf
}
