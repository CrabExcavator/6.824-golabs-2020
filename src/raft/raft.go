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
	"bytes"
	"log"
	"math/rand"
	"sync"
	"time"
)
import "sync/atomic"
import "../labrpc"
import "../labgob"

// import "bytes"
// import "../labgob"



//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	IsSnapShot   bool
	SnapShot     []byte
	Command      interface{}
	CommandIndex int
}

type LogEntry struct {
	Index   int
	Term    int
	Command interface{}
}

func newLogEntry(log LogEntry) *LogEntry {
	return &LogEntry{
		Index:   log.Index,
		Term:    log.Term,
		Command: log.Command,
	}
}

//
// A Go object implementing a single Raft peer.
//

type Role int32

const (
	Leader    Role = 0
	Candidate Role = 1
	Follower  Role = 2
)

func (r Role) String() string {
	switch r {
	case Leader:
		return "Leader"
	case Candidate:
		return "Candidate"
	case Follower:
		return "Follower"
	default:
		return "UNKNOWN"
	}
}

type SnapShotState struct {
	lastIncludedIndex int
	lastIncludedTerm  int
	data              []byte
}

type State struct {
	role        Role
	currentTerm int
	votedFor    int
	log         []*LogEntry
	commitIndex int
	lastApplied int
	nextIndex   []int
	matchIndex  []int

	// SnapShot
	snapshotState SnapShotState
}

type RandomGen struct {
	r *rand.Rand

	lowerBound time.Duration
	upperBound time.Duration
}

func NewRandomGen(_lowerBound time.Duration, _upperBound time.Duration, seed int) *RandomGen {
	ret := &RandomGen{
		r:          rand.New(rand.NewSource(time.Now().Unix() + int64(seed))),
		lowerBound: _lowerBound,
		upperBound: _upperBound,
	}
	return ret
}

func (r *RandomGen) random() time.Duration {
	return time.Duration(r.r.Int63n((r.upperBound - r.lowerBound).Nanoseconds())) + r.lowerBound
}

const (
	debug = false
)

type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	state           *State
	timeOut         *time.Timer
	heartBeatTicker *time.Ticker
	applyTicker     *time.Ticker
	randGen         *RandomGen
	applyCh         chan ApplyMsg
}

func (rf *Raft) lock(s string) {
	if debug {
		log.Printf("[%d]: Lock, %s\n", rf.me, s)
	}
	rf.mu.Lock()
}

func (rf *Raft) unlock(s string) {
	if debug {
		log.Printf("[%d]: UnLock, %s\n", rf.me, s)
	}
	rf.mu.Unlock()
}

func (rf *Raft) getLastLogIndex() int {
	return rf.state.log[len(rf.state.log)-1].Index
}

func (rf *Raft) getLastLogTerm() int {
	return rf.state.log[len(rf.state.log)-1].Term
}

func (rf *Raft) truncLog(index int) {
	rf.state.log = rf.state.log[index:]
	rf.state.log[0] = &LogEntry{
		Index:   rf.state.snapshotState.lastIncludedIndex,
		Term:    rf.state.snapshotState.lastIncludedTerm,
		Command: nil,
	}
}

func (rf *Raft) clearLog() {
	rf.state.log = rf.state.log[0:1]
	rf.state.log[0] = &LogEntry{
		Index:   rf.state.snapshotState.lastIncludedIndex,
		Term:    rf.state.snapshotState.lastIncludedTerm,
		Command: nil,
	}
}

func (rf *Raft) reviseIndex(index int) int {
	return index - rf.state.snapshotState.lastIncludedIndex
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.state.currentTerm
	isleader = rf.state.role == Leader

	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.state.currentTerm)
	e.Encode(rf.state.votedFor)
	e.Encode(rf.state.log)
	e.Encode(rf.state.snapshotState.lastIncludedIndex)
	e.Encode(rf.state.snapshotState.lastIncludedTerm)
	e.Encode(rf.state.snapshotState.data)
	data := w.Bytes()
	if rf.state.snapshotState.data == nil || len(rf.state.snapshotState.data) == 0 {
		rf.persister.SaveRaftState(data)
	} else {
		rf.persister.SaveStateAndSnapshot(data, rf.state.snapshotState.data)
		rf.state.snapshotState.data = rf.state.snapshotState.data[0:0]
	}
}


//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var logEntry []*LogEntry
	var lastIncludedIndex int
	var lastIncludedTerm int
	if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil ||
		d.Decode(&logEntry) != nil || d.Decode(&lastIncludedIndex) != nil ||
		d.Decode(&lastIncludedTerm) != nil || d.Decode(&rf.state.snapshotState.data) != nil {
		log.Println("decode error")
	} else {
		rf.state.currentTerm = currentTerm
		rf.state.votedFor = votedFor
		rf.state.log = logEntry
		rf.state.snapshotState.lastIncludedIndex = lastIncludedIndex
		rf.state.snapshotState.lastIncludedTerm = lastIncludedTerm
	}
}



//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.lock("RequestVote")
	defer rf.unlock("RequestVote")
	//log.Printf("[%d, %d]: request vote from [%d, %d] which has lastLog<%d, %d> whereas mine is <%d, %d>\n", rf.me, rf.state.currentTerm, args.CandidateID, args.Term, args.LastLogTerm, args.LastLogIndex, rf.getLastLogTerm(), rf.getLastLogIndex())
	flag := rf.recvHeartBeat(args.Term, args.CandidateID)
	lastLogIndex := rf.getLastLogIndex()
	lastLogTerm := rf.getLastLogTerm()

	cmp := func(term1 int, index1 int, term2 int, index2 int) bool {
		return (term1 < term2) || ((term1 == term2) && (index1 <= index2))
	}

	reply.Term = rf.state.currentTerm
	if flag && rf.state.votedFor == -1 && cmp(lastLogTerm, lastLogIndex, args.LastLogTerm, args.LastLogIndex) {
		rf.state.votedFor = args.CandidateID
		rf.persist()
		reply.VoteGranted = true
	} else {
		reply.VoteGranted = false
	}
}

type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term        int
	LastApplied int
	Success     bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.lock("AppendEntries")
	defer rf.unlock("AppendEntries")
	//log.Printf("[%d, %d]: append entries from %d whose prevLog <%d, %d> whereas my lastLog <%d, %d>\n", rf.me, rf.state.currentTerm, args.LeaderID, args.PrevLogTerm, args.PrevLogIndex, rf.getLastLogTerm(), rf.getLastLogIndex())
	flag := rf.recvHeartBeat(args.Term, args.LeaderID)
	lastLogIndex := rf.getLastLogIndex()

	cmp := func(term1 int, index1 int, term2 int, index2 int) bool {
		return (term1 == term2) && (index1 == index2)
	}

	reply.Term = rf.state.currentTerm
	reply.Success = false
	reply.LastApplied = rf.state.lastApplied
	if flag && args.PrevLogIndex >= 0 && args.PrevLogIndex <= lastLogIndex {
		reviseIndex := rf.reviseIndex(args.PrevLogIndex)
		if reviseIndex < 0 {
			return
		}

		prevLog := rf.state.log[rf.reviseIndex(args.PrevLogIndex)]
		if cmp(prevLog.Term, prevLog.Index, args.PrevLogTerm, args.PrevLogIndex) {
			rf.state.log = rf.state.log[0:rf.reviseIndex(prevLog.Index+1)]
			for _, entry := range args.Entries {
				rf.state.log = append(rf.state.log, newLogEntry(entry))
			}
			rf.persist()
			reply.Success = true
		}
	}
	min := func(a int, b int) int {
		if a > b {
			return b
		}
		return a
	}
	if reply.Success == true && args.LeaderCommit > rf.state.commitIndex {
		rf.state.commitIndex = min(args.LeaderCommit, rf.getLastLogIndex())
	}
}

func (rf *Raft) recvHeartBeat(term int, peer int) bool {
	rf.timeOut.Reset(rf.randGen.random())
	if rf.state.currentTerm < term {
		rf.state.role = Follower
		rf.state.currentTerm = term
		rf.state.votedFor = -1
		rf.persist()
	}
	return rf.state.currentTerm <= term
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderID          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Offset            int
	Data              []byte
	Done              bool
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.lock("InstallSnapshot")
	defer rf.unlock("InstallSnapshot")

	flag := rf.recvHeartBeat(args.Term, args.LeaderID)

	if flag {
		if rf.state.snapshotState.lastIncludedIndex <= args.LastIncludedIndex {
			rf.state.snapshotState.data = args.Data
			rf.state.snapshotState.lastIncludedIndex = args.LastIncludedIndex
			rf.state.snapshotState.lastIncludedTerm = args.LastIncludedTerm

			lastIndex := rf.getLastLogIndex()
			if lastIndex > args.LastIncludedIndex {
				reviseIndex := rf.reviseIndex(args.LastIncludedIndex)
				entry := rf.state.log[reviseIndex]
				if entry.Index == args.LastIncludedIndex && entry.Term == args.LastIncludedTerm {
					rf.truncLog(reviseIndex)
				} else {
					rf.clearLog()
				}
			} else {
				rf.clearLog()
			}
			rf.persist()

			rf.applyCh <- ApplyMsg{
				CommandValid: true,
				IsSnapShot:   true,
				SnapShot:     args.Data,
				Command:      nil,
				CommandIndex: args.LastIncludedIndex,
			}
		}
	}

	reply.Term = rf.state.currentTerm
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
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

func (rf *Raft) checkCurrentTerm(term int) {
	if rf.state.currentTerm < term {
		rf.state.role = Follower
		rf.state.currentTerm = term
		rf.persist()
	}
}

func (rf *Raft) doElectron() {
	rf.lock("doElectron")
	rf.state.role = Candidate
	rf.state.currentTerm++
	term := rf.state.currentTerm
	rf.state.votedFor = rf.me
	rf.persist()
	lastLogIndex := rf.getLastLogIndex()
	lastLogTerm := rf.getLastLogTerm()
	rf.unlock("doElectron")

	//log.Printf("[%d, %d]: timeout, start electron\n", rf.me, rf.state.currentTerm)
	vote := 1
	for peer, _ := range rf.peers {
		if peer == rf.me {
			continue
		}
		go func(peer int) {
			if rf.state.role != Candidate {
				return
			}
			args := RequestVoteArgs{
				Term:         term,
				CandidateID:  rf.me,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			}
			reply := RequestVoteReply{}
			//log.Printf("[%d, %d]: send request vote to %d", rf.me, rf.state.currentTerm, peer)
			flag := rf.sendRequestVote(peer, &args, &reply)
			//log.Printf("[%d, %d]: vote value %v", rf.me, rf.state.currentTerm, reply.VoteGranted)
			if flag {
				rf.lock("doElectron")
				defer rf.unlock("doElectron")
				rf.checkCurrentTerm(reply.Term)
				if rf.state.role != Candidate {
					return
				}
				if reply.VoteGranted {
					vote++
					if rf.isMajority(vote) {
						//log.Printf("[%d, %d]: become leader, lastLogIndex: %d, lastLogTerm: %d\n", rf.me, rf.state.currentTerm, rf.getLastLogIndex(), rf.getLastLogTerm())
						rf.state.role = Leader
						for i, _ := range rf.state.nextIndex {
							rf.state.nextIndex[i] = rf.getLastLogIndex() + 1
						}
						for i, _ := range rf.state.matchIndex {
							rf.state.matchIndex[i] = 0
						}
					}
				}
			}
		}(peer)
	}
	rf.timeOut.Reset(rf.randGen.random())
}

func (rf *Raft) isMajority(n int) bool {
	return n >= (1+len(rf.peers))/2
}

func (rf *Raft) moveCommit(index int) {
	flag := true
	cnt := 1
	for peer, x := range rf.state.matchIndex {
		if peer == rf.me {
			continue
		}
		if x >= index {
			cnt++
		}
	}
	if !rf.isMajority(cnt) {
		flag = false
	}
	/*reviseIndex := rf.reviseIndex(index)
	if rf.state.log[reviseIndex].Term != rf.state.currentTerm {
		flag = false
	}*/
	if rf.state.commitIndex > index {
		flag = false
	}
	if flag {
		rf.state.commitIndex = index
	}
}

func (rf *Raft) doHeartBeat() {
	//log.Printf("[%d, %d]: heartbeat role %v \n", rf.me, rf.state.currentTerm, rf.state.role.String())
	rf.lock("doHeartBeat")
	term := rf.state.currentTerm
	role := rf.state.role
	lastLogIndex := rf.getLastLogIndex()
	lastLogTerm := rf.getLastLogTerm()
	leaderCommit := rf.state.commitIndex
	rf.unlock("doHeartBeat")

	if role == Leader {
		//log.Printf("[%d, %d]: sending heartbeat, commitIndex: %d, lastApply: %d, lastLog: %d \n", rf.me, rf.state.currentTerm, rf.state.commitIndex, rf.state.lastApplied, rf.getLastLogIndex())
		for peer, _ := range rf.peers {
			if peer == rf.me {
				continue
			}
			go func(peer int) {

				rf.lock("doHeartBeat")
				if rf.state.role != Leader {
					rf.unlock("doHeartBeat")
					return
				}
				args := AppendEntriesArgs{
					Term:         term,
					LeaderID:     rf.me,
					PrevLogIndex: lastLogIndex,
					PrevLogTerm:  lastLogTerm,
					Entries:      make([]LogEntry, 0),
					LeaderCommit: leaderCommit,
				}
				isHeartBeat := true
				if rf.state.nextIndex[peer] <= lastLogIndex {
					//log.Println(rf.me, peer, rf.state.nextIndex[peer], rf.state.commitIndex ,lastLogIndex)

					// send snapshot
					if rf.state.nextIndex[peer] <= rf.state.snapshotState.lastIncludedIndex {
						lastIncludedIndex := rf.state.snapshotState.lastIncludedIndex
						lastIncludedTerm := rf.state.snapshotState.lastIncludedTerm
						snapArgs := InstallSnapshotArgs{
							Term:              rf.state.currentTerm,
							LeaderID:          rf.me,
							LastIncludedIndex: lastIncludedIndex,
							LastIncludedTerm:  lastIncludedTerm,
							Offset:            0,
							Data:              rf.persister.ReadSnapshot(),
							Done:              true,
						}
						snapReply := InstallSnapshotReply{}
						rf.unlock("doHeartBeat")

						flag := rf.sendInstallSnapshot(peer, &snapArgs, &snapReply)

						if flag {
							rf.lock("doHeartBeat")
							defer rf.unlock("doHeartBeat")
							rf.checkCurrentTerm(snapReply.Term)
							if rf.state.role != Leader {
								return
							}
							rf.state.matchIndex[peer] = lastIncludedIndex
							rf.state.nextIndex[peer] = lastIncludedIndex + 1
							rf.moveCommit(rf.state.matchIndex[peer])
						}
						return
					}

					isHeartBeat = false
					args.PrevLogIndex = rf.state.log[rf.reviseIndex(rf.state.nextIndex[peer]-1)].Index
					args.PrevLogTerm = rf.state.log[rf.reviseIndex(rf.state.nextIndex[peer]-1)].Term
					for i := rf.state.nextIndex[peer]; i <= lastLogIndex; i++ {
						args.Entries = append(args.Entries, *rf.state.log[rf.reviseIndex(i)])
					}
				}
				rf.unlock("doHeartBeat")

				reply := AppendEntriesReply{}
				flag := rf.sendAppendEntries(peer, &args, &reply)
				if flag {
					rf.lock("doHeartBeat")
					defer rf.unlock("doHeartBeat")
					rf.checkCurrentTerm(reply.Term)
					if rf.state.role != Leader {
						return
					}
					if !isHeartBeat {
						if reply.Success {
							rf.state.matchIndex[peer] = lastLogIndex
							rf.state.nextIndex[peer] = lastLogIndex + 1
							rf.moveCommit(rf.state.matchIndex[peer])
						} else {
							max := func(a int, b int) int {
								if a > b {
									return a
								}
								return b
							}
							rf.state.nextIndex[peer] = max(reply.LastApplied, 1)
						}
					}
				}
			}(peer)
		}
		rf.timeOut.Reset(rf.randGen.random())
	}
}

func (rf *Raft) doApply() {
	rf.lock("doApply")
	defer rf.unlock("doApply")
	for (rf.state.lastApplied <= rf.state.commitIndex) && (rf.state.lastApplied <= rf.getLastLogIndex()) {
		revisedLastApply := rf.reviseIndex(rf.state.lastApplied)
		if revisedLastApply >= 0 && rf.state.log[revisedLastApply] != nil && rf.state.log[revisedLastApply].Command != nil {
			applyMsg := ApplyMsg{
				CommandValid: true,
				IsSnapShot:   false,
				Command:      rf.state.log[rf.reviseIndex(rf.state.lastApplied)].Command,
				CommandIndex: rf.state.log[rf.reviseIndex(rf.state.lastApplied)].Index,
			}
			//log.Printf("[%d, %d]: lastApply: %d, commitIndex: %d, role: %v, Command: %v \n", rf.me, rf.state.currentTerm, rf.state.lastApplied, rf.state.commitIndex, rf.state.role, applyMsg.Command)
			rf.applyCh <- applyMsg
		}
		rf.state.lastApplied++
	}
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	rf.lock("Start")
	defer rf.unlock("Start")
	isLeader = rf.state.role == Leader
	term = rf.state.currentTerm
	if isLeader {
		index = rf.getLastLogIndex() + 1
		rf.state.log = append(rf.state.log, &LogEntry{
			Index:   index,
			Term:    term,
			Command: command,
		})
		rf.persist()
	} else {
		applyMsg := ApplyMsg{
			CommandValid: false,
			IsSnapShot:   false,
			Command:      command,
		}
		rf.applyCh <- applyMsg
	}

	//log.Printf("[%d, %d]: index: %d, term: %d, isLeader: %v, command: %v\n", rf.me, rf.state.currentTerm, index, term, isLeader, command)
	return index, term, isLeader
}

func (rf *Raft) StartSnapShot(index int, data []byte) {
	rf.lock("StartSnapShot")
	defer rf.unlock("StartSnapShot")
	revisedIndex := rf.reviseIndex(index)
	//log.Println(rf.me, revisedIndex, index)
	rf.state.snapshotState.data = data
	rf.state.snapshotState.lastIncludedIndex = rf.state.log[revisedIndex].Index
	rf.state.snapshotState.lastIncludedTerm = rf.state.log[revisedIndex].Term
	rf.truncLog(revisedIndex)
	rf.persist()
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
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
	rf.applyCh = applyCh

	// Your initialization code here (2A, 2B, 2C).
	rf.state = &State{
		role:        Follower,
		currentTerm: 0,
		votedFor:    -1,
		log:         make([]*LogEntry, 0),
		commitIndex: 0,
		lastApplied: 0,
		nextIndex:   make([]int, len(peers)),
		matchIndex:  make([]int, len(peers)),
	}
	rf.state.log = append(rf.state.log, &LogEntry{
		Index:   0,
		Term:    0,
		Command: nil,
	})
	rf.state.snapshotState = SnapShotState{
		lastIncludedIndex: 0,
		lastIncludedTerm:  0,
		data:              nil,
	}
	rf.randGen = NewRandomGen(time.Millisecond*400, time.Millisecond*500, me)
	rf.timeOut = time.NewTimer(rf.randGen.random())
	rf.heartBeatTicker = time.NewTicker(time.Millisecond * 150)
	rf.applyTicker = time.NewTicker(time.Millisecond * 150)

	go func() {
		for {
			select {
			case <-rf.heartBeatTicker.C:
				{
					rf.doHeartBeat()
				}
			case <-rf.timeOut.C:
				{
					rf.doElectron()
				}
			case <-rf.applyTicker.C:
				{
					rf.doApply()
				}
			}
		}
	}()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
