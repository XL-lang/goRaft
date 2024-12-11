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
	"encoding/gob"
	//"fmt"
	"labrpc"
	"math/rand"
	"sync"
	"time"
)

// import "bytes"
// import "encoding/gob"

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

type LogEntry struct {
	Term    int
	Command interface{}
	Index   int
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
	Term       int
	Success    bool
	MatchIndex int
}

type RequestVoteArgs struct {
	// Your data here.
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
	MatchIndex  int
}

type Raft struct {
	// Lock for shared access
	mu sync.Mutex

	// RPC of all peers
	peers []*labrpc.ClientEnd
	// Objects to store states of this server
	persister *Persister

	me    int // index into peers[]
	alive bool

	// Channels
	applyChannel           chan ApplyMsg
	voteToOthersChannel    chan bool
	appendEntriesChannel   chan bool
	electionSuccessChannel chan bool

	// follower		0
	// leader		1
	// candidate	2
	state      int
	TotalVotes int

	// State
	// Persistent state on all servers
	CurrentTerm int
	VoteFor     int
	logs        []LogEntry
	// Volatile state on all servers
	commitIndex int
	lastApplied int
	// Volatile state on leaders, reinitialized after election
	nextIndex  []int
	matchIndex []int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	return rf.CurrentTerm, rf.state == 1
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
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
	e.Encode(rf.CurrentTerm)
	e.Encode(rf.VoteFor)
	e.Encode(rf.logs)
	rf.persister.SaveRaftState(w.Bytes())
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	// Your code here.
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
	if data != nil {
		r := bytes.NewBuffer(data)
		d := gob.NewDecoder(r)
		d.Decode(&rf.CurrentTerm)
		d.Decode(&rf.VoteFor)
		d.Decode(&rf.logs)
	}
}

func (rf *Raft) isSenderLogUpToDate(args RequestVoteArgs) bool {
	// Your code here.
	lastEntry := rf.logs[len(rf.logs)-1]
	uptodate := false
	if lastEntry.Term < args.LastLogTerm || (lastEntry.Term == args.LastLogTerm && lastEntry.Index <= args.LastLogIndex) {
		uptodate = true
	}
	return uptodate
}

// State transition
func (rf *Raft) toLeader() {
	rf.state = 1
	rf.nextIndex = make([]int, len(rf.peers))
	for i := range rf.peers {
		rf.nextIndex[i] = len(rf.logs)
	}
	rf.matchIndex = make([]int, len(rf.peers))
}

func (rf *Raft) toFollower(term int) {
	rf.state = 0
	rf.CurrentTerm = term
	rf.VoteFor = -1
	rf.TotalVotes = 0
}

func (rf *Raft) saveLogs() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.processLogs(rf.lastApplied+1, rf.commitIndex)
}

func (rf *Raft) processLogs(startIndex, endIndex int) {
	for i := startIndex; i <= endIndex; i++ {
		rf.applyChannel <- ApplyMsg{
			Index:   i,
			Command: rf.logs[i].Command,
		}
		rf.lastApplied = i
	}
}

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
func shouldIgnoreVote(rf *Raft, args RequestVoteArgs, reply *RequestVoteReply) bool {
	return rf.state != 2 || args.Term != rf.CurrentTerm || reply.Term < rf.CurrentTerm
}

func shouldGrantVote(args RequestVoteArgs, rf *Raft) bool {
	return (rf.VoteFor == -1 || rf.VoteFor == args.CandidateId) && rf.isSenderLogUpToDate(args)
}

func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here.
	//fmt.Printf("server %d receive request vote from server %d, currentTerm %d, args.Term %d, state %d\n", rf.me, args.CandidateId, rf.CurrentTerm, args.Term, rf.state)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	if args.Term < rf.CurrentTerm {
		reply.Term = rf.CurrentTerm
		reply.VoteGranted = false
		return
	}
	if args.Term > rf.CurrentTerm {
		rf.toFollower(args.Term)
		rf.appendEntriesChannel <- true
	}
	reply.Term = rf.CurrentTerm
	reply.VoteGranted = shouldGrantVote(args, rf)
	if reply.VoteGranted {
		rf.VoteFor = args.CandidateId
		rf.voteToOthersChannel <- true
	}
}
func (rf *Raft) isMajority(votes int) bool {
	return votes == len(rf.peers)/2+1
}

func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	//fmt.Printf("server %d send request vote to server %d, currentTerm %d\n", rf.me, server, rf.CurrentTerm)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if !ok {
		return false
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if shouldIgnoreVote(rf, args, reply) {
		return false
	}

	if reply.Term > rf.CurrentTerm {
		rf.toFollower(reply.Term)
		rf.appendEntriesChannel <- true
		rf.persist()
		return false
	}

	if reply.VoteGranted {
		rf.TotalVotes++
		if rf.isMajority(rf.TotalVotes) {
			rf.toLeader()
			rf.electionSuccessChannel <- true
		}
	}

	return ok
}

func (rf *Raft) RequestVoteToAllPeers() {
	if rf.state != 2 {
		return
	}
	//fmt.Printf("server %d start election\n", rf.me)
	var args RequestVoteArgs
	args.Term = rf.CurrentTerm
	args.CandidateId = rf.me
	args.LastLogIndex = len(rf.logs) - 1
	args.LastLogTerm = rf.logs[len(rf.logs)-1].Term
	for i := range rf.peers {
		var reply RequestVoteReply
		if i != rf.me {
			go rf.sendRequestVote(i, args, &reply)
		}
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	//fmt.Printf("server %d receive append entries from server %d, preIndex %d, state=%d\n", rf.me, args.LeaderId, args.PrevLogIndex, rf.state)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	//defer fmt.Println("rf.me", rf.me, "term ", rf.CurrentTerm, "log ", rf.logs)

	if args.Term < rf.CurrentTerm {
		reply.Term = rf.CurrentTerm
		reply.Success = false
		return
	}

	if (rf.state == 1 && rf.CurrentTerm < args.Term) || (rf.state == 2 && rf.CurrentTerm <= args.Term) {
		// convert to follower
		rf.toFollower(args.Term)
		rf.appendEntriesChannel <- true
	}
	if args.Term > rf.CurrentTerm {
		rf.CurrentTerm = args.Term
	}
	reply.Term = rf.CurrentTerm
	if rf.state == 0 {
		// Current instance is newer than the leader, wait for next election.
		rf.appendEntriesChannel <- true
		// check if the log is consistent
		if args.PrevLogIndex >= len(rf.logs) || rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm {
			reply.Success = false
			return
		} else if len(args.Entries) > 0 {
			rf.logs = rf.logs[:args.PrevLogIndex+1]
			rf.logs = append(rf.logs, args.Entries...)
		}

		reply.Success = true
		reply.MatchIndex = len(rf.logs) - 1
		if args.LeaderCommit > rf.commitIndex {
			rf.commitIndex = min(args.LeaderCommit, len(rf.logs)-1)
			go func() {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
					applyMsg := ApplyMsg{}
					applyMsg.Index = i
					applyMsg.Command = rf.logs[i].Command
					rf.applyChannel <- applyMsg
					rf.lastApplied = i
				}
			}()
		}
	} else {
		reply.Success = false
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if !ok {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	if rf.state != 1 || args.Term != rf.CurrentTerm || reply.Term < rf.CurrentTerm {
		return
	}

	if reply.Term > rf.CurrentTerm {
		// There exists a new leader, step down to follower.
		rf.toFollower(args.Term)
		// Start the timer.
		rf.appendEntriesChannel <- true
		return
	}

	// Update local states.
	if reply.Success {
		// match index should not regress in case of stale rpc response
		newMatchIndex := args.PrevLogIndex + len(args.Entries)
		if newMatchIndex > rf.matchIndex[server] {
			//fmt.Println(368)
			rf.matchIndex[server] = newMatchIndex
		}
		rf.nextIndex[server] = rf.matchIndex[server] + 1
	} else {
		rf.nextIndex[server] -= 1
	}

	for n := len(rf.logs) - 1; n >= rf.commitIndex; n-- {
		count := 1
		if rf.logs[n].Term == rf.CurrentTerm {
			for i := range rf.peers {
				if i != rf.me && rf.matchIndex[i] >= n {
					count++
				}
			}
		}
		if count > len(rf.peers)/2 {
			rf.commitIndex = n
			go rf.saveLogs()
			break
		}
	}

	return
}

func (rf *Raft) getLogEntriesToSendForServer(server int) []LogEntry {
	entries := make([]LogEntry, 0)
	if rf.nextIndex[server] < len(rf.logs) {
		refEntriesToSend := rf.logs[rf.nextIndex[server]:]
		entries = make([]LogEntry, len(refEntriesToSend))
		copy(entries, refEntriesToSend)
	}
	return entries
}

func (rf *Raft) AppendEntriesToAllPeers() {
	// Send AppendEntries RPC to all peers.
	if rf.state != 1 {
		return
	}
	for i := range rf.peers {
		if i != rf.me {
			rf.mu.Lock()
			args := AppendEntriesArgs{
				Term:         rf.CurrentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: rf.nextIndex[i] - 1,
				PrevLogTerm:  rf.logs[rf.nextIndex[i]-1].Term,
				Entries:      rf.getLogEntriesToSendForServer(i),
				LeaderCommit: rf.commitIndex,
			}
			rf.mu.Unlock()
			reply := AppendEntriesReply{}
			go rf.sendAppendEntries(i, &args, &reply)
		}
	}
}

// The service using Raft (e.g. a k/v server) wants to start
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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	isLeader := rf.state == 1
	if !isLeader {
		return -1, rf.CurrentTerm, false
	}
	//fmt.Println("append ", command)
	term := rf.CurrentTerm
	index := len(rf.logs)
	rf.logs = append(rf.logs, LogEntry{term, command, index})
	rf.persist()
	return len(rf.logs) - 1, term, true
}

// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (rf *Raft) Kill() {
	// Your code here, if desired.
	//fmt.Printf("server %d is killed\n", rf.me)
}

func (rf *Raft) resetChannels() {
	rf.voteToOthersChannel = make(chan bool, 1)
	rf.appendEntriesChannel = make(chan bool, 1)
	rf.electionSuccessChannel = make(chan bool, 1)
}

func (rf *Raft) launchRaft() {
	for {
		switch rf.state {
		case 0:
			// follower
			select {
			case <-rf.appendEntriesChannel:
			case <-time.After(time.Duration(rand.Intn(200)+200) * time.Millisecond): // election timeout
				rf.mu.Lock()
				if rf.state == 0 {
					rf.resetChannels()
					rf.state = 2
					rf.CurrentTerm += 1
					rf.VoteFor = rf.me
					rf.TotalVotes = 1
					rf.persist()
					// rf.resetChannels()
					rf.RequestVoteToAllPeers()
				}
				rf.mu.Unlock()
			case <-rf.voteToOthersChannel: // receive from candidates, will be
			}
		case 1:
			// leader
			select {
			case <-time.After(120 * time.Millisecond):
				go rf.AppendEntriesToAllPeers()
			case <-rf.appendEntriesChannel:
			case <-rf.voteToOthersChannel:
			}
		case 2:
			// candidate
			select {
			case <-time.After(time.Duration(rand.Intn(200)+200) * time.Millisecond):
				rf.mu.Lock()
				rf.CurrentTerm += 1
				rf.VoteFor = rf.me
				rf.TotalVotes = 1
				// rf.resetChannels()
				rf.RequestVoteToAllPeers()
				rf.mu.Unlock()
			case <-rf.appendEntriesChannel:
			case <-rf.voteToOthersChannel:
			case <-rf.electionSuccessChannel:
				//fmt.Printf("server %d become leader\n", rf.me)
				//rf.resetChannels()
				go rf.AppendEntriesToAllPeers()
			}
		}
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {

	rf := &Raft{
		peers:        peers,
		persister:    persister,
		me:           me,
		applyChannel: applyCh,

		state:       0,
		CurrentTerm: 1,
		logs:        make([]LogEntry, 0),
		commitIndex: 0,
		lastApplied: 0,
		nextIndex:   make([]int, len(peers)),
		matchIndex:  make([]int, len(peers)),
	}
	rf.logs = append(rf.logs, LogEntry{0, nil, 0})
	rf.resetChannels()

	for i := range rf.nextIndex {
		rf.nextIndex[i] = 1
	}
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.persist()

	// launch the
	go rf.launchRaft()
	return rf
}
