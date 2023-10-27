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
	//	"bytes"
	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	CommandTerm	 int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type NodeState int
const (
	Follower NodeState = 0
	Candidate NodeState = 1
	Leader    NodeState = 2
)

type Entry struct {
	Index 	int
	Term 	int
	Command interface{}
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.RWMutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	applyCh			chan ApplyMsg
	applyCond		*sync.Cond
	replicatorCond	[]*sync.Cond
	state			NodeState

	currentTerm int
	votedFor    int
	logs        []Entry // the first entry is a dummy entry which contains LastSnapshotTerm, LastSnapshotIndex and nil Command

	commitIndex int
	lastApplied int
	nextIndex   []int
	matchIndex  []int

	electionTimer  *time.Timer
	heartbeatTimer *time.Timer
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term, isleader = rf.currentTerm, rf.state == Leader
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	w := new(bytes.Buffer)
	enc := labgob.NewEncoder(w)
	enc.Encode(rf.currentTerm)
	enc.Encode(rf.votedFor)
	enc.Encode(rf.logs)
	data := w.Bytes()
	rf.persister.Save(data, nil)
}


// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	r := bytes.NewBuffer(data)
	dec := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var logs []Entry
	if dec.Decode(&currentTerm) != nil || dec.Decode(&votedFor) != nil || dec.Decode(&logs) != nil {
		panic("readPersist failed")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.logs = logs
	}
}


// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}


// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term			int
	CandidateId		int
	LastLogIndex	int
	LastLogTerm		int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term			int
	VoteGranted		bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(request *RequestVoteArgs, response *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	defer DPrintf("{Node %v}'s state is {state %v,term %v,commitIndex %v,lastApplied %v,firstLog %v,lastLog %v} before processing requestVoteRequest %v and reply requestVoteResponse %v", rf.me, rf.state, rf.currentTerm, rf.commitIndex, rf.lastApplied, rf.getFirstLog(), rf.getLastLog(), request, response)

	if (request.Term < rf.currentTerm) || (request.Term == rf.currentTerm && rf.votedFor != -1 && rf.votedFor != request.CandidateId){
		response.Term, response.VoteGranted = rf.currentTerm, false
		return
	}
	if request.Term > rf.currentTerm {
		rf.ChangeState(Follower)
		rf.currentTerm, rf.votedFor = request.Term, -1
	}
	if !rf.isLogUpToDate(request.LastLogIndex, request.LastLogTerm) {
		response.Term, response.VoteGranted = rf.currentTerm, false
		return
	}
	rf.votedFor = request.CandidateId
	rf.electionTimer.Reset(RandomizedElectionTimeout())
	response.Term, response.VoteGranted = rf.currentTerm, true
}

func (rf *Raft) isLogUpToDate(lastLogIndex int, lastLogTerm int) bool {
	lastLog := rf.getLastLog()
	return lastLog.Term < lastLogTerm || (lastLog.Term == lastLogTerm && lastLog.Index <= lastLogIndex)
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}


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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != Leader {
		return -1, -1, false
	}
	newLog := rf.appendNewEntry(command)
	DPrintf("{Node %v} receives a new command[%v] to replicate in term %v", rf.me, newLog, rf.currentTerm)
	rf.BroadcastHeartbeat(false)
	return newLog.Index, newLog.Term, true
}

func (rf *Raft) appendNewEntry(command interface{}) Entry {
	defer rf.persist()
	lastLog := rf.getLastLog()
	newLog := Entry{Index: lastLog.Index + 1, Term: rf.currentTerm, Command: command}
	rf.logs = append(rf.logs, newLog)
	return newLog
}

type AppendEntriesRequest struct {
	Term 			int
	LeaderId		int
	PrevLogIndex	int
	PrevLogTerm		int
	Entries			[]Entry
	LeaderCommit	int
}

type AppendEntriesResponse struct {
	Term			int
	Success			bool
	ConflictTerm	int
	ConflictIndex	int
}

func (rf *Raft) genAppendEntriesRequest(prevLogIndex int) *AppendEntriesRequest {
	request := new(AppendEntriesRequest)
	request.Term = rf.currentTerm
	request.LeaderId = rf.me
	request.PrevLogIndex = prevLogIndex
	request.PrevLogTerm = rf.logs[prevLogIndex].Term
	request.Entries = make([]Entry, len(rf.logs[prevLogIndex+1:]))
	copy(request.Entries, rf.logs[prevLogIndex+1:])
	request.LeaderCommit = rf.commitIndex
	DPrintf("{Node %v} generates AppendEntriesRequest %v", rf.me, request)
	return request
}

func (rf *Raft) sendAppendEntries(peer int, request *AppendEntriesRequest, response *AppendEntriesResponse) bool {
	ok := rf.peers[peer].Call("Raft.AppendEntries", request, response)
	return ok
}

func (rf *Raft) handleAppendEntriesResponse(peer int, request *AppendEntriesRequest, response *AppendEntriesResponse) {
	if rf.state == Leader && rf.currentTerm == request.Term {
		if response.Success {
			rf.matchIndex[peer] = request.PrevLogIndex + len(request.Entries)
			rf.nextIndex[peer] = rf.matchIndex[peer] + 1
			rf.advanceCommitIndexForLeader()
		} else {
			if response.Term > rf.currentTerm {
				rf.ChangeState(Follower)
				rf.currentTerm, rf.votedFor = response.Term, -1
				rf.persist()
			} else if response.Term == rf.currentTerm {
				rf.nextIndex[peer] = response.ConflictIndex
				if response.ConflictTerm != -1 {
					firstIndex := rf.getFirstLog().Index
					for i := request.PrevLogIndex; i >= firstIndex; i-- {
						if rf.logs[i-firstIndex].Term == response.ConflictTerm {
							rf.nextIndex[peer] = i + 1
							break
						}
					}
				}
			}
		}
	}
}

func (rf *Raft) AppendEntries(request *AppendEntriesRequest, response *AppendEntriesResponse){
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	DPrintf("{Node %v}'s state is {state %v,term %v,commitIndex %v,lastApplied %v,firstLog %v,lastLog %v} before processing AppendEntriesRequest %v and reply AppendEntriesResponse %v", rf.me, rf.state, rf.currentTerm, rf.commitIndex, rf.lastApplied, rf.getFirstLog(), rf.getLastLog(), request, response)

	if request.Term < rf.currentTerm {
		response.Term, response.Success = rf.currentTerm, false
		return
	}

	if request.Term > rf.currentTerm {
		rf.currentTerm, rf.votedFor = request.Term, -1
	}

	DPrintf("{Node %v} HeartBeat", rf.me)
	rf.ChangeState(Follower)
	rf.electionTimer.Reset(RandomizedElectionTimeout())

	if request.PrevLogIndex < rf.getFirstLog().Index {
		response.Term, response.Success = 0, false
		DPrintf("{Node %v} receives unexpected AppendEntriesRequest %v from {Node %v} because prevLogIndex %v < firstLogIndex %v", rf.me, request, request.LeaderId, request.PrevLogIndex, rf.getFirstLog().Index)
		return
	}

	if !rf.matchLog(request.PrevLogIndex, request.PrevLogTerm) {
		response.Term, response.Success = rf.currentTerm, false
		lastIndex := rf.getLastLog().Index
		if lastIndex < request.PrevLogIndex {
			response.ConflictTerm, response.ConflictIndex = -1, lastIndex+1
		} else {
			firstIndex := rf.getFirstLog().Index
			response.ConflictTerm = rf.logs[request.PrevLogIndex-firstIndex].Term
			index := request.PrevLogIndex - 1
			for index >= firstIndex && rf.logs[index-firstIndex].Term == response.ConflictTerm {
				index--
			}
			response.ConflictIndex = index
		}
		return
	}

	firstIndex := rf.getFirstLog().Index
	for index, entry := range request.Entries {
		if entry.Index-firstIndex >= len(rf.logs) || rf.logs[entry.Index-firstIndex].Term != entry.Term {
			rf.logs = shrinkEntriesArray(append(rf.logs[:entry.Index-firstIndex], request.Entries[index:]...))
			break
		}
	}
	rf.advanceCommitIndexForFollower(request.LeaderCommit)
	response.Term, response.Success = rf.currentTerm, true
	DPrintf("{Node %v}'s state is {state %v,term %v,commitIndex %v,lastApplied %v,firstLog %v,lastLog %v} after processing AppendEntriesRequest %v and reply AppendEntriesResponse %v", rf.me, rf.state, rf.currentTerm, rf.commitIndex, rf.lastApplied, rf.getFirstLog(), rf.getLastLog(), request, response)
}

func shrinkEntriesArray(entries []Entry) []Entry {
	return entries
}

func (rf *Raft) advanceCommitIndexForLeader() {
	commitIndex := rf.commitIndex
	lastLogIndex := rf.getLastLog().Index
	for i := commitIndex + 1; i <= lastLogIndex; i++ {
		count := 1
		for peer := range rf.peers {
			if peer != rf.me && rf.matchIndex[peer] >= i {
				count++
			}
		}
		if count > len(rf.peers)/2 && rf.logs[i-rf.getFirstLog().Index].Term == rf.currentTerm {
			commitIndex = i
		}
	}
	if commitIndex != rf.commitIndex {
		rf.commitIndex = commitIndex
		rf.applyCond.Signal()
	}
}

func (rf *Raft) advanceCommitIndexForFollower(leadercommit int){
	if rf.commitIndex < leadercommit {
		lastIndex := rf.getLastLog().Index
		rf.commitIndex = Min(leadercommit, lastIndex)
		rf.applyCond.Signal()
	}
}

func (rf *Raft) matchLog(prevLogIndex int, prevLogTerm int) bool {
	if rf.getLastLog().Index < prevLogIndex {
		return false
	}
	if rf.logs[prevLogIndex].Term != prevLogTerm {
		return false
	}
	return true
}

func (rf *Raft) replicateOneRound(peer int) {
	rf.mu.RLock()
	if rf.state != Leader {
		rf.mu.RUnlock()
		return
	}

	prevLogIndex := rf.nextIndex[peer] - 1
	if prevLogIndex < rf.getFirstLog().Index {
		request := rf.genInstallSnapshotRequest()
		rf.mu.RUnlock()
		response := new(InstallSnapshotResponse)
		if rf.sendInstallSnapshot(peer, request, response) {
			rf.mu.Lock()
			rf.handleInstallSnapshotResponse(peer, request, response)
			rf.mu.Unlock()
		}
	} else {
		request := rf.genAppendEntriesRequest(prevLogIndex)
		rf.mu.RUnlock()
		response := new(AppendEntriesResponse)
		if rf.sendAppendEntries(peer, request, response) {
			rf.mu.Lock()
			rf.handleAppendEntriesResponse(peer, request, response)
			rf.mu.Unlock()
		}
	}
}

type InstallSnapshotRequest struct {
	Term 				int
	LearderId			int
	LastIncludedIndex	int
	LastIncludedTerm	int
	Offset				int
	Data				[]byte
	Done				bool
}

type InstallSnapshotResponse struct {
	Term				int
}

func (rf *Raft) genInstallSnapshotRequest() *InstallSnapshotRequest {
	request := new(InstallSnapshotRequest)
	return request
}

func (rf *Raft) sendInstallSnapshot(peer int, request *InstallSnapshotRequest, response *InstallSnapshotResponse) bool {
	ok := rf.peers[peer].Call("Raft.InstallSnapshot", request, response)
	return ok
}

func (rf *Raft) handleInstallSnapshotResponse(peer int, request *InstallSnapshotRequest, response *InstallSnapshotResponse) {

}


// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for !rf.killed() {
		// Your code here (2A)
		// Check if a leader election should be started.
		select {
		case <-rf.electionTimer.C:
			rf.mu.Lock()
			rf.ChangeState(Candidate)
			rf.currentTerm++
			rf.StartElection()
			rf.electionTimer.Reset(RandomizedElectionTimeout())
			rf.mu.Unlock()
		case <-rf.heartbeatTimer.C:
			rf.mu.Lock()
			if rf.state == Leader {
				DPrintf("{Node %v} Broadcast Heartbeat", rf.me)
				rf.BroadcastHeartbeat(true)
				rf.heartbeatTimer.Reset(StableHeartbeatTimeout())
			}
			rf.mu.Unlock()
		}

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) StartElection() {
	request := &RequestVoteArgs{
		Term: rf.currentTerm,
		CandidateId: rf.me,
		LastLogIndex: rf.getLastLog().Index,
		LastLogTerm: rf.getLastLog().Term,
	}
	DPrintf("{Node %v} starts election with RequestVoteRequest %v", rf.me, request)

	grantedVotes := 1
	rf.votedFor = rf.me
	rf.persist()
	for peer := range rf.peers{
		if peer == rf.me{
			continue
		}
		go func (peer int) {
			response := new(RequestVoteReply)
			if rf.sendRequestVote(peer, request, response) {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				DPrintf("{Node %v} receives RequestVoteResponse %v from {Node %v} after sending RequestVoteRequest %v in term %v", rf.me, response, peer, request, rf.currentTerm)
				if rf.currentTerm == request.Term && rf.state == Candidate {
					if response.VoteGranted {
						grantedVotes++
						if grantedVotes > len(rf.peers)/2 {
							DPrintf("{Node %v} receives majority votes in term %v", rf.me, rf.currentTerm)
							rf.ChangeState(Leader)
							rf.BroadcastHeartbeat(true)
						}
					} else if response.Term > rf.currentTerm {
						DPrintf("{Node %v} finds a new leader {Node %v} with term %v and steps down in term %v", rf.me, peer, response.Term, rf.currentTerm)
						rf.ChangeState(Follower)
						rf.currentTerm, rf.votedFor = response.Term, -1
						rf.persist()
					}
				}
			}
		}(peer)
	}
}

func (rf *Raft) BroadcastHeartbeat(isHeartBeat bool) {
	for peer := range rf.peers {
		if peer == rf.me{
			continue
		}

		if isHeartBeat {
			DPrintf("{Node %v} HeartBeat to Node %v", rf.me, peer)
			go rf.replicateOneRound(peer)
		} else {
			rf.replicatorCond[peer].Signal()
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
func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.dead = 0
	rf.applyCh = applyCh
	rf.replicatorCond = make([]*sync.Cond, len(peers))
	rf.state = Follower
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.logs = make([]Entry, 1)
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.heartbeatTimer = time.NewTimer(StableHeartbeatTimeout())
	rf.electionTimer = time.NewTimer(RandomizedElectionTimeout())


	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.applyCond = sync.NewCond(&rf.mu)
	lastLog := rf.getLastLog()
	for i := 0; i < len(peers); i++ {
		rf.matchIndex[i], rf.nextIndex[i] = 0, lastLog.Index+1
		if i != rf.me {
			rf.replicatorCond[i] = sync.NewCond(&sync.Mutex{})
			// start replicator goroutine to replicate entries in batch
			go rf.replicator(i)
		}
	}
	// start ticker goroutine to start elections
	go rf.ticker()
	// start applier goroutine to push committed logs into applyCh exactly once
	go rf.applier()

	return rf
}

func (rf *Raft) applier() {
	for !rf.killed() {
		rf.mu.Lock()
		for rf.lastApplied >= rf.commitIndex {
			rf.applyCond.Wait()
		}
		firstIndex, commitIndex, lastApplied := rf.getFirstLog().Index, rf.commitIndex, rf.lastApplied
		entries := make([]Entry, commitIndex-lastApplied)
		copy(entries, rf.logs[lastApplied+1-firstIndex:commitIndex+1-firstIndex])
		rf.mu.Unlock()
		for _, entry := range entries {
			rf.applyCh <- ApplyMsg{
				CommandValid: true,
				Command:      entry.Command,
				CommandTerm:  entry.Term,
				CommandIndex: entry.Index,
			}
		}
		rf.mu.Lock()
		DPrintf("{Node %v} applies entries %v-%v in term %v", rf.me, rf.lastApplied, commitIndex, rf.currentTerm)
		rf.lastApplied = Max(rf.lastApplied, commitIndex)
		rf.mu.Unlock()
	}
}

func Max(a, b int) int {
    if a > b {
        return a
    }
    return b
}

func Min(a, b int) int {
	if a < b {
        return a
    }
    return b
}

const (
	HeartbeatTimeout = 125
	ElectionTimeout  = 1000
)

func StableHeartbeatTimeout() time.Duration {
	return time.Duration(HeartbeatTimeout) * time.Millisecond
}

func RandomizedElectionTimeout() time.Duration {
	return time.Duration(ElectionTimeout+ rand.Intn(ElectionTimeout)) * time.Millisecond
}

func (rf *Raft) getFirstLog() Entry {
	return rf.logs[0]
}

func (rf *Raft) getLastLog() Entry {
	return rf.logs[len(rf.logs) - 1]
}

func (rf *Raft) replicator(peer int) {
	rf.replicatorCond[peer].L.Lock()
	defer rf.replicatorCond[peer].L.Unlock()
	for !rf.killed() {
		for !rf.needReplicating(peer) {
			rf.replicatorCond[peer].Wait()
		}
		rf.replicateOneRound(peer)
	}
}

func (rf *Raft) needReplicating(peer int) bool {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return rf.state == Leader && rf.matchIndex[peer] < rf.getLastLog().Index
}

func (rf *Raft) ChangeState(state NodeState) {
	rf.state = state
	switch state {
	case Leader:
		lastLogIndex := rf.getLastLog().Index
		for peer := range rf.peers {
			rf.matchIndex[peer], rf.nextIndex[peer] = 0, lastLogIndex+1
		}
		rf.heartbeatTimer.Reset(StableHeartbeatTimeout())
	}
}