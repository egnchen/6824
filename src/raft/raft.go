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
	"math/rand"
	"sort"

	//	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type RaftState int32

const (
	Leader RaftState = iota
	Candidate
	Follower
)

const TICKER_TIMEOUT = 50 * time.Millisecond
const INVALID_PEER_ID int = -1

type LogEntry struct {
	Index   int
	Term    int
	Command interface{}
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	state     RaftState
	stateCond *sync.Cond
	term      int
	votedFor  int
	log       []LogEntry

	// volatile state on all servers
	commitIndex int
	lastApplied int
	commitCond  *sync.Cond
	applyCh     chan ApplyMsg

	// volatile state on leader
	nextIndex  []int
	matchIndex []int
	appendCond *sync.Cond
	// volatile state on follower
	lastHeartbeat time.Time // last time heartbeat is heard from leader
}

func (rf *Raft) getElectionTimeout() time.Duration {
	return time.Duration(rand.Int()%500+750) * time.Millisecond
}

func (rf *Raft) getHeartbeatTimeout() time.Duration {
	return time.Duration(100) * time.Millisecond
}

// getState returns the current raft state in a thread-safe manner
func (rf *Raft) getState() RaftState {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.state
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term := rf.term
	isLeader := rf.state == Leader
	return term, isLeader
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
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
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
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	DPrintf("%v AE from %v term=%v prev=%v/%v len(e)=%v lc=%v", rf.me, args.LeaderId,
		args.Term, args.PrevLogTerm, args.PrevLogIndex, len(args.Entries), args.LeaderCommit)
	reply.Success = true
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// update term first
	if args.Term > rf.term {
		rf.term = args.Term
		rf.toFollowerLocked()
	}
	reply.Term = rf.term

	if args.Term < rf.term {
		reply.Success = false
		return
	}
	// check log entry @ prevLogIndex matches prevLogTerm
	rf.lastHeartbeat = time.Now()
	entry := rf.getLogAtIndexLocked(args.PrevLogIndex)
	if entry == nil && args.PrevLogIndex > 0 {
		DPrintf("%v prevLogIndex(%v) too large", rf.me, args.PrevLogIndex)
		reply.Success = false
		return
	} else if entry != nil && entry.Term != args.PrevLogTerm {
		DPrintf("%v log entry @ prevLogIndex(%v) term=%v doesn't match args.term=%v",
			rf.me, args.PrevLogIndex, entry.Term, args.PrevLogTerm)
		reply.Success = false
		return
	}
	// if an existing entry conflicts with the new one, replace it & delete all that follows
	rf.appendLogsLocked(args.Entries, args.PrevLogIndex)
	// set commitIndex to min(lastLogIndex, args.LeaderCommit)
	if args.LeaderCommit > rf.commitIndex {
		lastEntryIndex := rf.getLastLogIndexLocked()
		if lastEntryIndex <= args.LeaderCommit {
			rf.commitIndex = lastEntryIndex
		} else {
			rf.commitIndex = args.LeaderCommit
		}
		rf.commitCond.Signal()
	}
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.term {
		DPrintf("%v refused RV from %v: term %v < %v", rf.me, args.CandidateId, args.Term, rf.term)
		reply.VoteGranted = false
		reply.Term = rf.term
		return
	}
	// rules for all servers #2
	if rf.term < args.Term {
		rf.term = args.Term
		rf.votedFor = INVALID_PEER_ID
		rf.toFollowerLocked()
	}
	// invariant rf.term == args.Term
	Assert(args.Term == rf.term)
	reply.Term = rf.term
	if rf.votedFor == INVALID_PEER_ID || rf.votedFor == args.CandidateId {
		// check if log is up-to-date(below from paper 5.4.1)
		// Raft determines which of two logs is more up-to-date
		// by comparing the index and term of the last entries in the logs.
		// If the logs have last entries with different terms, then the log with the later term is more up-to-date.
		// If the logs end with the same term, then whichever log is longer is more up-to-date.
		llTerm := rf.getLastLogTermLocked()
		if llTerm != args.LastLogTerm {
			reply.VoteGranted = args.LastLogTerm > llTerm
		} else {
			reply.VoteGranted = args.LastLogTerm >= rf.getLastLogIndexLocked()
		}
		if reply.VoteGranted {
			rf.votedFor = args.CandidateId
		}
	} else {
		reply.VoteGranted = false
	}
	DPrintf("%v RV from %v granted=%v term=%v", rf.me, args.CandidateId, reply.VoteGranted, reply.Term)
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
func (rf *Raft) Start(command interface{}) (index int, term int, isLeader bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != Leader {
		return -1, -1, false
	} else {
		entry := LogEntry{
			Index:   rf.getLastLogIndexLocked() + 1,
			Term:    rf.term,
			Command: command,
		}
		rf.log = append(rf.log, entry)
		rf.nextIndex[rf.me] = entry.Index + 1
		rf.matchIndex[rf.me] = entry.Index
		DPrintf("%v received entry #%v", rf.me, entry.Index)
		rf.appendCond.Broadcast()
		return entry.Index, entry.Term, true
	}
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
	// broadcast on all Cond
	rf.stateCond.Broadcast()
	rf.appendCond.Broadcast()
	rf.commitCond.Broadcast()
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) getLastLogIndexLocked() int {
	return len(rf.log)
}

func (rf *Raft) getLastLogTermLocked() int {
	if len(rf.log) > 0 {
		return rf.log[len(rf.log)-1].Term
	} else {
		return 0
	}
}

// runElection runs a single term of election
func (rf *Raft) runElection(electionTimeout time.Duration) {
	rf.mu.Lock()
	// increment current term
	rf.term++
	rf.votedFor = rf.me

	args := RequestVoteArgs{
		Term:         rf.term,
		CandidateId:  rf.me,
		LastLogIndex: rf.getLastLogIndexLocked(),
		LastLogTerm:  rf.getLastLogTermLocked(),
	}
	// vote counter - wait for majority and vote for self
	counter := int32((len(rf.peers) + 1) / 2)
	Assert(counter >= 2)
	atomic.AddInt32(&counter, -1)

	// chan for final notification
	counterZeroChan := make(chan struct{})
	rf.mu.Unlock()

	// send RequestVote RPC to all peers(except self)
	for i := range rf.peers {
		if i == args.CandidateId {
			continue
		}
		serverId := i
		go func() {
			reply := RequestVoteReply{}
			DPrintf("%v send RV to %v term=%v", args.CandidateId, serverId, args.Term)
			ok := rf.sendRequestVote(serverId, &args, &reply)
			if ok {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if rf.term < reply.Term {
					rf.term = reply.Term
					rf.toFollowerLocked()
				} else if reply.VoteGranted {
					cnt := atomic.AddInt32(&counter, -1)
					if cnt == 0 {
						counterZeroChan <- struct{}{}
					}
				}
			}
			// TODO(2A) maybe retry if failed?
		}()
	}
	select {
	case <-counterZeroChan:
		// election succeed
		rf.mu.Lock()
		defer rf.mu.Unlock()
		rf.toLeaderLocked()
	case <-time.After(electionTimeout):
		// timeout, fail silently and wait for ticker to do next round
	}
}

func (rf *Raft) toFollowerLocked() {
	if rf.state == Follower {
		return
	}
	DPrintf("%v converting to follower", rf.me)
	rf.state = Follower
	rf.stateCond.Broadcast()
}

func (rf *Raft) toLeaderLocked() {
	if rf.state == Leader {
		return
	}
	DPrintf("%v converting to leader", rf.me)
	rf.state = Leader
	// nextIndex initialized to last log index + 1
	rf.nextIndex = make([]int, len(rf.peers))
	for i := range rf.nextIndex {
		rf.nextIndex[i] = rf.getLastLogIndexLocked() + 1
	}
	// matchIndex initialized to 0
	rf.matchIndex = make([]int, len(rf.peers))
	rf.stateCond.Broadcast()
}

func (rf *Raft) getLogsStartingFromIndexLocked(index int) []LogEntry {
	if index == 0 {
		DPrintf("%v Warning: starting from index 0", rf.me)
		index = 1
	}
	return rf.log[index-1:]
}

// truncate the log till prevLogIndex and append entries behind them
func (rf *Raft) appendLogsLocked(entries []LogEntry, prevLogIndex int) {
	rf.log = rf.log[:prevLogIndex]
	rf.log = append(rf.log, entries...)
}

func (rf *Raft) getLogAtIndexLocked(index int) *LogEntry {
	if index == 0 || index > len(rf.log) {
		return nil
	}
	return &rf.log[index-1]
}

// convert to candidate and start election routine
func (rf *Raft) toCandidateLocked(electionTimeout time.Duration) {
	if rf.state == Candidate {
		return
	}
	DPrintf("%v converting to candidate", rf.me)
	rf.state = Candidate
	go rf.runElection(electionTimeout)
	rf.stateCond.Broadcast()
}

// applyLoop listen to change in commitIndex and apply changes to
// the state machine
func (rf *Raft) applyLoop() {
	for !rf.killed() {
		rf.mu.Lock()
		for !rf.killed() && rf.commitIndex == rf.lastApplied {
			rf.commitCond.Wait()
		}
		if rf.killed() {
			break
		}
		for ; rf.lastApplied < rf.commitIndex; rf.lastApplied++ {
			entry := rf.getLogAtIndexLocked(rf.lastApplied + 1)
			rf.mu.Unlock()
			rf.applyCh <- ApplyMsg{
				CommandValid: true,
				Command:      entry.Command,
				CommandIndex: entry.Index,
				// TODO(2D)
				/*
					SnapshotValid: false,
					Snapshot:      nil,
					SnapshotTerm:  0,
					SnapshotIndex: 0,
				*/
			}
			rf.mu.Lock()
		}
		DPrintf("%v lastApplied @ %v", rf.me, rf.lastApplied)
		rf.mu.Unlock()
	}
}

// heartbeatLoop sends AppendEntries RPC to peer #serverId periodically
func (rf *Raft) heartbeatLoop(serverId int) {
	for rf.killed() == false {
		rf.mu.Lock()
		// wait until we're leader
		for rf.killed() == false && rf.state != Leader {
			rf.stateCond.Wait()
		}
		if rf.killed() {
			rf.mu.Unlock()
			break
		}
		// update the heartbeat timer directly
		rf.lastHeartbeat = time.Now()
		// construct log entries
		var entries []LogEntry
		if rf.getLastLogIndexLocked() >= rf.nextIndex[serverId] {
			entries = rf.getLogsStartingFromIndexLocked(rf.nextIndex[serverId])
		}
		prevEntry := rf.getLogAtIndexLocked(rf.nextIndex[serverId] - 1)
		prevLogIndex := 0
		prevLogTerm := 0
		if prevEntry != nil {
			prevLogIndex = prevEntry.Index
			prevLogTerm = prevEntry.Term
		}
		args := AppendEntriesArgs{
			Term:         rf.term,
			LeaderId:     rf.me,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
			Entries:      entries,
			LeaderCommit: rf.commitIndex,
		}
		go rf.sendHeartbeat(serverId, &args)
		rf.mu.Unlock()
		time.Sleep(rf.getHeartbeatTimeout())
		// TODO(2B) wait for appendCond
		// TODO(2B) retry immediately when AE RPC fails
	}
	//DPrintf("%v heartbeat routine exited", rf.me)
}

// sendHeartbeat sends AE to peer and deal with the response.
// This function should be run in a separate goroutine.
func (rf *Raft) sendHeartbeat(serverId int, args *AppendEntriesArgs) {
	reply := AppendEntriesReply{}
	ok := rf.sendAppendEntries(serverId, args, &reply)
	if ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		// change to follower if term is larger
		if reply.Term > rf.term {
			Assert(!reply.Success)
			rf.term = reply.Term
			rf.toFollowerLocked()
			return
		}
		// If AE fails because of log inconsistency, decrement nextIndex and retry
		if !reply.Success {
			rf.nextIndex[serverId]--
			// TODO(2B) retry immediately
		} else {
			// update nextIndex and matchIndex
			rf.nextIndex[serverId] = rf.getLastLogIndexLocked() + 1
			rf.matchIndex[serverId] = rf.getLastLogIndexLocked()
			// update commitIndex
			matchIndexes := make([]int, len(rf.matchIndex))
			copy(matchIndexes, rf.matchIndex)
			sort.Ints(matchIndexes)
			N := matchIndexes[len(matchIndexes)/2]
			DPrintf("%v %v N=%v", rf.me, rf.matchIndex, N)
			if N > rf.commitIndex {
				entry := rf.getLogAtIndexLocked(N)
				Assert(entry != nil)
				if entry.Term == rf.term {
					rf.commitIndex = N
					rf.commitCond.Signal()
				}
			}
		}
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// The tickerLoop go routine starts a new election if this peer hasn't received
// heartbeats recently.
func (rf *Raft) tickerLoop() {
	sleepTime := rf.getElectionTimeout()
	lastHeartbeat := rf.lastHeartbeat
	for rf.killed() == false {
		rf.mu.Lock()
		if sleepTime < 0 {
			// start election
			sleepTime = rf.getElectionTimeout()
			rf.toCandidateLocked(sleepTime)
		}
		if lastHeartbeat != rf.lastHeartbeat {
			rf.lastHeartbeat = lastHeartbeat
			sleepTime = rf.getElectionTimeout()
		} else {
			sleepTime -= TICKER_TIMEOUT
		}
		rf.mu.Unlock()
		if sleepTime > TICKER_TIMEOUT {
			time.Sleep(TICKER_TIMEOUT)
		} else {
			// make it more diverse
			time.Sleep(TICKER_TIMEOUT / 5)
		}
	}
	//DPrintf("%v ticker routine exited", rf.me)
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
	// TODO initialization (2C).
	rf := &Raft{
		peers:     peers,
		persister: persister,
		me:        me,
		dead:      0,
		state:     Follower,
		term:      0,
		votedFor:  INVALID_PEER_ID,
		//log:         nil,
		commitIndex: 0,
		lastApplied: 0,
		applyCh:     applyCh,
		//nextIndex:   nil,
		//matchIndex:  nil,
	}
	rf.stateCond = sync.NewCond(&rf.mu)
	rf.commitCond = sync.NewCond(&rf.mu)
	rf.appendCond = sync.NewCond(&rf.mu)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.tickerLoop()
	for i := range rf.peers {
		if i != rf.me {
			go rf.heartbeatLoop(i)
		}
	}
	go rf.applyLoop()

	return rf
}
