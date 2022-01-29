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
	"6.824/labgob"
	"bytes"
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

	// persistent state on all servers
	state         RaftState
	term          int
	votedFor      int
	snapshotIndex int
	snapshotTerm  int
	log           []*LogEntry // stored as pointers to be more GC-friendly

	// volatile state on all servers
	commitIndex int
	lastApplied int

	// volatile state on leader
	nextIndex  []int
	matchIndex []int

	// channels and cond vars
	stateCond  *sync.Cond
	commitCond *sync.Cond
	applyCh    chan ApplyMsg

	retryChan     []chan struct{} // serve as condition variable for broadcast
	heartbeatChan chan struct{}   // serve as condition variable for signal
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
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.term)
	e.Encode(rf.votedFor)
	e.Encode(rf.snapshotIndex)
	e.Encode(rf.snapshotTerm)
	e.Encode(len(rf.log))
	for _, l := range rf.log {
		e.Encode(*l)
	}
	rf.persister.SaveRaftState(w.Bytes())
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 {
		// bootstrap without any state?
		rf.mu.Lock()
		defer rf.mu.Unlock()
		rf.term = 0
		rf.votedFor = INVALID_PEER_ID
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var term int
	var votedFor int
	var snapshotIndex int
	var snapshotTerm int
	var logLen int
	var log []*LogEntry
	d.Decode(&term)
	d.Decode(&votedFor)
	d.Decode(snapshotIndex)
	d.Decode(snapshotTerm)
	d.Decode(logLen)
	log = make([]*LogEntry, logLen)
	for i := 0; i < logLen; i++ {
		log[i] = &LogEntry{}
		d.Decode(log[i])
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.term = term
	rf.votedFor = votedFor
	rf.snapshotIndex = snapshotIndex
	rf.snapshotTerm = snapshotTerm
	rf.log = log
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
	// raft paper page 7~8, optimization
	// predicted next nextIndex provided by follower
	PiggybackIndex int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	DPrintf("%v AE from %v term=%v prev=%v/%v len(e)=%v lc=%v", rf.me, args.LeaderId,
		args.Term, args.PrevLogTerm, args.PrevLogIndex, len(args.Entries), args.LeaderCommit)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// update term first
	if args.Term > rf.term {
		rf.updateTerm(args.Term)
		rf.toFollower()
	}

	reply.Term = rf.term
	if args.Term < rf.term {
		DPrintf("%v received AE term(%v) < %v", rf.me, args.Term, rf.term)
		reply.Success = false
		return
	}
	NotifyChan(rf.heartbeatChan)

	// check log entry @ prevLogIndex matches prevLogTerm
	entry := rf.getLogOfIndexLocked(args.PrevLogIndex)
	if entry == nil && args.PrevLogIndex > 0 {
		DPrintf("%v prevLogIndex(%v) too large", rf.me, args.PrevLogIndex)
		reply.Success = false
		reply.PiggybackIndex = rf.getLastLogIndexLocked() + 1
		DPrintf("%v setting piggyback to %v", rf.me, reply.PiggybackIndex)
		return
	} else if entry != nil && entry.Term != args.PrevLogTerm {
		DPrintf("%v log entry @ prevLogIndex(%v) %v doesn't match %v",
			rf.me, args.PrevLogIndex, entry.Term, args.PrevLogTerm)
		reply.Success = false
		firstEntry := rf.getFirstLogOfTermLocked(entry.Term)
		Assert(firstEntry != nil)
		reply.PiggybackIndex = firstEntry.Index
		if reply.PiggybackIndex <= rf.commitIndex {
			reply.PiggybackIndex = rf.commitIndex + 1
		}
		DPrintf("%v setting piggyback to %v", rf.me, reply.PiggybackIndex)
		return
	}
	// if an existing entry conflicts with the new one, replace it & delete all that follows
	rf.appendLogsLocked(args.PrevLogIndex, args.Entries...)
	DPrintf("%v last log index @%v", rf.me, rf.getLastLogIndexLocked())
	// set commitIndex to min(lastLogIndex, args.LeaderCommit)
	newCommitIndex := rf.getLastLogIndexLocked()
	if newCommitIndex > args.LeaderCommit {
		newCommitIndex = args.LeaderCommit
	}
	if newCommitIndex > rf.commitIndex {
		rf.commitIndex = newCommitIndex
		rf.commitCond.Signal()
	}
	reply.Success = true
}

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
	newTerm := rf.term
	newVotedFor := rf.votedFor
	if newTerm < args.Term {
		newTerm = args.Term
		newVotedFor = INVALID_PEER_ID
		rf.toFollower()
	}
	// invariant newTerm == args.Term
	Assert(args.Term == newTerm)
	reply.Term = newTerm
	if newVotedFor == INVALID_PEER_ID || newVotedFor == args.CandidateId {
		// check if log is up-to-date(below from paper 5.4.1)
		// Raft determines which of two logs is more up-to-date
		// by comparing the index and term of the last entries in the logs.
		// If the logs have last entries with different terms, then the log with the later term is more up-to-date.
		// If the logs end with the same term, then whichever log is longer is more up-to-date.
		llTerm := rf.getLastLogTermLocked()
		if llTerm != args.LastLogTerm {
			reply.VoteGranted = args.LastLogTerm > llTerm
		} else {
			reply.VoteGranted = args.LastLogIndex >= rf.getLastLogIndexLocked()
		}
		if reply.VoteGranted {
			newVotedFor = args.CandidateId
		} else {
			DPrintf("%v %v/%v not up-to-date with %v/%v", rf.me, args.LastLogTerm, args.LastLogIndex,
				rf.getLastLogTermLocked(), rf.getLastLogIndexLocked())
		}
	} else {
		DPrintf("%v already voted for %v term=%v", rf.me, newVotedFor, newTerm)
		reply.VoteGranted = false
	}
	rf.updateTermAndVotedFor(newTerm, newVotedFor)
	DPrintf("%v RV from %v granted=%v term=%v", rf.me, args.CandidateId, reply.VoteGranted, reply.Term)
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
		rf.appendLogsLocked(rf.getLastLogIndexLocked(), entry)
		rf.nextIndex[rf.me] = entry.Index + 1
		rf.matchIndex[rf.me] = entry.Index
		DPrintf("%v received entry #%v", rf.me, entry.Index)
		// TODO(2C) send AE to all peers
		//for _, c := range rf.retryChan {
		//	NotifyChan(c)
		//}
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// notify on all channels so that goroutines could exit
	NotifyChan(rf.heartbeatChan)
	for _, c := range rf.retryChan {
		NotifyChan(c)
	}
	// broadcast on all Cond
	rf.stateCond.Broadcast()
	rf.commitCond.Broadcast()
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// runElection runs a single term of election
func (rf *Raft) runElection(electionTimeout time.Duration) {
	rf.mu.Lock()
	// increment current term
	rf.updateTermAndVotedFor(rf.term+1, rf.me)
	DPrintf("%v election timeout=%v", rf.me, electionTimeout)

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
					rf.updateTerm(reply.Term)
					rf.toFollower()
				} else if reply.VoteGranted {
					cnt := atomic.AddInt32(&counter, -1)
					if cnt == 0 {
						close(counterZeroChan)
					}
				}
			}
		}()
	}
	select {
	case <-counterZeroChan:
		// election succeeded
		rf.mu.Lock()
		defer rf.mu.Unlock()
		rf.toLeader() // note that if current is not candidate, won't convert to leader
	case <-time.After(electionTimeout):
		// timeout, fail silently and wait for ticker to do next round
	}
}

// convert to follower with mutex held
func (rf *Raft) toFollower() {
	if rf.state == Follower {
		return
	}
	DPrintf("%v converting to follower", rf.me)
	rf.state = Follower
	rf.stateCond.Broadcast()
}

// convert to leader with mutex held
func (rf *Raft) toLeader() {
	if rf.state != Candidate {
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
	// retryChan are re-initialized
	rf.retryChan = make([]chan struct{}, 0, len(rf.peers))
	for range rf.peers {
		rf.retryChan = append(rf.retryChan, make(chan struct{}, 1))
	}
	rf.stateCond.Broadcast()
}

// convert to candidate and start election routine
func (rf *Raft) toCandidate(electionTimeout time.Duration) {
	// we can convert from Candidate and Follower, but not from Leader
	if rf.state == Leader {
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
			rf.mu.Unlock()
			break
		}
		for ; rf.lastApplied < rf.commitIndex; rf.lastApplied++ {
			entry := rf.getLogOfIndexLocked(rf.lastApplied + 1)
			msg := ApplyMsg{
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
			rf.mu.Unlock()
			rf.applyCh <- msg
			rf.mu.Lock()
		}
		DPrintf("%v lastApplied @ %v", rf.me, rf.lastApplied)
		rf.mu.Unlock()
	}
}

// heartbeatLoop sends AppendEntries RPC to peer #serverId periodically
func (rf *Raft) heartbeatLoop(serverId int) {
	for !rf.killed() {
		rf.mu.Lock()
		// wait until we're leader
		for !rf.killed() && rf.state != Leader {
			rf.stateCond.Wait()
		}
		if rf.killed() {
			rf.mu.Unlock()
			break
		}
		if serverId == rf.me {
			// send heartbeat to self - no need for RPC
			NotifyChan(rf.heartbeatChan)
		} else {
			// construct log entries
			var entries []LogEntry
			if rf.getLastLogIndexLocked() >= rf.nextIndex[serverId] {
				entries = rf.getLogsStartingFromIndexLocked(rf.nextIndex[serverId])
			}
			prevEntry := rf.getLogOfIndexLocked(rf.nextIndex[serverId] - 1)
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
			DrainChan(rf.retryChan[serverId])
			go rf.sendHeartbeat(serverId, &args)
		}
		rf.mu.Unlock()
		t := time.NewTimer(rf.getHeartbeatTimeout())
		select {
		case <-t.C:
		case <-rf.retryChan[serverId]:
			t.Stop()
		}
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
		if rf.state != Leader {
			return
		}
		if reply.Term > args.Term {
			Assert(!reply.Success)
			if reply.Term > rf.term {
				rf.updateTerm(reply.Term)
				rf.toFollower()
			}
			return
		}
		// If AE fails because of log inconsistency, decrement nextIndex and retry
		if !reply.Success {
			// raft paper page 7-8 optimization
			if reply.PiggybackIndex > 0 {
				// Invariant is PiggybackIndex < args.PrevLogIndex
				// but not PiggybackIndex < rf.nextIndex[serverId](might have changed)
				if reply.PiggybackIndex < rf.nextIndex[serverId] {
					rf.nextIndex[serverId] = reply.PiggybackIndex
				} else {
					DPrintf("%v ignoring piggyback(%v >= %v) from %v",
						rf.me, reply.PiggybackIndex, rf.nextIndex[serverId], rf.me)
				}
			} else {
				rf.nextIndex[serverId]--
				Assert(rf.nextIndex[serverId] > 0)
				//if rf.nextIndex[serverId] <= 0 {
				//	fmt.Printf("WARNING nextIndex[%v] = %v(decrement)\n", serverId, rf.nextIndex[serverId])
				//	fmt.Printf("args: %v\n", args)
				//	fmt.Printf("reply: %v\n", reply)
				//	rf.nextIndex[serverId] = 1
				//}
			}
			NotifyChan(rf.retryChan[serverId])
		} else {
			// update nextIndex and matchIndex
			matchIndex := args.PrevLogIndex
			if len(args.Entries) > 0 {
				matchIndex = args.Entries[len(args.Entries)-1].Index
			}
			rf.nextIndex[serverId] = matchIndex + 1
			rf.matchIndex[serverId] = matchIndex
			// update commitIndex
			matchIndexes := make([]int, len(rf.matchIndex))
			copy(matchIndexes, rf.matchIndex)
			sort.Ints(matchIndexes)
			N := matchIndexes[len(matchIndexes)/2]
			DPrintf("%v %v N=%v", rf.me, rf.matchIndex, N)
			if N > rf.commitIndex {
				entry := rf.getLogOfIndexLocked(N)
				if entry.Term == rf.term {
					rf.commitIndex = N
					rf.commitCond.Signal()
					// TODO(2C) retry immediately to commit to all servers
					//for _, c := range rf.retryChan {
					//	NotifyChan(c)
					//}
				}
			}
		}
	}
}

// The tickerLoop go routine starts a new election if this peer hasn't received
// heartbeats recently.
func (rf *Raft) tickerLoop() {
	t := time.NewTimer(rf.getElectionTimeout())
	for !rf.killed() {
		select {
		case <-rf.heartbeatChan:
			// all good, continue to next loop
			SafeResetTimer(t, rf.getElectionTimeout())
		case <-t.C:
			rf.mu.Lock()
			nextElectionTimeout := rf.getElectionTimeout()
			rf.toCandidate(nextElectionTimeout)
			t.Reset(nextElectionTimeout)
			rf.mu.Unlock()
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
	rf := &Raft{
		peers:     peers,
		persister: persister,
		me:        me,
		dead:      0,
		state:     Follower,
		// initialized by readPersist
		//term:      0,
		//votedFor:  INVALID_PEER_ID,
		//log:         nil,
		//snapshotIndex: 0,
		//snapshotTerm: 0,
		commitIndex: 0,
		lastApplied: 0,
		applyCh:     applyCh,
		// initialized by toLeader
		//nextIndex:   nil,
		//matchIndex:  nil,
		heartbeatChan: make(chan struct{}, 1),
	}
	rf.stateCond = sync.NewCond(&rf.mu)
	rf.commitCond = sync.NewCond(&rf.mu)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.tickerLoop()
	for i := range rf.peers {
		go rf.heartbeatLoop(i)
	}
	go rf.applyLoop()

	return rf
}
