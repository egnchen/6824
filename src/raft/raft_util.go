package raft

import (
	"fmt"
	"math/rand"
	"sort"
	"time"
)

func (rf *Raft) updateTerm(newTerm int) {
	Assert(newTerm > rf.term)
	rf.term = newTerm
	rf.persist()
}

func (rf *Raft) updateTermAndVotedFor(newTerm int, votedFor int) {
	if newTerm == rf.term && votedFor == rf.votedFor {
		return
	}
	Assert(newTerm >= rf.term)
	rf.term = newTerm
	rf.votedFor = votedFor
	rf.persist()
}

func (rf *Raft) getElectionTimeout() time.Duration {
	return time.Duration(rand.Int()%500+250) * time.Millisecond
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

func (rf *Raft) getLogsStartingFromIndexLocked(index int) []LogEntry {
	if index == 0 {
		DPrintf("%v Warning: starting from index 0", rf.me)
		index = 1
	}
	// TODO make a copy here to avoid data race, maybe better solution?
	ret := make([]LogEntry, len(rf.log[index-1:]))
	Assert(copy(ret, rf.log[index-1:]) == len(ret))
	return ret
}

// truncate the log till prevLogIndex and append entries behind them
func (rf *Raft) appendLogsLocked(prevLogIndex int, entries ...LogEntry) {
	if len(entries) == 0 {
		return
	}
	i := prevLogIndex
	if Debug {
		// find the first conflicting log entry
		for ; i < len(rf.log); i++ {
			if rf.log[i].Term != entries[i-prevLogIndex].Term {
				if rf.log[i].Index <= rf.commitIndex {
					// panic
					fmt.Println("ERROR: overwriting committed entries")
					fmt.Printf("Conflict: log[%v]=%v != %v\n", i, rf.log[i], entries[i-prevLogIndex])
					fmt.Println(entries)
					fmt.Println(rf.log[rf.commitIndex:])
					panic("ERROR")
				}
				break
			}
		}
	}
	rf.log = rf.log[:i]
	rf.log = append(rf.log, entries[i-prevLogIndex:]...)
	rf.persist()
}

func (rf *Raft) getLogAtIndexLocked(index int) *LogEntry {
	if index <= 0 || index > len(rf.log) {
		return nil
	}
	return &rf.log[index-1]
}

func (rf *Raft) getFirstLogOfTermLocked(term int) *LogEntry {
	if len(rf.log) == 0 {
		return nil
	}
	if term == 0 || term > rf.getLastLogTermLocked() {
		return nil
	}
	idx := sort.Search(len(rf.log), func(i int) bool {
		return rf.log[i].Term >= term
	})
	return &rf.log[idx]
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}
