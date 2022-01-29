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
	if len(rf.log) > 0 {
		return rf.log[len(rf.log)-1].Index
	} else {
		return rf.snapshotIndex
	}
}

func (rf *Raft) getLastLogTermLocked() int {
	if len(rf.log) > 0 {
		return rf.log[len(rf.log)-1].Term
	} else {
		return rf.snapshotTerm
	}
}

func (rf *Raft) getLogsStartingFromIndexLocked(index int) []LogEntry {
	if index == 0 {
		DPrintf("%v Warning: starting from index 0", rf.me)
		index = 1
	}
	ret := make([]LogEntry, len(rf.log[index-1:]))
	for i := index - 1; i < len(rf.log); i++ {
		ret[i-index+1] = *rf.log[i]
	}
	return ret
}

// truncate the log till prevLogIndex and append entries behind them
func (rf *Raft) appendLogsLocked(prevLogIndex int, entries ...LogEntry) {
	if len(entries) == 0 {
		return
	}
	i := prevLogIndex
	// find the first conflicting log entry
	for ; i < len(rf.log) && i-prevLogIndex < len(entries); i++ {
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
	// delete & append only if conflict
	if i-prevLogIndex < len(entries) {
		rf.log = rf.log[:i]
		// TODO maybe not GC-friendly
		for _, v := range entries {
			rf.log = append(rf.log, &v)
		}
		rf.persist()
	}
}

// getLogOffsetOfIndexLocked returns the offset in log[] array
// of the LogEntry with given index. -1 if not found.
func (rf *Raft) getLogOffsetOfIndexLocked(index int) int {
	if len(rf.log) == 0 {
		return -1
	}
	offset := index - rf.snapshotIndex - 1
	if offset < 0 || offset >= len(rf.log) {
		return -1
	}
	return offset
}

func (rf *Raft) getLogOfIndexLocked(index int) *LogEntry {
	offset := rf.getLogOffsetOfIndexLocked(index)
	if offset == -1 {
		return nil
	} else {
		return rf.log[offset]
	}
}

func (rf *Raft) getFirstLogOfTermLocked(term int) *LogEntry {
	Assert(term >= rf.snapshotIndex && term <= rf.getLastLogIndexLocked())
	idx := sort.Search(len(rf.log), func(i int) bool {
		return rf.log[i].Term >= term
	})
	if idx < len(rf.log) {
		return rf.log[idx]
	} else {
		return nil
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}
