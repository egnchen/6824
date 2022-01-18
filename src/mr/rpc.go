package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"time"
)
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type MRTaskType int

const (
	TYPE_NONE MRTaskType = iota
	TYPE_MAP
	TYPE_REDUCE
)

const INVALID_TASK_ID int = -1

type MRTask struct {
	Typ             MRTaskType
	CurTaskId       int
	NMap            int
	NReduce         int
	LastExecuteTime time.Time
	Filename        string
}

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

type GetNewTaskArgs struct {
	CompleteTaskTyp MRTaskType
	CompleteTaskId  int
	Ok              bool
}

type GetNewTaskReply struct {
	Valid bool
	Task  MRTask
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
