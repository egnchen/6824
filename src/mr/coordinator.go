package mr

import (
	"log"
	"math"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

const TASK_TIMEOUT = 5 * time.Second

type Coordinator struct {
	// Your definitions here.
	mu         sync.Mutex
	tasks      map[int]*MRTask
	availTasks chan *MRTask
	state      MRTaskType
	nMap       int
	nReduce    int
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// Transit coordinator state with mutex held
func (c *Coordinator) transitLocked() {
	switch c.state {
	case TYPE_MAP:
		// populate reduce tasks
		for i := 0; i < c.nReduce; i++ {
			newTask := &MRTask{
				Typ:       TYPE_REDUCE,
				CurTaskId: i,
				NMap:      c.nMap,
				NReduce:   c.nReduce,
			}
			c.availTasks <- newTask
		}
		c.state = TYPE_REDUCE
	case TYPE_REDUCE:
		// done
		c.state = TYPE_NONE
	}
}

func (c *Coordinator) GetNewTask(args *GetNewTaskArgs, reply *GetNewTaskReply) error {
	var newTask *MRTask = nil
	reply.Valid = true
	c.mu.Lock()
	if args.CompleteTaskTyp != TYPE_NONE {
		//log.Printf("Receiving complete type %v id %v", args.CompleteTaskTyp, args.CompleteTaskId)
		task, ok := c.tasks[args.CompleteTaskId]
		if ok {
			delete(c.tasks, args.CompleteTaskId)
			//log.Printf("tasks removed #%v", args.CompleteTaskId)
			if !args.Ok {
				// re-schedule this immediately
				log.Printf("Rescheduling task type %v id %v", task.Typ, task.CurTaskId)
				c.availTasks <- task
			}
		} else {
			// this means ack is received after timeout, ignore it
			log.Printf("Warning: task #%v not found, ignored", args.CompleteTaskId)
		}
	}
	// TODO optimize this with heap
	for i, t := range c.tasks {
		if t.LastExecuteTime.Add(TASK_TIMEOUT).Before(time.Now()) {
			log.Printf("#%v last execute time %v now %v", t.CurTaskId, t.LastExecuteTime, time.Now())
			c.availTasks <- t
			delete(c.tasks, i)
			log.Printf("tasks removed (timeout)#%v", i)
			break
		}
	}
	if len(c.tasks)+len(c.availTasks) == 0 {
		c.transitLocked()
	}
	if c.state == TYPE_NONE {
		reply.Valid = false
	}
	c.mu.Unlock()

	// non-critical, possibly blocking
	if reply.Valid && newTask == nil {
		newTask, reply.Valid = <-c.availTasks
	}
	if reply.Valid {
		//log.Printf("distributing task typ %v id %v", newTask.Typ, newTask.CurTaskId)
		newTask.LastExecuteTime = time.Now()
		c.mu.Lock()
		c.tasks[newTask.CurTaskId] = newTask
		//log.Printf("Added id %v to tasks", newTask.CurTaskId)
		c.mu.Unlock()
		reply.Task = *newTask
	}
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.state == TYPE_NONE {
		log.Printf("Coordinator exit")
	}
	return c.state == TYPE_NONE
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// NReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		tasks:      make(map[int]*MRTask),
		availTasks: make(chan *MRTask, int(math.Max(float64(len(files)), float64(nReduce)))),
		state:      TYPE_MAP,
		nMap:       len(files),
		nReduce:    nReduce,
	}

	// Your code here.
	// initialize the coordinator with map tasks
	for i, f := range files {
		newTask := &MRTask{
			Typ:       TYPE_MAP,
			CurTaskId: i,
			NMap:      c.nMap,
			NReduce:   c.nReduce,
			Filename:  f,
		}
		c.availTasks <- newTask
	}
	c.server()
	return &c
}
