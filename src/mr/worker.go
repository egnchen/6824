package mr

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
)
import "log"
import "net/rpc"
import "hash/fnv"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string `json:"k"`
	Value string `json:"v"`
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	lastCompletedTyp := TYPE_NONE
	lastCompletedId := INVALID_TASK_ID
	ok := true
	// Your worker implementation here.
	for {
		reply := CallGetNewTask(lastCompletedTyp, lastCompletedId, ok)
		lastCompletedTyp = reply.Task.Typ
		lastCompletedId = reply.Task.CurTaskId
		if reply.Valid {
			if reply.Task.Typ == TYPE_MAP {
				err := doMap(mapf, &reply.Task)
				if err != nil {
					log.Printf("Map task #%v failed: %v", reply.Task.CurTaskId, err)
				}
				ok = err == nil
			} else if reply.Task.Typ == TYPE_REDUCE {
				err := doReduce(reducef, &reply.Task)
				if err != nil {
					log.Printf("Reduce task #%v failed: %v", reply.Task.CurTaskId, err)
				}
				ok = err == nil
			} else {
				panic("invalid task type TYPE_NONE")
			}
		} else {
			break
		}
	}
}

func getIFName(mapId int, reduceId int) string {
	return fmt.Sprintf("mr-%v-%v.txt", mapId, reduceId)
}

func GetOFName(reduceId int) string {
	return fmt.Sprintf("mr-out-%v.txt", reduceId)
}

func doMap(mapf func(string, string) []KeyValue, task *MRTask) error {
	log.Printf("Doing map task #%v-%v", task.CurTaskId, task.Filename)
	content, err := os.ReadFile(task.Filename)
	if err != nil {
		return err
	}
	result := mapf(task.Filename, string(content))
	// distribute it
	resultBucket := make(map[int][]KeyValue)
	for _, kv := range result {
		h := ihash(kv.Key) % task.NReduce
		resultBucket[h] = append(resultBucket[h], kv)
	}
	for i := 0; i < task.NReduce; i++ {
		b, ok := resultBucket[i]
		if ok {
			bin, err := json.Marshal(b)
			if err != nil {
				return err
			}
			err = os.WriteFile(getIFName(task.CurTaskId, i), bin, 0644)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func doReduce(reducef func(string, []string) string, task *MRTask) error {
	log.Printf("Doing reduce task #%v", task.CurTaskId)
	// read all IFs and perform reduce
	all := make(map[string][]string)
	for i := 0; i < task.NMap; i++ {
		bin, err := os.ReadFile(getIFName(i, task.CurTaskId))
		if err != nil {
			return err
		}
		var content []KeyValue
		if err := json.Unmarshal(bin, &content); err != nil {
			return err
		}
		// no need for sorting, use hashmap instead
		for _, kv := range content {
			all[kv.Key] = append(all[kv.Key], kv.Value)
		}
	}
	f, err := os.OpenFile(GetOFName(task.CurTaskId), os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
	defer f.Close()
	w := bufio.NewWriter(f)
	if err != nil {
		return err
	}
	for k, v := range all {
		agg := reducef(k, v)
		_, err := fmt.Fprintf(w, "%v %v\n", k, agg)
		if err != nil {
			return err
		}
	}
	if err := w.Flush(); err != nil {
		return err
	}
	return nil
}

func CallGetNewTask(completeTaskTyp MRTaskType, CompleteTaskId int, ok bool) GetNewTaskReply {
	args := GetNewTaskArgs{
		CompleteTaskTyp: completeTaskTyp,
		CompleteTaskId:  CompleteTaskId,
		Ok:              ok,
	}
	reply := GetNewTaskReply{}
	call("Coordinator.GetNewTask", &args, &reply)
	return reply
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
