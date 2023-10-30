package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"
)

var (
	toBeMap    = []Task{}
	toBeReduce = []Task{}

	beingMapped  map[int]bool
	beingReduced map[int]bool
)

type Task struct {
	Number, NReduce int
	FileName        string
}

type Coordinator struct {
	// Your definitions here.

}

func popFront(queue *[]Task) Task {
	t := (*queue)[0]
	*queue = (*queue)[1:]
	return t
}

func pushTail(queue *[]Task, t Task) {
	*queue = append(*queue, t)
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
type crash struct {
	startTime time.Time
	args      Task
}

var crashmap = map[int]crash{}
var crashreduce = map[int]crash{}

var maplock = sync.Mutex{}

func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {

	maplock.Lock()
	defer maplock.Unlock()

	for _, c := range crashmap {
		duration := time.Now().Sub(c.startTime)
		// fmt.Println("DURATION TIMES: %lf", duration.Seconds())
		if duration.Seconds() > 10 {
			delete(beingMapped, c.args.Number)
			pushTail(&toBeMap, c.args)
		}
	}

	for _, c := range crashreduce {

		duration := time.Now().Sub(c.startTime)
		// fmt.Println("DURATION TIMES: %lf", duration.Seconds())
		if duration.Seconds() > 10 {
			delete(beingReduced, c.args.Number)
			pushTail(&toBeReduce, c.args)
		}
	}

	// fmt.Println("Replying")
	// fmt.Println(len(toBeMap), len(beingMapped), len(toBeReduce), len(beingReduced))
	fmt.Printf("")
	switch args.LastType {
	case 1:
		delete(beingMapped, args.LastTask.Number)
		delete(crashmap, args.LastTask.Number)
	case 2:
		// fmt.Println("DELETE: %d", args.LastTask.Number)
		delete(beingReduced, args.LastTask.Number)
		delete(crashreduce, args.LastTask.Number)
	}

	switch {
	case len(toBeMap) != 0:

		reply.TaskType = 1
		reply.Args = popFront(&toBeMap)

		beingMapped[reply.Args.Number] = true

		crashmap[reply.Args.Number] = crash{time.Now(), reply.Args}

	case len(beingMapped) != 0:

		reply.TaskType = 0

	case len(toBeReduce) != 0:

		reply.TaskType = 2
		reply.Args = popFront(&toBeReduce)
		// fmt.Printf("ADD: %d\n", reply.Args.Number)
		beingReduced[reply.Args.Number] = true

		crashreduce[reply.Args.Number] = crash{time.Now(), reply.Args}

	case len(beingReduced) != 0:

		reply.TaskType = 0

	default:
		reply.TaskType = -1
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
	maplock.Lock()
	defer maplock.Unlock()

	ret := len(toBeMap) == 0 && len(toBeReduce) == 0
	ret = ret && len(beingMapped) == 0 && len(beingReduced) == 0

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//

func MakeCoordinator(files []string, nReduce int) *Coordinator {

	c := Coordinator{}

	maplock.Lock()
	defer maplock.Unlock()

	beingMapped = make(map[int]bool)
	beingReduced = make(map[int]bool)

	// fmt.Printf("ASSIGN: %d\n", BackupNReduce)

	// Your code here.
	for i, s := range files {
		pushTail(&toBeMap, Task{i, nReduce, s})
	}
	for i := 0; i < nReduce; i++ {
		pushTail(&toBeReduce, Task{i, nReduce, strconv.Itoa(i)})
	}

	c.server()
	return &c
}
