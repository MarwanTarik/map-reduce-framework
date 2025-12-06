package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type TaskState int

const (
	Idle TaskState = iota
	Done
	InProgress
)

type Task struct {
	ID        int
	State     TaskState
	StartTime time.Time
	EndTime   time.Time
	Filename  string
}

type Coordinator struct {
	mu sync.Mutex

	mapTasks    []Task
	reduceTasks []Task

	nReduce int
	nMap    int

	isMapDone    bool
	isReduceDone bool
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

func (c *Coordinator) GetTask(args *TaskArgs, reply *TaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	go c.checkTimeouts()

	allMapDone := true
	for i := range c.mapTasks {
		if c.mapTasks[i].State == Idle {
			c.mapTasks[i].StartTime = time.Now()
			c.mapTasks[i].State = InProgress

			reply.Filename = c.mapTasks[i].Filename
			reply.TaskID = c.mapTasks[i].ID
			reply.TaskType = Map
			reply.NReduce = c.nReduce
			reply.NMap = c.nMap

			return nil
		}
		if c.mapTasks[i].State != Done {
			allMapDone = false
		}
	}

	c.isMapDone = allMapDone

	if !c.isMapDone {
		reply.TaskType = Wait
		return nil
	}

	allReduceDone := true
	for i := range c.reduceTasks {
		if c.reduceTasks[i].State == Idle {
			c.reduceTasks[i].StartTime = time.Now()
			c.reduceTasks[i].State = InProgress

			reply.TaskID = c.reduceTasks[i].ID
			reply.TaskType = Reduce
			reply.NReduce = c.nReduce
			reply.NMap = c.nMap

			return nil
		}
		if c.reduceTasks[i].State != Done {
			allReduceDone = false
		}
	}

	c.isReduceDone = allReduceDone

	if !c.isReduceDone {
		reply.TaskType = Wait
	} else {
		reply.TaskType = Exit
	}

	return nil
}

func (c *Coordinator) TaskDone(args *TaskDoneArgs, reply *TaskDoneReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if args.TaskType != Map && args.TaskType != Reduce {
		log.Fatal("rpc interface error: ", args.TaskType, "not exists")
		return nil
	}

	if args.TaskType == Map {
		if args.TaskID < 0 || args.TaskID > len(c.mapTasks) {
			log.Fatal("invalid task ID: ", args.TaskID)
			return nil
		}

		c.mapTasks[args.TaskID].State = Done
		c.mapTasks[args.TaskID].EndTime = time.Now()
	} else {
		if args.TaskID < 0 || args.TaskID > len(c.reduceTasks) {
			log.Fatal("invalid task ID: ", args.TaskID)
			return nil
		}

		c.reduceTasks[args.TaskID].State = Done
		c.reduceTasks[args.TaskID].EndTime = time.Now()
	}

	return nil
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	return c.isMapDone && c.isReduceDone
}

func (c *Coordinator) checkTimeouts() {
	timeout := 10 * time.Second
	now := time.Now()

	for i := range c.mapTasks {
		if c.mapTasks[i].State == InProgress {
			if now.Sub(c.mapTasks[i].StartTime) > timeout {
				c.mapTasks[i].State = Idle
			}
		}
	}

	for i := range c.reduceTasks {
		if c.reduceTasks[i].State == InProgress {
			if now.Sub(c.reduceTasks[i].StartTime) > timeout {
				c.reduceTasks[i].State = Idle
			}
		}
	}
}

// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	c.mu.Lock()
	defer c.mu.Unlock()

	c.mapTasks = make([]Task, len(files))
	c.nMap = len(c.mapTasks)

	for i, filename := range files {
		c.mapTasks[i] = Task{
			ID:       i,
			State:    Idle,
			Filename: filename,
		}
	}

	c.nReduce = nReduce
	c.reduceTasks = make([]Task, nReduce)
	for i := 0; i < nReduce; i++ {
		c.reduceTasks[i] = Task{
			ID:       i,
			State:    Idle,
			Filename: "",
		}
	}

	c.server()
	return &c
}
