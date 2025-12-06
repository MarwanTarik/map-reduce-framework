package mr

import (
	"log"
	"os"
	"strconv"
)

type TaskType int

const (
	Map TaskType = iota
	Reduce
	Wait
	Exit
)

type TaskDoneArgs struct {
	TaskID   int
	TaskType TaskType
}

type TaskDoneReply struct{}

type TaskArgs struct {
	WorkerID int
}

type TaskReply struct {
	TaskType TaskType
	TaskID   int
	Filename string
	NReduce  int
	NMap     int
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())

	log := log.Default()
	log.Output(2, s)
	return s
}
