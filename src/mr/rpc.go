package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

type AskTaskRequest struct {
	Status int // machine Status
}

type AskTaskReply struct {
	TaskType  int // -1: no Task 0: map task 1: reduce task
	TaskNum   int //
	FileName  string
	ReduceNum int
	MapNum    int
}

type JobDoneMsg struct {
	TaskType int
	TaskNum  int
	Status   int
}

type JobDoneMsgRE struct {
	Status int
}

const (
	MapTask    = 0
	ReduceTask = 1
)

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
