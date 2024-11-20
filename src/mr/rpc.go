package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

type TaskType int

const (
	MAP_TASK TaskType = iota
	REDUCE_TASK
	EXIT_TASK
	WAIT_TASK
)

type RpcGetTaskRequest struct {
}

type RpcGetTaskResponse struct {
	Task Task
}

type Task struct {
	TaskId    int
	FilePaths []string
	NReducer  int
	TaskType  TaskType
}

type RpcTaskDoneRequest struct {
	TaskId    int
	FilePaths map[int]string
	TaskType  TaskType
}

type RpcTaskDoneResponse struct {
}

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
