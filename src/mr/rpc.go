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

type MrArgs struct {
	TaskType int //1 map ,2 reduce
	TaskName string
	WorkerId int//没有就分配
	TaskId int //reduce id
}

type MrReply struct {
	TaskName string //map file name
	NReduce int
	WorkerId int
	TaskId int //reduce id
	TaskType int 
	MapId int
}

// Add your RPC definitions here.


// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
