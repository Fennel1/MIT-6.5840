package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"
import "time"

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

// Add your RPC definitions here.
type TaskStatus int

const (
	unassigned 	TaskStatus = 0
	assigned 	TaskStatus = 1
	timeout 	TaskStatus = 2
	finished 	TaskStatus = 3
)


type Task struct {
	fileName  string
	id        int
	startTime time.Time
	status    TaskStatus
}

type JobType int

const (
	MapJob 		JobType = 0
	ReduceJob 	JobType = 1
	WaitJob 	JobType = 2
	CompleteJob JobType = 3
)

type HeartbeatRequest struct {
}

type HeartbeatResponse struct{
	jobtype JobType
	task 	Task
	nReduce int
	nMap 	int
}

type HeartbeatMsg struct {
	response *HeartbeatResponse
	ok       chan struct{}
}

type ReportRequest struct {
	jobtype JobType
	id		int
}

type ReportResponse struct {

}

type ReportMsg struct {
	request *ReportRequest
	ok      chan struct{}
}


// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
