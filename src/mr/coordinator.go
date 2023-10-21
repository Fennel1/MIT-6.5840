package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"


type Coordinator struct {
	files   []string
	nReduce int
	nMap    int
	// phase   SchedulePhase
	tasks   []Task

	heartbeatCh chan HeartbeatMsg
	reportCh    chan ReportMsg
	doneCh      chan struct{}
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) Heartbeat(request *HeartbeatRequest, response *HeartbeatResponse) error {
	msg := HeartbeatMsg{response, make(chan struct{})}
	c.heartbeatCh <- msg
	<-msg.ok
	return nil
}

func (c *Coordinator) Report(request *ReportRequest, response *ReportResponse) error {
	msg := ReportMsg{request, make(chan struct{})}
	c.reportCh <- msg
	<-msg.ok
	return nil
}

func (c *Coordinator) schedule() {
	c.initMapPhase()
	for {
		select {
		case msg := <-c.heartbeatCh:
            // TODO
			msg.ok <- struct{}{}
		case msg := <-c.reportCh:
            // TODO
			msg.ok <- struct{}{}
		}
	}
}

func (c *Coordinator) initMapPhase() {

}


//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
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
	ret := false

	// Your code here.


	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.


	c.server()
	return &c
}
