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
	finMapTaskNum 		int
	finReduceTaskNum 	int

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
			if c.finMapTaskNum < c.nMap {	// assign map task
				taskNum := len(c.tasks)
				for i := 0; i < taskNum; i++ {
					task := c.tasks[i]
					if task.status == unassigned {
						task.status = assigned
						msg.response.jobtype = MapJob
						msg.response.task = task
						msg.response.nReduce = c.nReduce
						break
					}
				}
				msg.ok <- struct{}{}
			} else {						// assign reduce task
				taskNum := len(c.tasks)
				for i := 0; i < taskNum; i++ {
					task := c.tasks[i]
					if task.status == unassigned {
						task.status = assigned
						msg.response.jobtype = ReduceJob
						msg.response.task = task
						msg.response.nMap = c.nMap
						break
					}
				}
				msg.ok <- struct{}{}
			}
		case msg := <-c.reportCh:
			if msg.request.jobtype == MapJob {
				c.tasks[msg.request.id].status = finished
				c.finMapTaskNum++
				if c.finMapTaskNum == c.nMap {
					c.initReducePhase()
				}
			} else if msg.request.jobtype == ReduceJob {
				c.tasks[msg.request.id].status = finished
				c.finReduceTaskNum++
				if c.finReduceTaskNum == c.nReduce {
					c.doneCh <- struct{}{}
				}
			}
			msg.ok <- struct{}{}
		}
	}
}

func (c *Coordinator) initMapPhase() {
	c.tasks = make([]Task, c.nMap)
	for i := 0; i < c.nMap; i++ {
		c.tasks[i].fileName = c.files[i]
		c.tasks[i].id = i
		c.tasks[i].status = unassigned
	}
}

func (c *Coordinator) initReducePhase() {
	c.tasks = make([]Task, c.nReduce)
	for i := 0; i < c.nReduce; i++ {
		c.tasks[i].id = i
		c.tasks[i].status = unassigned
	}
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
	ret := true

	// Your code here.
	<- c.doneCh

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
	c.files = files
	c.nMap = len(files)
	c.nReduce = nReduce
	c.heartbeatCh = make(chan HeartbeatMsg)
	c.reportCh = make(chan ReportMsg)
	c.doneCh = make(chan struct{})
	go c.schedule()

	c.server()
	return &c
}
