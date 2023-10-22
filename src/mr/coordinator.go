package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"time"
)

type Coordinator struct {
	files   []string
	nReduce int
	nMap    int
	// phase   SchedulePhase
	tasks            []Task
	finMapTaskNum    int
	finReduceTaskNum int

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
			taskNum := len(c.tasks)
			for i := 0; i < taskNum; i++ {
				if c.tasks[i].Status == assigned {
					if time.Now().Sub(c.tasks[i].StartTime) > 10*time.Second {
						c.tasks[i].Status = unassigned
					}
				}
			}
			if c.finMapTaskNum < c.nMap { // assign map task
				assign := false
				for i := 0; i < taskNum; i++ {
					if c.tasks[i].Status == unassigned {
						c.tasks[i].Status = assigned
						c.tasks[i].StartTime = time.Now()
						msg.response.Jobtype = MapJob
						msg.response.Task = c.tasks[i]
						msg.response.NumReduce = c.nReduce
						assign = true
						break
					}
				}
				if !assign {
					msg.response.Jobtype = WaitJob
				}
				msg.ok <- struct{}{}
			} else if c.finReduceTaskNum < c.nReduce { // assign reduce task
				assign := false
				for i := 0; i < taskNum; i++ {
					if c.tasks[i].Status == unassigned {
						c.tasks[i].Status = assigned
						c.tasks[i].StartTime = time.Now()
						msg.response.Jobtype = ReduceJob
						msg.response.Task = c.tasks[i]
						msg.response.NumMap = c.nMap
						assign = true
						break
					}
				}
				if !assign {
					msg.response.Jobtype = WaitJob
				}
				msg.ok <- struct{}{}
			} else {
				msg.response.Jobtype = CompleteJob
				msg.ok <- struct{}{}
			}
		case msg := <-c.reportCh:
			if msg.request.Jobtype == MapJob && c.tasks[msg.request.Id].Status == assigned {
				c.tasks[msg.request.Id].Status = finished
				c.finMapTaskNum++
				if c.finMapTaskNum == c.nMap {
					c.initReducePhase()
				}
			} else if msg.request.Jobtype == ReduceJob && c.tasks[msg.request.Id].Status == assigned {
				c.tasks[msg.request.Id].Status = finished
				c.finReduceTaskNum++
				if c.finReduceTaskNum == c.nReduce {
					c.doneCh <- struct{}{}
				}
			}
			msg.ok <- struct{}{}
		}
	}
}

func Now() {
	panic("unimplemented")
}

func (c *Coordinator) initMapPhase() {
	c.finMapTaskNum = 0
	c.tasks = make([]Task, c.nMap)
	for i := 0; i < c.nMap; i++ {
		c.tasks[i].FileName = c.files[i]
		c.tasks[i].Id = i
		c.tasks[i].Status = unassigned
	}
}

func (c *Coordinator) initReducePhase() {
	c.finReduceTaskNum = 0
	c.tasks = make([]Task, c.nReduce)
	for i := 0; i < c.nReduce; i++ {
		c.tasks[i].Id = i
		c.tasks[i].Status = unassigned
	}
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
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

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := true

	// Your code here.
	<-c.doneCh
	// fmt.Printf("finish all tasks\n")

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.files = files
	c.nMap = len(files)
	c.nReduce = nReduce
	c.heartbeatCh = make(chan HeartbeatMsg, 1)
	c.reportCh = make(chan ReportMsg, 1)
	c.doneCh = make(chan struct{})
	go c.schedule()

	c.server()
	return &c
}
