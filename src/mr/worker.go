package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "time"
import "os"
import "sort"


//
// Map functions return a slice of KeyValue.
//
type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	for {
		response := doHeartbeat()
		switch response.jobtype {
		case MapJob:
			doMapTask(mapf, response)
		case ReduceJob:
			doReduceTask(reducef, response)
		case WaitJob:
			time.Sleep(1 * time.Second)
		case CompleteJob:
			return
		default:
			panic(fmt.Sprintf("unexpected jobType %v", response.jobtype))
		}
	}
}

func doHeartbeat() *HeartbeatResponse {
	args := HeartbeatRequest{}
	reply := HeartbeatResponse{}
	call("Coordinator.Heartbeat", &args, &reply)
	return &reply
}

func doMapTask(mapf func(string, string) []KeyValue, response *HeartbeatResponse) {
	content, err := os.ReadFile(response.task.fileName)
	if err != nil {
        log.Fatal(err)
    }
	intermediate := mapf(response.task.fileName, string(content))
	sort.Sort(ByKey(intermediate))

	outnamelist := make([]string, response.nReduce)
	for i := 0; i < response.nReduce; i++ {
		outnamelist[i] = fmt.Sprintf("mr-%v-%v-tmp", response.task.id, i)
	}
	outfiles := make([]*os.File, response.nReduce)
	for i := 0; i < response.nReduce; i++ {
		outfiles[i], err = os.Create(outnamelist[i])
		if err != nil {
			log.Fatal(err)
		}
	}
	for i := 0; i < len(intermediate); i++ {		// write to tmp files
		kv := intermediate[i]
		j := ihash(kv.Key) % response.nReduce
		outfile := outfiles[j]
		if outfile == nil {
			outfile, err = os.Create(outnamelist[j])
			if err != nil {
				log.Fatal(err)
			}
			outfiles[j] = outfile
		}
		fmt.Fprintf(outfile, "%v %v\n", kv.Key, kv.Value)
	}
	for i := 0; i < response.nReduce; i++ {		// close all files & rename
		outfiles[i].Close()
		err := os.Rename(outnamelist[i], fmt.Sprintf("mr-%v-%v", response.task.id, i))
		if err != nil {
			log.Fatal(err)
		}
	}
}

func doReduceTask(reducef func(string, []string) string, response *HeartbeatResponse) {
	
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
