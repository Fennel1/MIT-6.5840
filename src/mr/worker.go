package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "time"
import "os"
import "sort"
import "bufio"


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
		// fmt.Printf("do map task %s\n", response.Task.FileName)
		switch response.Jobtype {
		case MapJob:
			doMapTask(mapf, response)
		case ReduceJob:
			doReduceTask(reducef, response)
		case WaitJob:
			time.Sleep(1 * time.Second)
		case CompleteJob:
			return
		default:
			panic(fmt.Sprintf("unexpected jobType %v", response.Jobtype))
		}
	}
}

func doHeartbeat() *HeartbeatResponse {
	request := HeartbeatRequest{}
	response := HeartbeatResponse{}
	call("Coordinator.Heartbeat", &request, &response)
	return &response
}

func doMapTask(mapf func(string, string) []KeyValue, response *HeartbeatResponse) {
	// fmt.Printf("do map task %v do file %s\n", response.Task.Id, response.Task.FileName)
	content, err := os.ReadFile(response.Task.FileName)
	if err != nil {
		fmt.Printf("doMapTask %v: cannot open %v\n", response.Task.Id, response.Task.FileName)
        log.Fatal(err)
    }
	intermediate := mapf(response.Task.FileName, string(content))
	sort.Sort(ByKey(intermediate))

	outfiles := make([]*os.File, response.NumReduce)
	for i := 0; i < response.NumReduce; i++ {
		outfiles[i], err = os.Create(fmt.Sprintf("mr-%v-%v-tmp", response.Task.Id, i))
		if err != nil {
			log.Fatal(err)
		}
	}
	for i := 0; i < len(intermediate); i++ {		// write to tmp files
		kv := intermediate[i]
		j := ihash(kv.Key) % response.NumReduce
		fmt.Fprintf(outfiles[j], "%v %v\n", kv.Key, kv.Value)
	}
	for i := 0; i < response.NumReduce; i++ {		// close all files & rename
		outfiles[i].Close()
		err := os.Rename(fmt.Sprintf("mr-%v-%v-tmp", response.Task.Id, i), fmt.Sprintf("mr-%v-%v", response.Task.Id, i))
		if err != nil {
			log.Fatal(err)
		}
	}

	request := ReportRequest{}
	reply := ReportResponse{}
	request.Jobtype = MapJob
	request.Id = response.Task.Id
	call("Coordinator.Report", &request, &reply)
}

func doReduceTask(reducef func(string, []string) string, response *HeartbeatResponse) {
	intermediate := make(map[string][]string)
	for i := 0; i < response.NumMap; i++ {
		file, err := os.Open(fmt.Sprintf("mr-%v-%v", i, response.Task.Id))
		if err != nil {
			fmt.Printf("doReduceTask %v: cannot open %v\n", response.Task.Id, fmt.Sprintf("mr-%v-%v", i, response.Task.Id))
			log.Fatal(err)
		}
		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			line := scanner.Text()
			var key, value string
			_, err := fmt.Sscanf(line, "%s %s", &key, &value)
			if err != nil {
				log.Fatal(err)
			} else {
				intermediate[key] = append(intermediate[key], value)
			}
		}
		file.Close()
	}

	outfile, err := os.Create(fmt.Sprintf("mr-out-%v-tmp", response.Task.Id))
	if err != nil {
		log.Fatal(err)
	}
	for key, values := range intermediate {
		output := reducef(key, values)
		fmt.Fprintf(outfile, "%v %v\n", key, output)
	}
	outfile.Close()
	err = os.Rename(fmt.Sprintf("mr-out-%v-tmp", response.Task.Id), fmt.Sprintf("mr-out-%v", response.Task.Id))
	if err != nil {
		log.Fatal(err)
	}

	request := ReportRequest{}
	reply := ReportResponse{}
	request.Jobtype = ReduceJob
	request.Id = response.Task.Id
	call("Coordinator.Report", &request, &reply)
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
