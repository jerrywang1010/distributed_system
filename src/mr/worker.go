package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
)

//
// Map functions return a slice of KeyValue.
//
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
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	w := WORKER{mapf: mapf, redecef: reducef}
	// talk to master to get an ID
	w.register()
	for {
		t := w.askForTask()
		w.executeTask(t)
	}
}

type WORKER struct {
	id        int
	mapf      func(string, string) []KeyValue
	redecef   func(string, []string) string
	numMap    int
	numReduce int
}

func (w *WORKER) doMapTask(t Task) {
	file, err := os.Open(t.FileName)
	defer file.Close()
	if err != nil {
		log.Fatalf("unable to open file=%v", t.FileName)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read file=%v", t.FileName)
	}
	kva := w.mapf(t.FileName, string(content))

	reduceTasks := make([][]KeyValue, w.numReduce)

	for _, kv := range kva {
		reduceTaskIndex := ihash(kv.Key) % w.numReduce
		// add the kv pair to the cooresponding reduce task
		reduceTasks[reduceTaskIndex] = append(reduceTasks[reduceTaskIndex], kv)
	}

	// create intermediate file for each map task output
	for reduceTaskIndex, kva := range reduceTasks {
		// A reasonable naming convention for intermediate files is mr-X-Y,
		// where X is the Map task number, and Y is the reduce task number.
		intermediateFileName := fmt.Sprintf("mr-%v-%v", t.Id, reduceTaskIndex)
		intermediateFile, err := os.Create(intermediateFileName)
		if err != nil {
			log.Fatalf("can not create intermediate file=%v\n", intermediateFileName)
		}
		enc := json.NewEncoder(intermediateFile)
		for _, kv := range kva {
			err := enc.Encode(&kv)
			if err != nil {
				log.Fatalf("can not encode kva=%v for file=%v", kv, intermediateFileName)
			}
		}
	}
	w.done(t)
}

func (w *WORKER) doReduceTask(t Task) {
	// aggregate across all map results
	kvMap := make(map[string][]string)
	for i := 0; i < w.numMap; i++ {
		intermediateFileName := fmt.Sprintf("mr-%v-%v", i, t.Id)
		intermediateFile, err := os.Open(intermediateFileName)
		defer intermediateFile.Close()
		if err != nil {
			// if the intermediate file does not exist, it's ok
			continue
		}
		dec := json.NewDecoder(intermediateFile)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kvMap[kv.Key] = append(kvMap[kv.Key], kv.Value)
		}
	}
	for k, v := range kvMap {
		// output in files named mr-out-X, one for each reduce task
		outputFileName := fmt.Sprintf("mr-out-%d", t.Id)
		file, err := os.OpenFile(outputFileName, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
		if err != nil {
			log.Fatalf("error when openning output file=%v, err=%v\n", outputFileName, err)
		}

		content := fmt.Sprintf("%v %v\n", k, w.redecef(k, v))
		bytes := []byte(content)
		_, err = file.Write(bytes)
		if err != nil {
			log.Fatalf("can not write to output file=%v, err=%v", outputFileName, err)
		}
		file.Close()
	}
	w.done(t)
}

func (w *WORKER) executeTask(t Task) {
	if t.Phase == inMap {
		w.doMapTask(t)
	} else {
		w.doReduceTask(t)
	}
}

func (w *WORKER) askForTask() Task {
	args := AskForTaskArgs{}
	reply := AskForTaskReply{}
	if w.id < 0 {
		panic("invalid worker.id\n")
	}
	args.WorkerId = w.id
	success := call("Master.AssignATask", &args, &reply)
	if !success {
		log.Fatal("Unable to ask master for a task\n")
	}
	if reply.Task.Phase == inMap {
		debugPrintln("worker=%v got map task from master, fileName=%v, taskID=%v", w.id, reply.Task.FileName, reply.Task.Id)
	} else {
		debugPrintln("worker=%v got reduce task from master, taskID=%v", w.id, reply.Task.Id)
	}
	return reply.Task
}

func (w *WORKER) done(t Task) {
	args := DoneTaskArgs{}
	reply := DoneTaskReply{}
	args.TaskId = t.Id
	args.WorkerId = w.id
	success := call("Master.DoneTask", &args, &reply)
	if !success {
		log.Fatalf("unable to send rpc of DoneTask to master form worker=%v\n", w.id)
	}

	if t.Phase == inMap {
		debugPrintln("worker=%v has done map task, reply=%v\n", w.id, reply.Reply)
	} else {
		debugPrintln("worker=%v has done reduce task, reply=%v\n", w.id, reply.Reply)
	}
}

func (w *WORKER) register() {
	args := RegisterWorkerArgs{}
	reply := RegisterWorkerReply{}
	args.S = "register"
	success := call("Master.RegisterWorker", &args, &reply)
	if !success {
		log.Fatal("Unable to register a worker\n")
	}
	w.id = reply.WorkerID
	w.numMap = reply.NumMap
	w.numReduce = reply.NumReduce
	debugPrintln("registering a new worker, id=%v, numMap=%v, numReduce=%v\n",
		reply.WorkerID, reply.NumMap, reply.NumReduce)
}

//
// example function to show how to make an RPC call to the master.
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
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
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
