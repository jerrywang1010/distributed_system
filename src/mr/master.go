package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

const (
	timeout                = time.Second * 10
	updateProgressInterval = time.Millisecond * 500
)

type Master struct {
	// Your definitions here.
	files          []string
	nReduce        int
	nextWorkerId   int
	mtx            sync.Mutex
	allDone        bool
	phase          int
	allTasksStates []TaskState
	taskCh         chan Task
	done           bool
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (m *Master) RegisterWorker(args *RegisterWorkerArgs, reply *RegisterWorkerReply) error {
	m.mtx.Lock()
	defer m.mtx.Unlock()
	reply.WorkerID = m.nextWorkerId
	m.nextWorkerId += 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	m.mtx.Lock()
	defer m.mtx.Unlock()
	ret := m.allDone
	return ret
}

// intialize all tasks to be map, called at the beggining
// when this is called, there is only 1 master thread running
func (m *Master) initMap() {
	debugPrintln("initializing master, marking all map task as not started\n")
	m.allTasksStates = make([]TaskState, len(m.files))
	m.phase = inMap
	m.taskCh = make(chan Task, len(m.files))
	for idx, _ := range m.allTasksStates {
		m.allTasksStates[idx].state = new
		m.allTasksStates[idx].workerId = -1
	}
}

// initialize all task to be reduce, called after all map tasks are done
// when this is called, master's lock is already acquired
func (m *Master) initReduce() {
	debugPrintln("initializing reduce tasks\n")
	m.allTasksStates = make([]TaskState, m.nReduce)
	m.phase = inRedeuce
	m.taskCh = make(chan Task, m.nReduce)
	for idx, _ := range m.allTasksStates {
		m.allTasksStates[idx].state = new
		m.allTasksStates[idx].workerId = -1
	}
}

func (m *Master) AssignATask(args *AskForTaskArgs, reply *AskForTaskReply) error {
	task := <-m.taskCh
	m.mtx.Lock()
	defer m.mtx.Unlock()
	m.allTasksStates[task.id].startTime = time.Now()
	m.allTasksStates[task.id].state = running
	m.allTasksStates[task.id].workerId = args.WorkerId
	if m.phase == inMap {
		reply.MapOrReduce = inMap
		debugPrintln("master received rpc from worker=%v, assigning a map task, filename=%v\n", args.WorkerId, task.fileName)
	} else {
		reply.MapOrReduce = inRedeuce
		debugPrintln("master received rpc from worker=%v, assigning a reduce task", args.WorkerId)
	}
	reply.Task = task

	return nil
}

// check for newly finished tasks, timed out tasks, and update Done/Phase accordingly
func (m *Master) updateTaskProgress() {
	m.mtx.Lock()
	defer m.mtx.Unlock()
	allTaskDone := true
	for idx, taskState := range m.allTasksStates {
		switch taskState.state {
		case new:
			// add to channel, change state
			// build a new task and push it into the channel to make it visible for workers
			// reduce don't need a filename
			var task Task
			if m.phase == inMap {
				task.fileName = m.files[idx]
			} else {
				task.fileName = "reduce"
			}
			task.id = idx
			m.taskCh <- task
			m.allTasksStates[idx].state = inQueue
			allTaskDone = false
		case inQueue:
			allTaskDone = false
			// do nothing, still waiting to be assgined to a worker
		case running:
			allTaskDone = false
			// check timeout
			if time.Now().Sub(taskState.startTime) >= timeout {
				// add the task back to channel and mark it as in queue
				debugPrintln("task_id=%v has timed out, original worker_id=%v\n", idx, taskState.workerId)
				m.allTasksStates[idx].workerId = -1
				m.allTasksStates[idx].state = inQueue
				// task's filename and id remains unchanged
				var task Task
				if m.phase == inMap {
					task.fileName = m.files[idx]
				} else {
					task.fileName = "reduce"
				}
				task.id = idx
				m.taskCh <- task
			}

		case done:
			// do nothing
		default:
			log.Fatal("unknow task status")
		}
	}
	if allTaskDone {
		if m.phase == inMap {
			m.initReduce()
		} else {
			m.done = true
		}
	}
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}
	// Your code here.
	m.mtx = sync.Mutex{}
	m.nReduce = nReduce
	m.files = files
	m.nextWorkerId = 0
	m.allDone = false
	m.initMap()

	m.server()
	debugPrintln("master is up\n")
	return &m
}
