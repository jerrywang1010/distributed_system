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

// global variable
// they should only be modified by master
// nReduce should be initialized and never change
// phase will be initialized and updated once only by acquire a lock of master
type Master struct {
	// Your definitions here.
	files          []string
	nextWorkerId   int
	mtx            sync.Mutex
	allDone        bool
	allTasksStates []TaskState
	taskCh         chan Task
	done           bool
	phase          int
	numReduce      int
	numMap         int
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
	reply.NumMap = m.numMap
	reply.NumReduce = m.numReduce
	debugPrintln("registering a new worker, workerID=%v, numMap=%v, numReduce=%v\n",
		m.nextWorkerId, m.numMap, m.numReduce)
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

// when a worker has finished a task, map or reduce, will send this rpc to master
func (m *Master) DoneTask(args *DoneTaskArgs, reply *DoneTaskReply) error {
	// need to modify the stata of tasks, so first lock
	m.mtx.Lock()
	defer m.mtx.Unlock()
	debugPrintln("received DoneTask RPC from worker=%v, for task=%v\n", args.WorkerId, args.TaskId)
	reply.Reply = "Thank you!"
	// some checks
	if m.allTasksStates[args.TaskId].state != running {
		log.Fatal("task is not running\n")
	}
	if m.allTasksStates[args.TaskId].workerId != args.WorkerId {
		log.Fatalf("task belong to other worker=%v\n", m.allTasksStates[args.TaskId].workerId)
	}

	m.allTasksStates[args.TaskId].state = done
	m.allTasksStates[args.TaskId].workerId = -1
	return nil
}

// intialize all tasks to be map, called at the beggining
// when this is called, there is only 1 master thread running
func (m *Master) initMap() {
	m.mtx.Lock()
	defer m.mtx.Unlock()
	debugPrintln("initializing master, marking all map task as not started\n")
	m.allTasksStates = make([]TaskState, len(m.files))
	m.phase = inMap
	if len(m.files) >= m.numReduce {
		m.taskCh = make(chan Task, len(m.files))
	} else {
		m.taskCh = make(chan Task, m.numReduce)
	}
	m.numMap = len(m.files)
	for idx, _ := range m.allTasksStates {
		m.allTasksStates[idx].state = new
		m.allTasksStates[idx].workerId = -1
	}
}

// initialize all task to be reduce, called after all map tasks are done
// when this is called, master's lock is already acquired
func (m *Master) initReduce() {
	debugPrintln("initializing reduce tasks\n")
	if m.numReduce <= 0 {
		log.Fatal("invalid nReduce number\n")
	}
	m.allTasksStates = make([]TaskState, m.numReduce)
	m.phase = inRedeuce
	for idx := range m.allTasksStates {
		m.allTasksStates[idx].state = new
		m.allTasksStates[idx].workerId = -1
	}
}

func (m *Master) AssignATask(args *AskForTaskArgs, reply *AskForTaskReply) error {
	debugPrintln("master received request task rpc from worker=%v", args.WorkerId)
	task := <-m.taskCh
	m.mtx.Lock()
	defer m.mtx.Unlock()
	m.allTasksStates[task.Id].startTime = time.Now()
	m.allTasksStates[task.Id].state = running
	m.allTasksStates[task.Id].workerId = args.WorkerId
	if m.phase == inMap {
		debugPrintln("assigning a map task, taskId=%v, filename=%v\n",
			task.Id, task.FileName)
	} else {
		debugPrintln("assigning a reduce task, taskId=%v",
			task.Id)
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
				task.FileName = m.files[idx]
				task.Phase = inMap
			} else {
				task.FileName = "reduce"
				task.Phase = inRedeuce
			}
			task.Id = idx
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
				debugPrintln("task_id=%v has timed out, worker_id=%v\n", idx, taskState.workerId)
				m.allTasksStates[idx].workerId = -1
				m.allTasksStates[idx].state = inQueue
				// task's filename and id remains unchanged
				var task Task
				if m.phase == inMap {
					task.FileName = m.files[idx]
					task.Phase = inMap
				} else {
					task.FileName = "reduce"
					task.Phase = inRedeuce
				}
				task.Id = idx
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

func (m *Master) sleepAndUpdate(sleepTime time.Duration) {
	for !m.Done() {
		go m.updateTaskProgress()
		time.Sleep(sleepTime)
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
	m.numReduce = nReduce
	m.files = files
	m.nextWorkerId = 0
	m.allDone = false
	m.initMap()
	go m.sleepAndUpdate(updateProgressInterval)
	m.server()
	debugPrintln("master is up\n")
	return &m
}
