package mr

import (
	"log"
	"time"
)

const DEBUG = true

// the overall progress of map reduce
const (
	inMap     = 0
	inRedeuce = 1
)

// the states of individual task
const (
	// newly created task, not visible to workers yet
	new = 0
	// visible to worker, but haven't been assigned yet
	inQueue = 1
	// assigned to worker, haven't finished
	running = 2
	// finished
	done = 3
)

type TaskState struct {
	state     int
	workerId  int
	startTime time.Time
}

type Task struct {
	fileName string
	// index into the master.allTaskStates[], maxID = len(files) - 1 in map, maxID = nReduce - 1 in reduce
	id int
}

func debugPrintln(format string, v ...interface{}) {
	if DEBUG {
		log.Printf(format+"\n", v...)
	}
}
