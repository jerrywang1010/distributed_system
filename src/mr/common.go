package mr

import (
	"log"
)

const DEBUG = true

const (
	mapTask     = 0
	redeuceTask = 1
)

type TASK struct {
	taskType int
	fileName string
	seq      int
}

func debugPrintln(format string, v ...interface{}) {
	if DEBUG {
		log.Printf(format+"\n", v...)
	}
}
