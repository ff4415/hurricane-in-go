package boltExecutor

import (
    "iBolt"
    "executor"
    "taskItem"
)

type BoltExecutor struct {
    Bolt &iBolt.IBolt
    TaskQueue chan taskItem.TaskItem
}

func (se *SpoutExecutor) Start()  {
    go func() {
        select {
        case taskItem := <- se.TaskQueue :
            se.Bolt.Execute(taskItem.GetTuple())
        }
    }()
}
