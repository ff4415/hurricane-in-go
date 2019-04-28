package boltExecutor

import (
    "github.com/ff4415/hurricane-in-go/iBolt"
    // "github.com/ff4415/hurricane-in-go/executor"
    "github.com/ff4415/hurricane-in-go/taskItem"
)

type BoltExecutor struct {
    Bolt *iBolt.IBolt
    TaskQueue chan *taskItem.TaskItem
}

func (be *BoltExecutor) Start()  {
    go func() {
        select {
        case taskItem := <- be.TaskQueue :
            (*be.Bolt).Execute(taskItem.GetTuple())
        }
    }()
}
