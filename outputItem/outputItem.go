package outputItem

import (
     "github.com/ff4415/hurricane-in-go/tuple"
     // "github.com/ff4415/hurricane-in-go/taskItem"
)

type OutputItem struct {
    taskIndex int
    tuple *tuple.Tuple
}

func (oi *OutputItem) GetTaskIndex() int  {
    return oi.taskIndex
}

func (oi *OutputItem) GetTuple() *tuple.Tuple  {
    return oi.tuple
}

func NewOutputItem(taskIndex int, t *tuple.Tuple, taskName string) *OutputItem  {
    ti := &OutputItem {
        taskIndex: taskIndex,
        tuple: t,
    }
    ti.tuple.SourceTask = taskName
    return ti
}
