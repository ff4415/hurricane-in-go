package outputItem

import (
     "tuple"
     "taskInfo"
)

type OutputItem struct {
    taskIndex int
    tuple tuple.Tuple
}

func (oi *OutputItem) GetTaskIndex() int  {
    return oi.taskItem
}

func (oi *OutputItem) GetTuple() tuple.Tuple  {
    return oi.Tuple
}

func NewOutputItem(taskIndex int, tuple tuple.Tuple, taskName string) *OutputItem  {
    ti := &TaskItem {
        taskIndex: taskIndex,
        tuple: tuple
    }
    ti.tuple.SourceTask = taskName
    return ti
}
