package taskItem

import (
  "github.com/ff4415/hurricane-in-go/tuple"

)

type TaskItem struct {
    taskIndex int
    tuple *tuple.Tuple
}

func (ti *TaskItem) GetTaskIndex() int  {
    return ti.taskIndex
}

func (ti *TaskItem) GetTuple() *tuple.Tuple  {
    return ti.tuple
}

func NewTaskItem(taskIndex int, t *tuple.Tuple) *TaskItem  {
    return &TaskItem {
        taskIndex: taskIndex,
        tuple: t,
    }
}
