package taskItem

import "tuple"

type TaskItem struct {
    taskIndex int
    tuple tuple.Tuple
}

func (ti *TaskItem) GetTaskIndex() int  {
    return ti.taskItem
}

func (ti *TaskItem) GetTuple() tuple.Tuple  {
    return ti.Tuple
}

func NewTaskItem(taskIndex int, tuple tuple.Tuple) *TaskItem  {
    return &OutputItem {
        taskIndex: taskIndex,
        tuple: tuple
    }
}
