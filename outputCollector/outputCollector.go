package outputCollector

import (
    "github.com/ff4415/hurricane-in-go/outputItem"
    "github.com/ff4415/hurricane-in-go/tuple"
)

const itemCount = 10

type OutputCollector struct {
    taskIndex int
    taskName string
    Queue chan *outputItem.OutputItem
}

func NewOutputCollector(taskIndex int, taskName string, queue chan outputItem.OutputItem) *OutputCollector {
    return &OutputCollector {
        taskIndex: taskIndex,
        taskName: taskName,
        Queue: make(chan *outputItem.OutputItem, itemCount),
    }
}

func (oc *OutputCollector) Emit(tuple *tuple.Tuple)  {
    oc.Queue <- outputItem.NewOutputItem(oc.taskIndex, tuple, oc.taskName)
}
