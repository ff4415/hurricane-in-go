package outputCollector

import (
    "outputItem"
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
        queue: make(chan &outputItem.OutputItem, itemCount)
    }
}

func (oc *OutputCollector) Emit(tuple tuple.Tuple)  {
    oc.queue <- outputItem.NewOutputItem(oc.taskIndex, tuple, oc.taskName)
}
