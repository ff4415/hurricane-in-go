package iBolt

import(
    "iTask"
    "tuple"
)

type IBolt interface {
    iTask.ITask
    Clone() *IBolt
    Execute(tuple tuple.Tuple)
}
