package iBolt

import(
    "github.com/ff4415/hurricane-in-go/iTask"
    "github.com/ff4415/hurricane-in-go/tuple"
)

type IBolt interface {
    iTask.ITask
    Clone() IBolt
    Execute(tuple *tuple.Tuple)
}
