package iSpout

import (
    "github.com/ff4415/hurricane-in-go/iTask"
)

type ISpout interface {
    iTask.ITask
    Clone() ISpout
    NextTuple()
}
