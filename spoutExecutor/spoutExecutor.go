package spoutExecutor

import (
    "github.com/ff4415/hurricane-in-go/iSpout"
    // "github.com/ff4415/hurricane-in-go/executor"
)

type SpoutExecutor struct {
    Spout *iSpout.ISpout
    // FlowParam int
}

func (se *SpoutExecutor) Start()  {
    go func() {
        for {
            (*se.Spout).NextTuple()
        }
    }()
}
