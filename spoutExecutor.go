package spoutExecutor

import (
    "iSpout"
    "executor"
)

type SpoutExecutor struct {
    Spout &iSpout.ISpout
    // FlowParam int
}

func (se *SpoutExecutor) Start()  {
    go func() {
        for {
            se.Spout.NextTuple()
        }
    }()
}
