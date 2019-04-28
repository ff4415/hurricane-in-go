package iTask

import (
    "github.com/ff4415/hurricane-in-go/outputCollector"
)

type ITask interface {
    Prepare(outputCollector *outputCollector.OutputCollector)
    Cleanup()
    DeclareFields() []string
}
