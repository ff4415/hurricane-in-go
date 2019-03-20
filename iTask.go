package iTask

import (
    "outputCollector"
)

type ITask interface {
    Prepare(outputCollector *outputCollector.OutputCollector)
    Cleanup()
    DeclareFields() []string
}
