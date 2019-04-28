package command

import (
    "github.com/ff4415/hurricane-in-go/taskInfo"
    "github.com/ff4415/hurricane-in-go/tuple"
)

type CommandManager struct {
    SourceManagerAddress string
    ExecutorPosition *taskInfo.ExecutorPosition
    Tuple *tuple.Tuple
}

type CommandPresident struct {
    TaskPathName string
    TaskPath string
    FieldValue string
}
