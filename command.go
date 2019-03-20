package command

import (
    "taskInfo"
    "tuple"
)

type CommandManager struct {
    SourceManagerAddress string
    ExecutorPosition taskInfo.ExecutorPosition
    Tuple tuple.Tuple
}

type CommandPresident struct {
    TaskPathName string
    TaskPath string
    FieldValue string
}
