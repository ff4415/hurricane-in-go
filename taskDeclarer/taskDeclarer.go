package taskDeclarer

import (
    "github.com/ff4415/hurricane-in-go/taskType"
    "github.com/ff4415/hurricane-in-go/groupMethodType"
)


type TaskDeclarer struct {
    TopologyName string
    TaskName string
    Type taskType.TaskType
    GroupMethod groupMethodType.GroupMethodType
    SourceTaskName string
    ParallismHint int
}
