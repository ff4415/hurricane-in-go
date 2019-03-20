package taskDeclarer

import (
    "taskType"
    "groupMethodType"
)


type TaskDeclarer struct {
    TopologyName string
    TaskName string
    Type taskType.TaskType
    GroupMethod groupMethodType.GroupMethodType
    SourceTaskName string
    ParallismHint int
}
