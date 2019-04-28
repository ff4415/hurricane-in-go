package taskInfo

import (
    "github.com/ff4415/hurricane-in-go/groupMethodType"
    // "github.com/ff4415/hurricane-in-go/managerContext"
)

type ExecutorPosition struct {
    Manager string
    ExecutorIndex int
}

type PathInfo struct {
    GroupMethod groupMethodType.GroupMethodType
    DestinationTask string
    FieldName string
    DestinationExecutors []*ExecutorPosition
}

type TaskInfo struct {
    TopologyName string
    TaskName string
    Paths []*PathInfo
    ExecutorIndex int
    // ManagerContext managerContext.ManagerContext
}

func (ti *TaskInfo) AddPath(path *PathInfo)  {
    ti.Paths = append(ti.Paths, path)
}
