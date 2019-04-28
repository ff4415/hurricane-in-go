package managerContext

import (
    "github.com/ff4415/hurricane-in-go/taskType"
    "github.com/ff4415/hurricane-in-go/taskInfo"
)
// type TaskType taskType.TaskType
// type TaskInfo taskInfo.TaskInfo

type ManagerContext struct {
    ExecutorType taskType.TaskType
    Id string
    NetAddress string
    SpoutCount int
    BoltCount int
    Spouts map[int]bool
    Bolts map[int]bool
    BoltTaskInfos []*taskInfo.TaskInfo
    SpoutTaskInfos []*taskInfo.TaskInfo
    // FreeSpouts map[int]bool
    // BusySpouts map[int]bool
    // FreeBolts map[int]bool
    // BusyBolts map[int]bool
}

func (mc *ManagerContext) UseSpout(spoutIndex int)  {
    if spoutIndex < mc.SpoutCount {
        mc.Spouts[spoutIndex] = true
    }
}

func (mc *ManagerContext) UseNextSpout() int  {
    for spoutIndex := 0; spoutIndex < mc.SpoutCount; spoutIndex++ {
        if mc.Spouts[spoutIndex] != true {
            mc.Spouts[spoutIndex] = true
            return spoutIndex
        }
    }
    return 0
}

func (mc *ManagerContext) FreeSpout(spoutIndex int)  {
    mc.Spouts[spoutIndex] = false
}

func (mc *ManagerContext) UseBolt(boltIndex int)  {
    if boltIndex < mc.BoltCount {
        mc.Bolts[boltIndex] = true
    }
}

func (mc *ManagerContext) UseNextBolt() int  {
    for boltIndex := 0; boltIndex < mc.BoltCount; boltIndex++ {
        if mc.Bolts[boltIndex] != true {
            mc.Bolts[boltIndex] = true
            return boltIndex
        }
    }
    return 0
}

func (mc *ManagerContext) FreeBolt(boltIndex int)  {
    mc.Bolts[boltIndex] = false
}
