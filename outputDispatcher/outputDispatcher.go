package outputDispatcher

import (
    "github.com/ff4415/hurricane-in-go/taskInfo"
    "github.com/ff4415/hurricane-in-go/taskItem"
    "github.com/ff4415/hurricane-in-go/outputItem"
    "github.com/ff4415/hurricane-in-go/groupMethodType"
    "github.com/ff4415/hurricane-in-go/command"
    "github.com/ff4415/hurricane-in-go/commandUtil"
    "math/rand"
    "time"
    // "sync"
    "encoding/json"
    log "github.com/Sirupsen/logrus"
)

type TaskPathName struct {
    Name string
    TaskPath string
}

type OutputDispatcher struct {
    Queue chan outputItem.OutputItem
    SelfAddress string
    SelfSpoutCount int
    SelfBoltCount int
    BoltTaskInfos []*taskInfo.TaskInfo
    SpoutTaskInfos []*taskInfo.TaskInfo
    SelfTasks []chan *taskItem.TaskItem
    PresidentAddress string
    CommandAddresses map[string]string
    FieldsDestinations map[TaskPathName]map[string]*taskInfo.ExecutorPosition
    TaskFields map[string][]string
    TaskFieldsMap map[string]map[string]int
}

var client *commandUtil.Client
var args *commandUtil.Args

func (od *OutputDispatcher) Start()  {
    go func() {

        client = commandUtil.GetClient()
        args = commandUtil.AcquireArgs()

        defer commandUtil.ReleaseArgs(args)

        select {
        case outputItem := <-od.Queue :
            taskIndex := outputItem.GetTaskIndex()
            taskInfo := od.BoltTaskInfos[taskIndex] // todo? : bolts or spouts or both bolts and spouts
            for _, pathInfo := range taskInfo.Paths {
                od.processPath(taskInfo, pathInfo, &outputItem)
            }
        }
    }()
}

func (od *OutputDispatcher) processPath(ti *taskInfo.TaskInfo, pathInfo *taskInfo.PathInfo, outputItem *outputItem.OutputItem)  {
    sourceTaskName := ti.TaskName
    destTaskName := pathInfo.DestinationTask
    groupMethod := pathInfo.GroupMethod

    if groupMethod == groupMethodType.Global {
        executorPosition := pathInfo.DestinationExecutors[0]
        od.sendTupleTo(outputItem, executorPosition)

    } else if groupMethod == groupMethodType.Random {
        rand.New(rand.NewSource(time.Now().UnixNano()))
        destIndex := rand.Intn(len(pathInfo.DestinationExecutors))
        executorPosition := pathInfo.DestinationExecutors[destIndex]
        od.sendTupleTo(outputItem, executorPosition)

    } else if groupMethod == groupMethodType.Field {
        taskPathName := TaskPathName {
            Name: sourceTaskName,
            TaskPath: destTaskName,
        }
        destinations, ok := od.FieldsDestinations[taskPathName]
        if !ok {
            log.Info("no such path")
            od.FieldsDestinations[taskPathName] = make(map[string]*taskInfo.ExecutorPosition)
        }
        fieldIndex, ok := od.TaskFieldsMap[sourceTaskName][pathInfo.FieldName]
        if !ok {
            log.Info("no such path")
        }
        fieldValue := string(outputItem.GetTuple().Values[fieldIndex])

        fieldDestIter, ok := destinations[fieldValue]
        if !ok {
            callback := func (ep *taskInfo.ExecutorPosition) {
                od.FieldsDestinations[taskPathName][fieldValue] = ep
                od.sendTupleTo(outputItem, ep)
            }
            od.askField(taskPathName, fieldValue, callback)
        } else {
            od.sendTupleTo(outputItem, fieldDestIter)
        }
    }
}

func (od *OutputDispatcher) sendTupleTo(outputItem *outputItem.OutputItem, executorPosition *taskInfo.ExecutorPosition)  {
    destIdentifier := executorPosition.Manager
    selfIdentifier := od.SelfAddress

    if destIdentifier == selfIdentifier {
        // executorIndex := executorPosition.ExecutorIndex

        taskItem := taskItem.NewTaskItem(outputItem.GetTaskIndex(), outputItem.GetTuple())
        od.SelfTasks[taskItem.GetTaskIndex()] <- taskItem
    } else {
        command := command.CommandManager {
            SourceManagerAddress: selfIdentifier,
            ExecutorPosition: executorPosition,
            Tuple: outputItem.GetTuple(),
        }
        b, err := json.Marshal(command)
        if err != nil {
            log.Error("err:", err)
        }
        args.SetBytesV("command", b)
        destAddress := destIdentifier + "/sendTuple"
        statusCode, _, err := client.Post(nil, destAddress, args)
        if err != nil || statusCode != 200 {
            log.Error("err:%s,statusCode: %s", err, statusCode)
        }
    }
}

func (od *OutputDispatcher) askField(taskPathName TaskPathName, fieldValue string, callback func(executorPosition *taskInfo.ExecutorPosition))  {
    destIdentifier := od.PresidentAddress
    command := command.CommandPresident {
        TaskPathName: taskPathName.Name,
        TaskPath: taskPathName.TaskPath,
        FieldValue: fieldValue,
    }
    b, err := json.Marshal(command)
    if err != nil {
        log.Error("err:", err)
    }
    args.SetBytesV("command", b)
    destAddress := destIdentifier + "/onAskField"
    statusCode, body, err := client.Post(nil, destAddress, args)
    if err != nil || statusCode != 200 {
        log.Error("err:%s,statusCode: %s", err, statusCode)
    }
    resp := make(map[string]*taskInfo.ExecutorPosition)
    err = json.Unmarshal(body, resp)
    if err != nil {
        log.Error("err:", err)
    }
    callback(resp["command"])
}
