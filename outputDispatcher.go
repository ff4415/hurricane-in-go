package outputDispatcher

import (
    "taskInfo"
    "taskItem"
    "outputItem"
    "groupMethodType"
    "command"
    "commandUtil"
    "math/rand"
    "time"
    "sync"
    "encoding/json"
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
    SelfTasks chan taskItem.TaskItem
    PresidentAddress string
    CommandAddresses map[string]string
    FieldsDestinations map[TaskPathName]map[string]*taskInfo.ExecutorPosition
    TaskFields map[string][]string
    TaskFieldsMap map[string]map[string]int
}

var client commandUtil.Client
var args commandUtil.Args

func (od *OutputDispatcher) Start()  {
    go func() {

        client = commandUtil.GetClient()
        args = commandUtil.Args.AcquireArgs()

        defer commandUtil.ReleaseArgs(args)

        select {
        case outputItem := od.Queue :
            taskIndex := outputItem.GetTaskIndex()
            taskInfo := od.TaskInfos[taskIndex]
            for pathInfo := range taskInfo.Paths {
                processPath(taskInfo, pathInfo, outputItem)
            }
        }
    }()
}

func (od *OutputDispatcher) processPath(taskInfo taskInfo.TaskInfo, pathInfo taskInfo.PathInfo, outputItem outputItem.OutputItem)  {
    sourceTaskName := taskInfo.TaskName
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
            TaskPath: destTaskName
        }
        if destinations, !ok := od.FieldsDestinations[taskPathName] {
            log.printf("no such path")
            od.FieldsDestinations[taskPathName] = make(map[string]*taskInfo.ExecutorPosition)
        }
        if fieldIndex, !ok := od.TaskFieldsMap[sourceTaskName][pathInfo.FieldName] {
            log.printf("no such path")
        }
        fieldValue := outputItem.Tuple.Values[fieldIndex]

        if fieldDestIter, !ok := destinations[fieldValue] {
            callback := func (ep *taskInfo.ExecutorPosition) {
                od.FieldsDestinations[taskPathName] = map[fieldValue]ep
                od.sendTupleTo(outputItem, ep)
            }
            od.askField(taskPathName, fieldValue, callback)
        } else {
            od.sendTupleTo(outputItem, fieldDestIter)
        }
    }
}

func (od *OutputDispatcher) sendTupleTo(outputItem *OutputItem, executorPosition *ExecutorPosition)  {
    destIdentifier := executorPosition.Manager
    selfIdentifier := od.selfAddress

    if destIdentifier == selfIdentifier {
        executorIndex := executorPosition.ExecutorIndex

        taskItem := taskItem.NewTaskItem(outputItem.GetTaskIndex(), outputItem.GetTuple())
        od.SelfTasks <- taskItem
    } else {
        command := command.CommandManager {
            SourceManagerAddress: selfIdentifier,
            ExecutorPosition: executorPosition,
            Tuple: outputItem.GetTuple()
        }
        b, err := json.Marshal(command)
        if err != nil {
            log("err:", err)
        }
        args.SetBytesV("command", b)
        destAddress := destIdentifier + "/sendTuple"
        statusCode, body, err := client.Post(nil, destAddress, args)
        if err != nil || statusCode != 200 {
            log("err:%s,statusCode: %s", err, statusCode)
        }
    }
}

func (od *OutputDispatcher) askField(taskPathName TaskPathName, fieldValue string, callback func(executorPosition *taskInfo.ExecutorPosition))  {
    destIdentifier := od.PresidentAddress
    command := command.CommandPresident {
        TaskPathName: taskPathName.Name,
        TaskPath: taskPathName.TaskPath,
        FieldValue: fieldValue
    }
    b, err := json.Marshal(command)
    if err != nil {
        log("err:", err)
    }
    args.SetBytesV("command", b)
    destAddress := destIdentifier + "/onAskField"
    statusCode, body, err := client.Post(nil, destAddress, args)
    if err != nil || statusCode != 200 {
        log("err:%s,statusCode: %s", err, statusCode)
    }
    resp := make(map[string]taskInfo.ExecutorPosition)
    err := json.Unmarshal(body, resp)
    if err != nil {
        log("err:", err)
    }
    callback(resp["command"])
}
