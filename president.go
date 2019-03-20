package president

import (
    "configure"
    "groupMethodType"
    "topology"
    "commandUtil"
    "spoutDeclarer"
    "boltDeclarer"
    "managerContext"
    "taskInfo"
    "time"
    "math/rand"
)

rand := rand.New(rand.NewSource(time.Now().UnixNano()))

type taskPathName struct {
    Name string
    TaskPath string
}

type President struct {
    PresidentHost string
    Managers []*managerContext.ManagerContext
    ManagerCount int
    Configuration configure.Configure
    ManagerAddresses map[string]string
    FieldsCandidates map[taskPathName][]*taskInfo.ExecutorPosition
    FieldsDestinations map[taskPathName]map[string]*taskInfo.ExecutorPosition
    OrderIds map[string]int
    Submitted bool
}

var president *President

var args commandUtil.Args
var client commandUtil.Client
var sever commandUtil.Server

func GetPresident(configure string) *President  {
    sync.Once.Do(func ()  {
        president = &President {
            Configuration: configure.InitConfig(configure)
        }
        president.PresidentHost = president.Configuration["CONF_KEY_PRESIDENT_HOST"]
        president.ManagerCount = president.Configuration["CONF_KEY_MANAGER_COUNT"]
    })
    p.initEvents()
    return president
}

func (p *President) initEvents()  {
    server = commandUtil.GetServer()

    server.Name = p.Configuration["CONF_KEY_PRESIDENT_NAME"]
    server.Handler = p.requestHandler()

    if err := server.ListenAndServe(p.Configuration["CONF_KEY_PRESIDENT_HOST"]); err != nil {
        panic("error in ListenAndServe: %s", err)
    }
}

func (p *President) requestHandler(ctx *commandUtil.RequestCtx)  {

    args = commandUtil.Args.AcquireArgs()
    defer commandUtil.ReleaseArgs(args)

    switch string(ctx.Path()) {
    case "/onJoin":
        p.onJoin(ctx)
   case "/onAskField":
        p.onAskField(ctx)
    case "/onOrderId":
        // m.onOrderId(ctx)
    default:
        ctx.Error("Unsupported path", commandUtil.StatusNotFound)
    }
}

func (p *President) onJoin(ctx *commandUtil.RequestCtx)  {
    var context managerContext.ManagerContext
    err := json.Unmarshal(ctx.PostArgs(), &context)
    if err != nil {
        ctx.SetStatusCode(commandUtil.StatusOK)
        ctx.SetBody([]byte("error in json Unmarshal: %s", err))
        return
    }
    p.Managers = append(p.Managers, &context)
    p.ManagerAddresses = append(p.ManagerAddresses, map[context.Id]context.NetAddress)

    p.sendHeartbeat(context.Id)

    if len(p.Managers) == p.ManagerCount {
        topologyName := p.Configuration["CONF_KEY_TOPOLOGY_NAME"]
        topology := topology.GetTopologyLoader().Topologies[topologyName]
        p.submitTopology(topology)
    }

    ctx.SetStatusCode(commandUtil.StatusOK)
    ctx.SetBody([]byte("JoinPresident OK."))
}

func (p *president) onAskField(ctx *commandUtil.RequestCtx)  {
    var commandPresident command.CommandPresident
    err := json.Unmarshal(ctx.PostArgs(), &commandPresident)
    if err != nil {
        ctx.SetStatusCode(commandUtil.StatusOK)
        ctx.SetBody([]byte("error in json Unmarshal: %s", err))
        return
    }
    taskPathName := taskPathName {
        Name: commandPresident.taskPathName,
        TaskPath: commandPresident.TaskPath
    }
    fieldValue := commandPresident.FieldValue
    if taskPairIter, !ok := p.FieldsDestinations[taskPathName] {
        p.FieldsDestinations[taskPathName] = make(map[string]*taskInfo.ExecutorPosition)
        taskPairIter = p.FieldsDestinations[taskPathName]
    }
    if destinationPairIter, !ok := taskPairIter[fieldValue] {
        candidates := p.FieldsCandidates[taskPathName]
        positionIndex := rand.Intn(len(candidates))
        taskPairIter[fieldValue] = candidates[positionIndex]
        destinationPairIter = taskPairIter[fieldValue]
    }

    command := map[string]taskInfo.ExecutorPosition {
        "command": destinationPairIter
    }
    b, err := json.Marshal(command)
    if err != nil {
        log("err:", err)
    }

    ctx.SetStatusCode(commandUtil.StatusOK)
    ctx.SetBody([]byte(b)
}

func (p *President) submitTopology(topology  *topology.Topology)  {
    p.OrderId[topology.Name] = 0

    spoutDeclarers := topology.SpoutDeclarers
    boltDeclarers := topology.BoltDeclarers

    originSpoutTasks := getAllSpoutTasks(spoutDeclarers, &topology)
    nameToSpoutTasks := p.allocateSpoutTasks(originSpoutTasks)

    originBoltTasks := getAllBoltTasks(topology, boltDeclarers)
    nameToBoltTasks := p.allocateBoltTasks(originBoltTasks)

    p.calculateTaskPaths(nameToBoltTasks, boltDeclarers, nameToSpoutTasks)

    p.syncWithManagers()
    p.Submitted = true
    p.startConnections()
}

func getAllSpoutTasks(spoutDeclarers map[string]*spoutDeclarer.SpoutDeclarer, topology *topology.Topology) (originSpoutTasks []taskInfo.TaskInfo) {
    for _, spoutDeclarer := range spoutDeclarers {
        for taskIndex := 0; taskIndex < spoutDeclarer.ParallismHint; taskIndex++ {
            taskInfo := &taskInfo.TaskInfo {
                TopologyName: topology.name,
                TaskName: spoutDeclarer.TaskName
            }
            originSpoutTasks = append(originSpoutTasks, taskInfo)
        }
    }
    return originSpoutTasks
}

func (p *President) allocateSpoutTasks(originSpoutTasks *taskInfo.TaskInfo) (nameToSpoutTasks map[string][]*taskInfo.TaskInfo)  {
    nameToSpoutTasks = make(map[string][]*taskInfo.TaskInfo)

    for managerContext := range p.Managers {
        var index = 0
        for {
            if len(originSpoutTasks) == 0 {
                break
            }
            if index > len(originSpoutTasks) {
                break
            }
            spoutIndex = managerContext.useNextSpout()
            taskInfo := originSpoutTasks[index]

            ++index

            managerContext.SpoutTaskInfos[spoutIndex] = taskInfo
            taskInfo.ManagerContext = managerContext
            taskInfo.ExecutorIndex = spoutIndex

            taskName := taskInfo.TaskName
            if spoutTasks, !ok := nameToSpoutTasks[taskName] {
                nameToSpoutTasks[taskName] = make([]*taskInfo.TaskInfo)
                spoutTasks = nameToSpoutTasks[taskName]
            }
            spoutTasks = append(spoutTasks, managerContext.SpoutTaskInfos[spoutIndex])
            nameToSpoutTasks[taskName] = spoutTasks
        }
    }
    return nameToSpoutTasks
}

func getAllBoltTasks(topology *topology.Topology, boltDeclarers map[string]*boltDeclarer.BoltDeclarers) (originBoltTasks []taskInfo.TaskInfo)  {
    for _, boltDeclarer := range boltDeclarers {
        for taskIndex := 0; taskIndex < boltDeclarer.ParallismHint; taskIndex++ {
            taskInfo := &taskInfo.TaskInfo {
                TopologyName: topology.Name,
                TaskName: boltDeclarer.TaskName
            }
            originBoltTasks = append(originBoltTasks, taskInfo)
        }
    }
    return originBoltTasks
}

func (p *President) allocateBoltTasks(originBoltTasks []*taskInfo.TaskInfo) (nameToBoltTasks map[string][]*taskInfo.TaskInfo)  {

    nameToBoltTasks = make(map[string][]*taskInfo.TaskInfo)

    for managerContext := range p.Managers {
        var index = 0
        for {
            if len(originBoltTasks) == 0 {
                break
            }
            if index > len(originBoltTasks) {
                break
            }
            boltIndex = managerContext.UseNextBolt()
            taskInfo := originBoltTasks[index]
            taskInfo.ManagerContext = managerContext
            taskInfo.ExecutorIndex = boltIndex

            ++index

            managerContext.BoltTaskInfos[boltIndex] = taskInfo

            taskName := taskInfo.TaskName
            if spoutTasks, !ok := nameToBoltTasks[taskName] {
                nameToBoltTasks[taskName] = make([]*taskInfo.TaskInfo)
                boltTasks = nameToBoltTasks[taskName]
            }
            boltTasks = append(boltTasks, managerContext.BoltTaskInfos[spoutIndex])
            nameToBoltTasks[taskName] = boltTasks
        }
    }
    return nameToBoltTasks
}

func (p *President) calculateTaskPaths(nameToBoltTasks map[string][]*taskInfo.TaskInfo, boltDeclarers map[string]*boltDeclarer.BoltDeclarer, nameToSpoutTasks map[string][]*taskInfo.TaskInfo)  {
    for _, boltDeclarer := range boltDeclarers {
        if boltDeclarer.SourceTaskName == "" {
            continue
        }
        sourceTaskName := boltDeclarer.SourceTaskName
        sourceTasks := findSourceTask(nameToBoltTasks, nameToSpoutTasks, sourceTaskName)
        destTaskName := boltDeclarer.TaskName
        destTasks := findDestTask(nameToBoltTasks, destTaskName)

        var destExecutorPositions []*taskInfo.ExecutorPosition
        for destTask := range destTasks {
            destExecutorPositions = append(destExecutorPositions, &taskInfo.ExecutorPosition{
                Manager: destTask.ManagerContext.NetAddress,
                ExecutorIndex: destTask.ExecutorIndex
            })
        }
        if boltDeclarer.GroupMethod == groupMethodType.Global {
            for sourceTask := range sourceTasks {
                destTaskIndex := rand.Intn(len(destTasks))
                destTask := destTasks[destTaskIndex]
                pathInfo := &taskInfo.PathInfo{
                    GroupMethod: groupMethodType.Global,
                    DestinationTask: destTask.TaskName,
                    DestinationExecutors: []{
                        taskInfo.ExecutorPosition{
                            Manager: destTask.ManagerContext.NetAddress,
                            ExecutorIndex: destTask.ExecutorIndex
                        }
                    }
                    sourceTask.AddPath(pathInfo)
                }
            } else if boltDeclarer.GroupMethod == groupMethodType.Field {
                for sourceTask := range sourceTasks {
                    pathInfo := &taskInfo.PathInfo{
                        GroupMethod: groupMethodType.Field,
                        DestinationTask: destTaskName,
                        FieldName: boltDeclarer.GroupField
                    }
                    sourceTask.AddPath(pathInfo)
                }
                taskPathName := taskPathName{
                    Name: sourceTaskName,
                    TaskPath: destTaskName
                }
                p.FieldsCandidates[taskPathName] = destExecutorPositions
            } else if boltDeclarer.GroupMethod == groupMethodType.Random {
                for sourceTask := range sourceTasks {
                    pathInfo := &taskInfo.PathInfo{
                        GroupMethod: groupMethodType.Random,
                        DestinationTask: destTaskName,
                        DestinationExecutors: destExecutorPositions
                    }
                    sourceTask.AddPath(pathInfo)
                }
            } else {
                log("Unsupported group method occured")
                return
            }
        }
    }
}

func findSourceTask(nameToBoltTasks map[string][]*taskInfo.TaskInfo, nameToSpoutTasks map[string][]*taskInfo.TaskInfo, sourceTaskName string) []*taskInfo.TaskInfo  {
    if spoutTask, ok := nameToSpoutTasks[sourceTaskName] {
        return spoutTask
    }
    if boltTask, ok := nameToBoltTasks[sourceTaskName] {
        return boltTask
    }
    return make([]*taskInfo.TaskInfo)
}

func findDestTask(nameToBoltTasks map[string][]*taskInfo.TaskInfo, sourceTaskName string) []*taskInfo.TaskInfo {
    if boltTask, ok := nameToBoltTasks[sourceTaskName] {
        return boltTask
    }
    return make([]*taskInfo.TaskInfo)
}

func (p *President) syncWithManagers()  {
    args = commandUtil.Args.AcquireArgs()
    defer commandUtil.ReleaseArgs(args)

    var context managerContext.ManagerContext

    for managerContext := range p.Managers {
        destAddress := p.ManagerAddresses[managerContext.Id] + "/syncMetadata"

        client = commandUtil.GetClient()

        b, err := json.Marshal(context)
        if err != nil {
            log("err:", err)
        }
        args.SetBytesV("context", b)
        statusCode, body, err := client.Post(nil, destAddress, args)
        if err != nil || statusCode != 200 {
            log("err:%s,statusCode: %s", err, statusCode)
        }
    }
}

func (p *President) startConnections()  {
    go func() {
        timeDuration = time.Duration(10000)
        time.Sleep(timeDuration)
        for managerContext := range p.Managers {
            p.sendHeartbeat(managerContext.Id)
        }
    }()
}

func (p *President) sendHeartbeat(managerId string)  {
    args = commandUtil.Args.AcquireArgs()
    defer commandUtil.ReleaseArgs(args)

    workDuration = time.Duration(rand.Intn(2000))

    client = commandUtil.GetClient()
    client.ReadTimeout = workDuration
    client.WriteTimeout = workDuration

    destAddress := p.ManagerAddresses[managerId] + "/heartbeat"
    b := []byte("heartbeat")
    args.SetBytesV("context", b)
    statusCode, body, err := client.Post(nil, destAddress, args)
    if err != nil || statusCode != 200 {
        log("err:%s,statusCode: %s", err, statusCode)
    }
    log("alived")
}
