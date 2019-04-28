package president

import (
    "github.com/ff4415/hurricane-in-go/configure"
    "github.com/ff4415/hurricane-in-go/groupMethodType"
    "github.com/ff4415/hurricane-in-go/topology"
    "github.com/ff4415/hurricane-in-go/commandUtil"
    "github.com/ff4415/hurricane-in-go/spoutDeclarer"
    "github.com/ff4415/hurricane-in-go/boltDeclarer"
    "github.com/ff4415/hurricane-in-go/managerContext"
    "github.com/ff4415/hurricane-in-go/taskInfo"
    "github.com/ff4415/hurricane-in-go/command"
    "time"
    "math/rand"
    "sync"
    "strconv"
    "encoding/json"
    log "github.com/Sirupsen/logrus"
    "github.com/valyala/fasthttp"
    "fmt"
)

var r = rand.New(rand.NewSource(time.Now().UnixNano()))

type taskPathName struct {
    Name string
    TaskPath string
}

type President struct {
    PresidentHost string
    Managers []*managerContext.ManagerContext
    ManagerCount int
    Configuration configure.Configuration
    ManagerAddresses map[string]string
    FieldsCandidates map[taskPathName][]*taskInfo.ExecutorPosition
    FieldsDestinations map[taskPathName]map[string]*taskInfo.ExecutorPosition
    OrderIds map[string]int
    Submitted bool
}

var president *President

var args *commandUtil.Args
var client *commandUtil.Client
var server *commandUtil.Server

func GetPresident(cf string) *President  {
    var once sync.Once
    once.Do(func ()  {
        president = &President {
            Configuration: configure.InitConfig(cf),
        }
        president.PresidentHost = president.Configuration["CONF_KEY_PRESIDENT_HOST"]
        mc, err := strconv.Atoi(president.Configuration["CONF_KEY_MANAGER_COUNT"])
        if err != nil {
          log.Error(err)
          president.ManagerCount = 0
        } else {
          president.ManagerCount = mc
        }
    })
    president.initEvents()
    return president
}

func (p *President) initEvents()  {
    server = commandUtil.GetServer()

    server.Name = p.Configuration["CONF_KEY_PRESIDENT_NAME"]
    server.Handler = p.requestHandler

    if err := server.ListenAndServe(p.Configuration["CONF_KEY_PRESIDENT_HOST"]); err != nil {
        panic(err)
    }
}

func (p *President) requestHandler(ctx *fasthttp.RequestCtx)  {

    args = commandUtil.AcquireArgs()
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

func (p *President) onJoin(ctx *fasthttp.RequestCtx)  {
    var context managerContext.ManagerContext
    err := json.Unmarshal(ctx.PostBody(), &context)
    if err != nil {
        ctx.SetStatusCode(commandUtil.StatusOK)
        m := fmt.Sprintf("error in json Unmarshal: %s", err)
        ctx.SetBody([]byte(m))
        return
    }
    p.Managers = append(p.Managers, &context)
    p.ManagerAddresses[context.Id] = context.NetAddress
    // p.ManagerAddresses = append(p.ManagerAddresses, map[context.Id]context.NetAddress)

    p.sendHeartbeat(context.Id)

    if len(p.Managers) == p.ManagerCount {
        topologyName := p.Configuration["CONF_KEY_TOPOLOGY_NAME"]
        topology := topology.GetTopologyLoader().Topologies[topologyName]
        p.submitTopology(topology)
    }

    ctx.SetStatusCode(commandUtil.StatusOK)
    ctx.SetBody([]byte("JoinPresident OK."))
}

func (p *President) onAskField(ctx *fasthttp.RequestCtx)  {
    var commandPresident command.CommandPresident
    err := json.Unmarshal(ctx.PostBody(), &commandPresident)
    if err != nil {
        ctx.SetStatusCode(commandUtil.StatusOK)
        m := fmt.Sprintf("error in json Unmarshal: %s", err)
        ctx.SetBody([]byte(m))
        return
    }
    taskPathName := taskPathName {
        Name: commandPresident.TaskPathName,
        TaskPath: commandPresident.TaskPath,
    }
    fieldValue := commandPresident.FieldValue

    // var taskPairIter map[string]*taskInfo.ExecutorPosition
    // var destinationPairIter *taskInfo.ExecutorPosition

    taskPairIter, ok := p.FieldsDestinations[taskPathName]
    if !ok {
        p.FieldsDestinations[taskPathName] = make(map[string]*taskInfo.ExecutorPosition)
        taskPairIter = p.FieldsDestinations[taskPathName]
    }

    destinationPairIter, ok := taskPairIter[fieldValue]
    if !ok {
        candidates := p.FieldsCandidates[taskPathName]
        positionIndex := r.Intn(len(candidates))
        taskPairIter[fieldValue] = candidates[positionIndex]
        destinationPairIter = taskPairIter[fieldValue]
    }

    command := map[string]*taskInfo.ExecutorPosition {
        "command": destinationPairIter,
    }
    b, err := json.Marshal(command)
    if err != nil {
        log.Error("err:", err)
    }

    ctx.SetStatusCode(commandUtil.StatusOK)
    ctx.SetBody([]byte(b))
}

func (p *President) submitTopology(topology  *topology.Topology)  {
    // p.OrderId[topology.Name] = 0

    spoutDeclarers := topology.SpoutDeclarers
    boltDeclarers := topology.BoltDeclarers

    originSpoutTasks := getAllSpoutTasks(spoutDeclarers, topology)
    nameToSpoutTasks := p.allocateSpoutTasks(originSpoutTasks)

    originBoltTasks := getAllBoltTasks(topology, boltDeclarers)
    nameToBoltTasks := p.allocateBoltTasks(originBoltTasks)

    p.calculateTaskPaths(nameToBoltTasks, boltDeclarers, nameToSpoutTasks)

    p.syncWithManagers()
    p.Submitted = true
    p.startConnections()
}

func getAllSpoutTasks(spoutDeclarers map[string]*spoutDeclarer.SpoutDeclarer, topology *topology.Topology) (originSpoutTasks []*taskInfo.TaskInfo) {
    for _, spoutDeclarer := range spoutDeclarers {
        for taskIndex := 0; taskIndex < spoutDeclarer.ParallismHint; taskIndex++ {
            taskInfo := &taskInfo.TaskInfo {
                TopologyName: topology.Name,
                TaskName: spoutDeclarer.TaskName,
            }
            originSpoutTasks = append(originSpoutTasks, taskInfo)
        }
    }
    return originSpoutTasks
}

func (p *President) allocateSpoutTasks(originSpoutTasks []*taskInfo.TaskInfo) (nameToSpoutTasks map[string][]*taskInfo.TaskInfo)  {
    // nameToSpoutTasks := make(map[string][]*taskInfo.TaskInfo)

    for _, managerContext := range p.Managers {
        var index = 0
        for {
            if len(originSpoutTasks) == 0 {
                break
            }
            if index > len(originSpoutTasks) {
                break
            }
            spoutIndex := managerContext.UseNextSpout()
            ti := originSpoutTasks[index]

            index++

            managerContext.SpoutTaskInfos[spoutIndex] = ti
            // ti.ManagerContext = managerContext
            ti.ExecutorIndex = spoutIndex
            taskName := ti.TaskName

            // var spoutTasks []*taskInfo.TaskInfo
            spoutTasks, ok := nameToSpoutTasks[taskName]
            if !ok {
                nameToSpoutTasks[taskName] = []*taskInfo.TaskInfo{}
                spoutTasks = nameToSpoutTasks[taskName]
            }
            spoutTasks = append(spoutTasks, managerContext.SpoutTaskInfos[spoutIndex])
            nameToSpoutTasks[taskName] = spoutTasks
        }
    }
    return nameToSpoutTasks
}

func getAllBoltTasks(topology *topology.Topology, boltDeclarers map[string]*boltDeclarer.BoltDeclarer) (originBoltTasks []*taskInfo.TaskInfo)  {
    for _, boltDeclarer := range boltDeclarers {
        for taskIndex := 0; taskIndex < boltDeclarer.ParallismHint; taskIndex++ {
            taskInfo := &taskInfo.TaskInfo {
                TopologyName: topology.Name,
                TaskName: boltDeclarer.TaskName,
            }
            originBoltTasks = append(originBoltTasks, taskInfo)
        }
    }
    return originBoltTasks
}

func (p *President) allocateBoltTasks(originBoltTasks []*taskInfo.TaskInfo) (nameToBoltTasks map[string][]*taskInfo.TaskInfo)  {
    // nameToBoltTasks := make(map[string][]*taskInfo.TaskInfo)

    for _, managerContext := range p.Managers {
        var index = 0
        for {
            if len(originBoltTasks) == 0 {
                break
            }
            if index > len(originBoltTasks) {
                break
            }
            boltIndex := managerContext.UseNextBolt()
            ti := originBoltTasks[index]
            // ti.ManagerContext = managerContext
            ti.ExecutorIndex = boltIndex

            index++

            managerContext.BoltTaskInfos[boltIndex] = ti

            taskName := ti.TaskName

            boltTasks, ok := nameToBoltTasks[taskName]
            if !ok {
                nameToBoltTasks[taskName] = []*taskInfo.TaskInfo{}
                boltTasks = nameToBoltTasks[taskName]
            }
            boltTasks = append(boltTasks, managerContext.BoltTaskInfos[boltIndex])
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
    for _, destTask := range destTasks {
      destExecutorPositions = append(destExecutorPositions, &taskInfo.ExecutorPosition{
        // Manager: destTask.ManagerContext.NetAddress,
        ExecutorIndex: destTask.ExecutorIndex,
      })
    }
    if boltDeclarer.GroupMethod == groupMethodType.Global {
      for _, sourceTask := range sourceTasks {
        destTaskIndex := r.Intn(len(destTasks))
        destTask := destTasks[destTaskIndex]
        pi := &taskInfo.PathInfo{
          GroupMethod: groupMethodType.Global,
          DestinationTask: destTask.TaskName,
          DestinationExecutors: []*taskInfo.ExecutorPosition{
              &taskInfo.ExecutorPosition{
                  // Manager: destTask.ManagerContext.NetAddress,
                  ExecutorIndex: destTask.ExecutorIndex,
              },
          },
        }
        sourceTask.AddPath(pi)
      }
    } else if boltDeclarer.GroupMethod == groupMethodType.Field {
      for _, sourceTask := range sourceTasks {
        pathInfo := &taskInfo.PathInfo{
          GroupMethod: groupMethodType.Field,
          DestinationTask: destTaskName,
          FieldName: boltDeclarer.GroupField,
        }
        sourceTask.AddPath(pathInfo)
      }
      taskPathName := taskPathName{
        Name: sourceTaskName,
        TaskPath: destTaskName,
      }
      p.FieldsCandidates[taskPathName] = destExecutorPositions
    } else if boltDeclarer.GroupMethod == groupMethodType.Random {
      for _, sourceTask := range sourceTasks {
        pathInfo := &taskInfo.PathInfo{
          GroupMethod: groupMethodType.Random,
          DestinationTask: destTaskName,
          // DestinationExecutors: destExecutorPositions
        }
        sourceTask.AddPath(pathInfo)
      }
    } else {
        log.Error("Unsupported group method occured")
        return
    }
  }
}

func findSourceTask(nameToBoltTasks map[string][]*taskInfo.TaskInfo, nameToSpoutTasks map[string][]*taskInfo.TaskInfo, sourceTaskName string) []*taskInfo.TaskInfo  {
    if spoutTask, ok := nameToSpoutTasks[sourceTaskName]; ok {
        return spoutTask
    }
    if boltTask, ok := nameToBoltTasks[sourceTaskName]; ok {
        return boltTask
    }
    return []*taskInfo.TaskInfo{}
}

func findDestTask(nameToBoltTasks map[string][]*taskInfo.TaskInfo, sourceTaskName string) []*taskInfo.TaskInfo {
    if boltTask, ok := nameToBoltTasks[sourceTaskName]; ok {
        return boltTask
    }
    return []*taskInfo.TaskInfo{}
}

func (p *President) syncWithManagers()  {
    args = commandUtil.AcquireArgs()
    defer commandUtil.ReleaseArgs(args)

    var context managerContext.ManagerContext

    for _, managerContext := range p.Managers {
        destAddress := p.ManagerAddresses[managerContext.Id] + "/syncMetadata"

        client = commandUtil.GetClient()

        b, err := json.Marshal(context)
        if err != nil {
            log.Error("err:", err)
        }
        args.SetBytesV("context", b)
        statusCode, _, err := client.Post(nil, destAddress, args)
        if err != nil || statusCode != 200 {
            log.Error("err:%s,statusCode: %s", err, statusCode)
        }
    }
}

func (p *President) startConnections()  {
    go func() {
        timeDuration := time.Duration(10000)
        time.Sleep(timeDuration)
        for _, managerContext := range p.Managers {
            p.sendHeartbeat(managerContext.Id)
        }
    }()
}

func (p *President) sendHeartbeat(managerId string)  {
    args = commandUtil.AcquireArgs()
    defer commandUtil.ReleaseArgs(args)

    workDuration := time.Duration(r.Intn(2000))

    client = commandUtil.GetClient()
    client.ReadTimeout = workDuration
    client.WriteTimeout = workDuration

    destAddress := p.ManagerAddresses[managerId] + "/heartbeat"
    b := []byte("heartbeat")
    args.SetBytesV("context", b)
    statusCode, _, err := client.Post(nil, destAddress, args)
    if err != nil || statusCode != 200 {
        log.Error("err:%s,statusCode: %s", err, statusCode)
    }
    log.Info("alived")
}
