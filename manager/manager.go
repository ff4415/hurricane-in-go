package manager

import (
  "github.com/ff4415/hurricane-in-go/command"
  "github.com/ff4415/hurricane-in-go/commandUtil"
  "github.com/ff4415/hurricane-in-go/managerContext"
  "github.com/ff4415/hurricane-in-go/spoutExecutor"
  "github.com/ff4415/hurricane-in-go/boltExecutor"
  "github.com/ff4415/hurricane-in-go/outputCollector"
  "github.com/ff4415/hurricane-in-go/outputDispatcher"
  "github.com/ff4415/hurricane-in-go/taskItem"
  "github.com/ff4415/hurricane-in-go/taskInfo"
  "github.com/ff4415/hurricane-in-go/outputItem"
  "github.com/ff4415/hurricane-in-go/topology"
  "github.com/ff4415/hurricane-in-go/configure"
  // "github.com/ff4415/hurricane-in-go/iSpout"
  // "github.com/ff4415/hurricane-in-go/iBolt"
  "strconv"
  "sync"
  "encoding/json"
  "fmt"
  "github.com/valyala/fasthttp"
  log "github.com/Sirupsen/logrus"
)

type Manager struct {
    Name string
    Host string
    ManagerConfiguration configure.Configuration
    PresidentAddress string
    SelfContext *managerContext.ManagerContext
    SpoutExecutors []*spoutExecutor.SpoutExecutor
    BoltExecutors []*boltExecutor.BoltExecutor
    SpoutCollectors []*outputCollector.OutputCollector
    BoltCollectors []*outputCollector.OutputCollector
    BoltTaskQueues []chan *taskItem.TaskItem
    Topology *topology.Topology
    OutputDispatcher *outputDispatcher.OutputDispatcher
    TaskFields map[string][]string
    TaskFieldsMap map[string]map[string]int
}

var args *commandUtil.Args
var client *commandUtil.Client
var server *commandUtil.Server

var manager *Manager

func GetManager(cf string) *Manager  {
    var once sync.Once
    once.Do(func ()  {
        manager = &Manager {
            // ManagerConfiguration: configure.InitConfig("/db_configuration")
            ManagerConfiguration: configure.InitConfig(cf),
        }
        manager.Name = manager.ManagerConfiguration["CONF_KEY_MANAGER_NAME"]
        manager.Host = manager.ManagerConfiguration["CONF_KEY_MANAGER_HOST"]
    })

    manager.initPresidentConnector()
    manager.initSelfContext()
    manager.reserveExecutors()
    manager.initEvents()

    return manager
}

func (m *Manager) initPresidentConnector()  {
    m.PresidentAddress = m.ManagerConfiguration["CONF_KEY_PRESIDENT_HOST"]
    client = commandUtil.GetClient()
    client.Name = m.ManagerConfiguration["CONF_KEY_MANAGER_NAME"]
}

func (m *Manager) initSelfContext()  {
    sc, err := strconv.Atoi(m.ManagerConfiguration["CONF_KEY_SPOUT_COUNT"])
    if err != nil {
      panic(err)
    }
    bc, err := strconv.Atoi(m.ManagerConfiguration["CONF_KEY_BOLT_COUNT"])
    if err != nil {
      panic(err)
    }
    m.SelfContext = &managerContext.ManagerContext{
        Id: m.Name,
        NetAddress: m.Host,
        SpoutCount: sc,
        BoltCount: bc,
        SpoutTaskInfos: make([]*taskInfo.TaskInfo, sc),
        BoltTaskInfos: make([]*taskInfo.TaskInfo, bc),
        Spouts: make(map[int]bool),
        Bolts: make(map[int]bool),
    }
    for index := 0; index < m.SelfContext.SpoutCount; index++ {
        m.SelfContext.Spouts[index] = false
    }
    for index := 0; index < m.SelfContext.BoltCount; index++ {
        m.SelfContext.Bolts[index] = false
    }
}

func (m *Manager) reserveExecutors()  {
    sc, err := strconv.Atoi(m.ManagerConfiguration["CONF_KEY_SPOUT_COUNT"])
    if err != nil {
      panic(err)
    }
    bc, err := strconv.Atoi(m.ManagerConfiguration["CONF_KEY_BOLT_COUNT"])
    if err != nil {
      panic(err)
    }
    m.SpoutExecutors = make([]*spoutExecutor.SpoutExecutor, sc)
    m.BoltExecutors = make([]*boltExecutor.BoltExecutor, bc)
    m.SpoutCollectors = make([]*outputCollector.OutputCollector, sc)
    m.BoltCollectors = make([]*outputCollector.OutputCollector, bc)
    m.BoltTaskQueues = make([]chan *taskItem.TaskItem, bc)
    m.OutputDispatcher = &outputDispatcher.OutputDispatcher {
        SelfAddress: m.ManagerConfiguration["CONF_KEY_MANAGER_HOST"],
        PresidentAddress: m.ManagerConfiguration["CONF_KEY_PRESIDENT_HOST"],
        Queue: make(chan outputItem.OutputItem),
        SelfTasks: m.BoltTaskQueues,
        SelfSpoutCount: sc,
        SelfBoltCount: bc,
    }
    m.OutputDispatcher.Start()
}

func (m *Manager) initEvents()  {
    server = commandUtil.GetServer()

    server.Name = m.ManagerConfiguration["CONF_KEY_MANAGER_NAME"]
    server.Handler = m.requestHandler

    if err := server.ListenAndServe(m.ManagerConfiguration["CONF_KEY_MANAGER_HOST"]); err != nil {
        panic(fmt.Sprintf("error in ListenAndServe: %s", err))
    }
}

func (m *Manager) requestHandler(ctx *fasthttp.RequestCtx)  {

    args = commandUtil.AcquireArgs()
    defer commandUtil.ReleaseArgs(args)

    switch string(ctx.Path()) {
    case "/heartbeat":
       m.onHeartbeat(ctx)
    case "/syncMetadata":
        m.onSyncMetadata(ctx)
    case "/sendTuple":
        m.onSendTuple(ctx)
    default:
        ctx.Error("Unsupported path", commandUtil.StatusNotFound)
    }
}

func (m *Manager) onHeartbeat(ctx *fasthttp.RequestCtx)  {
    ctx.SetStatusCode(commandUtil.StatusOK)
    ctx.SetBody([]byte(fmt.Sprintf("%s is online.", m.Name)))
}

func (m *Manager) onSyncMetadata(ctx *fasthttp.RequestCtx)  {
    var metaData *managerContext.ManagerContext
    err := json.Unmarshal(ctx.PostBody(), &metaData)
    if err != nil {
        ctx.SetStatusCode(commandUtil.StatusOK)
        ctx.SetBody([]byte(fmt.Sprintf("error in json Unmarshal: %s", err)))
        return
    }
    m.SelfContext = metaData

    // m.ownManagerTasks()
    m.OutputDispatcher.SpoutTaskInfos = m.SelfContext.SpoutTaskInfos
    m.OutputDispatcher.BoltTaskInfos = m.SelfContext.BoltTaskInfos

    // m.showManagerMetadata();
    // m.showTaskInfos();

    m.Topology = topology.GetTopologyLoader().Topologies[m.ManagerConfiguration["CONF_KEY_TOPOLOGY_NAME"]]

    m.initTaskFieldsMap();
    m.initExecutors();

    ctx.SetStatusCode(commandUtil.StatusOK)
    ctx.SetBody([]byte("SyncMetadata OK."))
}


func (m *Manager) onSendTuple(ctx *fasthttp.RequestCtx)  {
    var context command.CommandManager
    err := json.Unmarshal(ctx.PostBody(), &context)
    if err != nil {
        ctx.SetStatusCode(commandUtil.StatusOK)
        ctx.SetBody([]byte(fmt.Sprintf("error in json Unmarshal: %s", err)))
        return
    }
    executorIndex := context.ExecutorPosition.ExecutorIndex
    boltCount := m.SelfContext.BoltCount

    taskQueue := m.BoltTaskQueues[boltCount]

    taskItem := taskItem.NewTaskItem(executorIndex, context.Tuple)

    taskQueue <- taskItem

    ctx.SetStatusCode(commandUtil.StatusOK)
    ctx.SetBody([]byte("SyncMetadata OK."))
}

func (m *Manager) ownManagerTasks()  { }

func (m *Manager) initTaskFieldsMap()  {
    spoutDeclarers := m.Topology.SpoutDeclarers

    for _, spoutDeclarer := range spoutDeclarers {
        m.TaskFields[spoutDeclarer.TaskName] = spoutDeclarer.GetFields()
        m.TaskFieldsMap[spoutDeclarer.TaskName] = spoutDeclarer.GetFieldsMap()
    }

    boltDeclarers := m.Topology.BoltDeclarers

    for _, boltDeclarer := range boltDeclarers {
        m.TaskFields[boltDeclarer.TaskName] = boltDeclarer.GetFields()
        m.TaskFieldsMap[boltDeclarer.TaskName] = boltDeclarer.GetFieldsMap()
    }
    m.OutputDispatcher.TaskFields = m.TaskFields
    m.OutputDispatcher.TaskFieldsMap = m.TaskFieldsMap
}

func (m *Manager) initExecutors()  {
    m.initSpoutExecutors()
    m.initBoltExecutors()

    bolts := m.SelfContext.Bolts
    spouts := m.SelfContext.Spouts

    for index := 0; index < m.SelfContext.SpoutCount; index++ {
            if spouts[index] == true {
                m.SpoutExecutors[index].Start()
            }
    }

    for index := 0; index < m.SelfContext.BoltCount; index++ {
            if bolts[index] == true {
                m.BoltExecutors[index].Start()
            }
    }
}

func (m *Manager) initSpoutExecutors()  {
    log.Info("Init spout executors")
    spoutDeclarers := m.Topology.SpoutDeclarers
    spouts := m.SelfContext.Spouts
    for spoutIndex, _ := range spouts {
        if spouts[spoutIndex] == true {
            spoutTask := m.SelfContext.SpoutTaskInfos[spoutIndex]
            spoutDeclarer := spoutDeclarers[spoutTask.TaskName]
            m.SpoutCollectors[spoutIndex] = outputCollector.NewOutputCollector(spoutIndex, spoutTask.TaskName, m.OutputDispatcher.Queue)

            spout := (*spoutDeclarer.GetSpout()).Clone()
            (*spout).Prepare(m.SpoutCollectors[spoutIndex])

            spoutExecutor := &spoutExecutor.SpoutExecutor {
                Spout: spout,
            }
            m.SpoutExecutors[spoutIndex] = spoutExecutor
        }
    }
}

func (m *Manager) initBoltExecutors()  {
    log.Info("Init bolt executors")
    boltDeclarers := m.Topology.BoltDeclarers
    bolts := m.SelfContext.Bolts
    for boltIndex, _ := range bolts {
        if bolts[boltIndex] == true {
            boltTask := m.SelfContext.BoltTaskInfos[boltIndex]
            boltDeclarer := boltDeclarers[boltTask.TaskName]
            m.BoltCollectors[boltIndex] = outputCollector.NewOutputCollector(boltIndex, boltTask.TaskName, m.OutputDispatcher.Queue)

            bolt := (*boltDeclarer.GetBolt()).Clone()
            (*bolt).Prepare(m.BoltCollectors[boltIndex])

            boltExecutor := &boltExecutor.BoltExecutor {
                Bolt: bolt,
                TaskQueue: m.BoltTaskQueues[boltIndex],
            }
            m.BoltExecutors[boltIndex] = boltExecutor
        }
    }
}

func (m *Manager) JoinPresident()  {
    args = commandUtil.AcquireArgs()
    defer commandUtil.ReleaseArgs(args)

    context := m.SelfContext
    b, err := json.Marshal(context)
    if err != nil {
        log.Error("err:", err)
    }
    args.SetBytesV("context", b)
    destAddress := m.PresidentAddress + "/onJoin"
    statusCode, _, err := client.Post(nil, destAddress, args)
    if err != nil || statusCode != 200 {
        log.Error("err:%s,statusCode: %s", err, statusCode)
    }
}
