package manager

import (
    "commandUtil"
    "managerContext"
    "spoutExecutor"
    "boltExecutor"
    "outputCollector"
    "outputDispatcher"
    "taskItem"
    "outputItem"
    "topology"
    "configure"
    "commandUtil"
    "command"
    "iSpout"
    "iBolt"
    "sync"
    "encoding/json"
)

type Manager struct {
    Name string
    Host string
    ManagerConfiguration configuration.Configuration
    PresidentAddress string
    SelfContext *managerContext.ManagerContext
    SpoutExecutors []*spoutExecutor.SpoutExecutor
    BoltExecutors []*boltExecutor.BoltExecutor
    SpoutCollectors []*outputCollector.OutputCollector
    BoltCollectors []*outputCollector.OutputCollector
    BoltTaskQueues []chan taskItem.TaskItem
    Topology topology.Topology
    OutputDispatcher *outputDispatcher.OutputDispatcher
    TaskFields map[string][]string
    TaskFieldsMap map[string]map[string]int
}

var args commandUtil.Args
var client commandUtil.Client
var sever commandUtil.Server

var manager *Manager

func GetManager(configure string) *Manager  {
    sync.Once.Do(func ()  {
        manager = &manager {
            // ManagerConfiguration: configure.InitConfig("/db_configuration")
            ManagerConfiguration: configure.InitConfig(configure)
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
    m.SelfContext = managerContext.ManagerContext{
        Id: m.Name,
        netAddress: m.Host,
        SpoutCount: m.managerConfiguration["CONF_KEY_SPOUT_COUNT"],
        BoltCount: m.managerConfiguration["CONF_KEY_BOLT_COUNT"],
        SpoutTaskInfos: make([]*managerContext.TaskInfo, m.managerConfiguration["CONF_KEY_SPOUT_COUNT"]),
        BoltTaskInfos: make([]*managerContext.TaskInfo, m.managerConfiguration["CONF_KEY_BOLT_COUNT"]),
        Spouts: make(map[int]bool),
        Bolts: make(map[int]bool),
    }
    for index := 0; index < m.SpoutCount; index++ {
        m.Spouts[index] = false
    }
    for index := 0; index < m.BoltCount; index++ {
        m.Bolts[index] = false
    }
}

func (m *Manager) reserveExecutors()  {
    m.SpoutExecutors = make([]*spoutExecutor.SpoutExecutor, m.managerConfiguration["CONF_KEY_SPOUT_COUNT"])
    m.boltExecutors = make([]*boltExecutor.BoltExecutor, m.managerConfiguration["CONF_KEY_BOLT_COUNT"])
    m.SpoutCollectors = make([]*outputCollector.OutputCollector, m.managerConfiguration["CONF_KEY_SPOUT_COUNT"])
    m.BoltCollectors = make([]*outputCollector.OutputCollector, m.managerConfiguration["CONF_KEY_BOLT_COUNT"])
    m.BoltTaskQueues = make([]chan taskItem.TaskItem, m.managerConfiguration["CONF_KEY_BOLT_COUNT"])
    m.OutputDispatcher = outputDispatcher.OutputDispatcher {
        SelfAddress: m.managerConfiguration["CONF_KEY_MANAGER_HOST"],
        PresidentAddress: m.managerConfiguration["CONF_KEY_PRESIDENT_HOST"],
        Queue: chan outputItem.OutputItem,
        SelfAddress: m.ManagerConfiguration["CONF_KEY_MANAGER_HOST"],
        SelfTasks: m.BoltTaskQueues,
        SelfSpoutCount: m.managerConfiguration["CONF_KEY_SPOUT_COUNT"],
        SelfBoltCount: m.managerConfiguration["CONF_KEY_BOLT_COUNT"],
    }
    m.OutputDispatcher.Start()
}

func (m *Manager) initEvents()  {
    server = commandUtil.GetServer()

    server.Name = m.ManagerConfiguration["CONF_KEY_MANAGER_NAME"]
    server.Handler = m.requestHandler()

    if err := server.ListenAndServe(m.ManagerConfiguration["CONF_KEY_MANAGER_HOST"]); err != nil {
        panic("error in ListenAndServe: %s", err)
    }
}

func (m *manager) requestHandler(ctx *commandUtil.RequestCtx)  {

    args = commandUtil.Args.AcquireArgs()
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

func (m *Manager) onHeartbeat(ctx *commandUtil.RequestCtx)  {
    ctx.SetStatusCode(commandUtil.StatusOK)
    ctx.SetBody([]byte("%s is online.", m.name))
}

func (m *Manager) onSyncMetadata(ctx *commandUtil.RequestCtx)  {
    var metaData managerContext.ManagerContext
    err := json.Unmarshal(ctx.PostArgs(), &metaData)
    if err != nil {
        ctx.SetStatusCode(commandUtil.StatusOK)
        ctx.SetBody([]byte("error in json Unmarshal: %s", err))
        return
    }
    m.selfContext = metadata

    // m.ownManagerTasks()
    m.outputDispatcher.SpoutTaskInfos = &m.selfContext.SpoutTaskInfos
    m.outputDispatcher.BoltTaskInfos = &m.selfContext.BoltTaskInfos

    // m.showManagerMetadata();
    // m.showTaskInfos();

    m.topology = topology.GetTopologyLoader().Topologies[m.ManagerConfiguration["CONF_KEY_TOPOLOGY_NAME"]]

    m.initTaskFieldsMap();
    m.initExecutors();

    ctx.SetStatusCode(commandUtil.StatusOK)
    ctx.SetBody([]byte("SyncMetadata OK."))
}


func (m *Manager) onSendTuple(ctx *commandUtil.RequestCtx)  {
    var context command.CommandManager
    err := json.Unmarshal(ctx.PostArgs(), &context)
    if err != nil {
        ctx.SetStatusCode(commandUtil.StatusOK)
        ctx.SetBody([]byte("error in json Unmarshal: %s", err))
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
    spoutDeclarers = m.Topology.SpoutDeclarers

    for _, spoutDeclarer := range spoutDeclarers {
        m.TaskFields[spoutDeclarer.TaskName] = &spoutDeclarer.GetFields()
        m.taskFieldsMap[spoutDeclarer.TaskName] = spoutDeclarer.GetFieldsMap()
    }

    boltDeclarers = m.Topology.BoltDeclarers

    for _, boltDeclarer := range boltDeclarers {
        m.taskFields[boltDeclarer.TaskName] = &boltDeclarer.GetFields()
        m.taskFieldsMap[boltDeclarer.TaskName] = boltDeclarer.GetFieldsMap()
    }
    m.OutputDispatcher.TaskFields = m.taskFields
    m.outputDispatcher.TaskFieldsMap = m.taskFieldsMap
}

func (m *Manager) initExecutors()  {
    m.initSpoutExecutors()
    m.initBoltExecutors()

    bolts := m.selfContext.Bolts
    spouts := m.selfContext.Spouts

    for index := 0; index < m.selfContext.SpoutCount; index++ {
            if bolt[index] == true {
                m.SpoutExecutors[index].Start()
            }
    }

    for index := 0; index < m.selfContext.BoltCount; index++ {
            if bolt[index] == true {
                m.BoltExecutors[index].Start()
            }
    }
}

func (m *Manager) initSpoutExecutors()  {
    log("Init spout executors")
    spoutDeclarers := m.Topology.SpoutDeclarers
    spouts := m.SelfContext.Spouts
    for spoutIndex, _ := range spouts {
        if spouts[spoutIndex] == true {
            spoutTask := m.SelfContext.SpoutTaskInfos[spoutIndex]
            spoutDeclarer := spoutDeclarers[spoutTask.TaskName]
            m.SpoutCollectors[spoutIndex] = outputCollector.NewOutputCollector(spoutIndex, spoutTask.TaskName, m.OutputDispatcher.Queue)

            spout := &spoutDeclarer.GetSpout().Clone()
            spout.Prepare(m.SpoutCollectors[spoutIndex])

            spoutExecutor := &spoutExecutor.SpoutExecutor {
                Spout: spout
            }
            m.SpoutExecutors[spoutIndex] = spoutExecutor
        }
    }
}

func (m *Manager) initBoltExecutors()  {
    log("Init bolt executors")
    boltDeclarers := m.Topology.BoltDeclarers
    bolts := m.SelfContext.Bolts
    for boltIndex, _ := range bolts {
        if bolts[boltIndex] == true {
            boltTask := m.SelfContext.BoltTaskInfos[boltIndex]
            boltDeclarer := boltDeclarers[boltTask.TaskName]
            m.BoltCollectors[boltIndex] = outputCollector.NewOutputCollector(boltIndex, boltTask.TaskName, m.OutputDispatcher.Queue)

            bolt := &boltDeclarer.GetBolt().Clone()
            bolt.Prepare(m.BoltCollectors[boltIndex])

            boltExecutor := &boltExecutor.BoltExecutor {
                Bolt: bolt,
                TaskQueue: m.BoltTaskQueues[boltIndex]
            }
            m.BoltExecutors[boltIndex] = boltExecutor
        }
    }
}

func (m *Manager) JoinPresident()  {
    args = commandUtil.Args.AcquireArgs()
    defer commandUtil.ReleaseArgs(args)

    context := m.SelfContext
    b, err := json.Marshal(context)
    if err != nil {
        log("err:", err)
    }
    args.SetBytesV("context", b)
    destAddress := m.presidentAddress + "/onJoin"
    statusCode, body, err := client.Post(nil, destAddress, args)
    if err != nil || statusCode != 200 {
        log("err:%s,statusCode: %s", err, statusCode)
    }
}
