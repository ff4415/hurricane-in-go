package topology

import (
    "boltDeclarer"
    "spoutDeclarer"
    "sync"
)

type Topology struct {
    Name string
    SpoutDeclarers map[string]*spoutDeclarer.SpoutDeclarer
    BoltDeclarers map[string]*boltDeclarer.BoltDeclarer
}

type TopologyLoader struct {
    Topologies map[string]*Topology
    // LibraryHandles map[string]func(string)
}

var topologyLoader *TopologyLoader

func GetTopologyLoader() *TopologyLoader  {
    return topologyLoader
}

func SetTopologyLoader(topologyName string, topology *Topology)  {
    sync.Once.Do(func ()  {
        if !topologyLoader {
            topologyLoader = &TopologyLoader {
                Topologies: map[string]*Topology{
                    topologyName: topology
                }
            }
        }
    })
}
