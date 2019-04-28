package topology

import (
    "github.com/ff4415/hurricane-in-go/boltDeclarer"
    "github.com/ff4415/hurricane-in-go/spoutDeclarer"
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
    var once sync.Once
    once.Do(func ()  {
        if topologyLoader == nil {
            topologyLoader = &TopologyLoader {
                Topologies: map[string]*Topology{
                    topologyName: topology,
                },
            }
        }
    })
}
