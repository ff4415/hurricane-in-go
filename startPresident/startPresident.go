package main

import(
    "github.com/ff4415/hurricane-in-go/president"
    // "github.com/ff4415/hurricane-in-go/configure"
    "github.com/ff4415/hurricane-in-go/topology"
    "github.com/ff4415/hurricane-in-go/boltDeclarer"
    "github.com/ff4415/hurricane-in-go/spoutDeclarer"
    // "github.com/ff4415/hurricane-in-go/iSpout"
    // "github.com/ff4415/hurricane-in-go/iBolt"
)

func main()  {
    configure := "/db_configuration"
    topo := topology.Topology{
        Name: "word-count-topology",
        SpoutDeclarers: map[string]*spoutDeclarer.SpoutDeclarer{
            "hello-world-spout": spoutDeclarer.NewSpoutDeclarer("hello-world-spout", &topology.HelloWorldSpout{}),
        },
        BoltDeclarers: map[string]*boltDeclarer.BoltDeclarer{
            "split-sentence-bolt": boltDeclarer.NewBoltDeclarer("split-sentence-bolt", &topology.SplitSentenceBolt{}),
            "word-count-bolt": boltDeclarer.NewBoltDeclarer("word-count-bolt", topology.WordCountBolt{}),
        },
    }
    topo.SpoutDeclarers["hello-world-spout"].SetParallismHint(1)
    topo.BoltDeclarers["split-sentence-bolt"].Random("hello-world-spout")
    topo.BoltDeclarers["split-sentence-bolt"].SetParallismHint(3)
    topo.BoltDeclarers["word-count-bolt"].Field("split-sentence-bolt", "word")
    topo.BoltDeclarers["word-count-bolt"].SetParallismHint(2)
    topology.SetTopologyLoader("topology", &topo)
    // president := president.GetPresident(configure)
    president.GetPresident(configure)
}
