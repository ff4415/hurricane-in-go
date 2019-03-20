package main

import(
    "president"
    "configure"
    "topology"
    "boltDeclarer"
    "spoutDeclarer"
    "iSpout"
    "iBolt"
)

func main()  {
    configure := "/db_configuration"
    topo := topology.Topology{
        Name: "word-count-topology",
        SpoutDeclarers: map[string]*spoutDeclarer.SpoutDeclarer{
            "hello-world-spout": spoutDeclarer.NewSpoutDeclarer("hello-world-spout", &iSpout.HelloWorldSpout{})
        },
        BoltDeclarers: map[string]*boltDeclarer..BoltDeclarer{
            "split-sentence-bolt": boltDeclarer.NewBoltDeclarer("split-sentence-bolt", &iBolt.SplitSentenceBolt{}),
            "word-count-bolt": boltDeclarer.NewBoltDeclarer("word-count-bolt", &iBolt.WordCountBolt{})
        }
    }
    topo.SpoutDeclarers["hello-world-spout"].ParallismHint(1)
    topo.BoltDeclarers["split-sentence-bolt"].Random("hello-world-spout").ParallismHint(3)
    topo.BoltDeclarers["word-count-bolt"].Field("split-sentence-bolt", "word").ParallismHint(2)
    topology.SetTopologyLoader(topo)
    president := president.GetPresident(configure)
}
