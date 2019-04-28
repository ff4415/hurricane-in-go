package topology

import (
  "strings"
  "github.com/ff4415/hurricane-in-go/outputCollector"
  "github.com/ff4415/hurricane-in-go/tuple"
  "github.com/ff4415/hurricane-in-go/iSpout"
  "math/rand"
  "time"
)

var r = rand.New(rand.NewSource(time.Now().UnixNano()))

type HelloWorldSpout struct {
  outputCollector *outputCollector.OutputCollector
  words []string
}

func (h *HelloWorldSpout) Prepare(oc *outputCollector.OutputCollector)  {
  h.outputCollector = oc
  h.words = strings.Split("Hello world there are some words we generate new sentence randomly", " ")
}

func (h *HelloWorldSpout) Cleanup() { }

func (h *HelloWorldSpout) Clone() iSpout.ISpout  {
  // hws := new(HelloWorldSpout)
  // return &(iSpout.ISpout(*hws))
  return &HelloWorldSpout{}
}

func (h *HelloWorldSpout) DeclareFields() []string {
  return []string{
    "sentence",
  }
}

func (h *HelloWorldSpout) NextTuple()  {
   t := make([]string, 5)

  for index:= 0; index < 5; index++ {
    t[index] = h.words[r.Intn(len(h.words))]
  }
  b := strings.Join(t, " ")
  h.outputCollector.Emit(&tuple.Tuple{
    Values: []byte(b),
  })
}
