package iSpout

import (
    "string"
    "outputCollector"
    "tuple"
)

rand := rand.New(rand.NewSource(time.Now().UnixNano()))

type HelloWorldSpout struct {
    words []string
    outputCollector *outputCollector.OutputCollector
}

func (h *HelloWorldSpout) Clone() *iSpout  {
    return &HelloWorldSpout{}
}

func (h *HelloWorldSpout) Prepare(outputCollector *outputCollector.OutputCollector)  {
    h.outputCollector = outputCollector
    h.words = string.Split("Hello world there are some words we generate new sentence randomly"," ")
}

func (h *HelloWorldSpout) Cleanup()  {

}

func (h *HelloWorldSpout) DeclareFields() []string {
    return []{ "sentence" }
}

func (h *HelloWorldSpout) NextTuple()  {
    var words [5]string
    for i := 0; i < 5; i++ {
        words[i] = h.words[rand.Intn(5)]
    }
    sentence = string.Join(words, " ")
    h.outputCollector.Emit(&tuple.Tuple{
        Values: sentence,
        Field: []string{"sentence"}
    })
}
