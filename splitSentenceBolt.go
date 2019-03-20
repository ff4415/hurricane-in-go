package iBolt

import(
    "string"
    "io"
    "stringconv"
    "tuple"
    "outputCollector"
)

type SplitSentenceBolt struct {
    outputCollector: outputCollector.OutputCollector
}

func (s *SplitSentenceBolt) Clone() *iBolt  {
    return &SplitSentenceBolt{}
}

func (s *SplitSentenceBolt) Prepare(outputCollector *outputCollector.OutputCollector)  {
    s.outputCollector = &outputCollector
}

func (s *SplitSentenceBolt) DeclareFields() []string {
    return []{ "word" }
}

func (s *SplitSentenceBolt) Execute(tuple *tuple.Tuple)  {
    sentence := tuple.Values
    words := string.Split(sentence, " ")

    for word := range words {
        s.outputCollector.Emit(&tuple{
            values: word
        })
    }
}

type WordCountBolt struct {
    outputCollector: outputCollector.OutputCollector
    wordcounts: map[string]int
    logFile: io.Writer
}

func (w *WordCountBolt) Clone() *iBolt  {
    return &WordCountBolt{}
}

func (w *WordCountBolt) Prepare(outputCollector *outputCollector.OutputCollector)  {
    w.outputCollector = &outputCollector
    logFile: os.Stdout
    wordcounts: make(map[string]int)
}

func (w *WordCountBolt) Cleanup()  {
    logFile.Close()
}

func (w *WordCountBolt) DeclareFields() []string {
    return []{ "word", "count" }
}

func (w *WordCountBolt) Execute(tuple *tuple.Tuple)  {
    word := tuple.Values
    if _, !ok := wordcounts[word] {
        wordcounts[word] = 0
        wordcount = wordcounts[word]
    }

    ++wordcounts
    b := string.join([]string{
        word,
        strconv.Itoa(wordcount)
    }, ",")
    w.outputCollector.Emit(&tuple{
        Values: b
    })

}
