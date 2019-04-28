package topology

import(
    "strings"
    "strconv"
    "github.com/ff4415/hurricane-in-go/tuple"
    "github.com/ff4415/hurricane-in-go/outputCollector"
    "github.com/Sirupsen/logrus"
    // "os"
    "github.com/ff4415/hurricane-in-go/iBolt"
)

type SplitSentenceBolt struct {
    outputCollector *outputCollector.OutputCollector
}

func (s SplitSentenceBolt) Clone() iBolt.IBolt  {
    return new(SplitSentenceBolt)
}

func (s SplitSentenceBolt) Prepare(outputCollector *outputCollector.OutputCollector)  {
    s.outputCollector = outputCollector
}

func (s SplitSentenceBolt) DeclareFields() []string {
    return []string{"word"}
}

func (s SplitSentenceBolt) Execute(t *tuple.Tuple)  {
    sentence := string(t.Values)
    words := strings.Split(sentence, " ")

    for _, word := range words {
        s.outputCollector.Emit(&tuple.Tuple{
            Values: []byte(word),
        })
    }
}
func (s SplitSentenceBolt) Cleanup()  { }

type WordCountBolt struct {
    outputCollector *outputCollector.OutputCollector
    wordcounts map[string]int
    logFile *logrus.Logger
}

func (w WordCountBolt) Clone() iBolt.IBolt  {
    return &WordCountBolt{}
}

func (w WordCountBolt) Prepare(outputCollector *outputCollector.OutputCollector)  {
    w.outputCollector = outputCollector
    w.logFile = logrus.New()
    // w.logFile.Out = os.Stdout
    w.wordcounts = make(map[string]int)
}

func (w WordCountBolt) Cleanup()  {

}

func (w WordCountBolt) DeclareFields() []string {
    return []string{ "word", "count" }
}

func (w WordCountBolt) Execute(t *tuple.Tuple)  {
    word := string(t.Values)
    if _, ok := w.wordcounts[word]; !ok {
      w.wordcounts[word] = 0
    }

    w.wordcounts[word]++
    b := strings.Join([]string{
        word,
        strconv.Itoa(w.wordcounts[word]),
    }, ",")
    w.outputCollector.Emit(&tuple.Tuple {
        Values: []byte(b),
    })

}
