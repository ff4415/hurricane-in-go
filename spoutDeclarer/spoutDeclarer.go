package spoutDeclarer

import (
    "github.com/ff4415/hurricane-in-go/taskDeclarer"
    "github.com/ff4415/hurricane-in-go/iSpout"
    "github.com/ff4415/hurricane-in-go/taskType"
)

type SpoutDeclarer struct {
    taskDeclarer.TaskDeclarer
    spout *iSpout.ISpout
    fields []string
    fieldsMap map[string]int
}

func NewSpoutDeclarer(spoutName string, spout iSpout.ISpout) *SpoutDeclarer  {
    sd := &SpoutDeclarer{}
    sd.Type = taskType.Spout
    sd.TaskName = spoutName
    sd.fields = spout.DeclareFields()
    sd.fieldsMap = make(map[string]int)

    for fieldIndex, field := range sd.fields {
        sd.fieldsMap[field] = fieldIndex
    }
    return sd
}

func (sd *SpoutDeclarer) GetSpout() *iSpout.ISpout {
    return sd.spout
}

func (sd *SpoutDeclarer) GetFields() []string {
    return sd.fields
}

func (sd *SpoutDeclarer) GetFieldsMap() map[string]int {
    return sd.fieldsMap
}

func (sd *SpoutDeclarer) SetParallismHint(ph int)  {
    sd.ParallismHint = ph
}
