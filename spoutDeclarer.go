package spoutDeclarer

import (
    "taskDeclarer"
    "iSpout"
)

type SpoutDeclarer struct {
    taskDeclarer.TaskDeclarer
    spout *iSpout.ISpout
    fields []string
    fieldsMap map[string]int
}

func NewSpoutDeclarer(spoutName string, spout *iSpout.ISpout) *SpoutDeclarer  {
    sd = new(SpoutDeclarer)
    sd.Type = taskDeclarer.Spout
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

func (sd *SpoutDeclarer) ParallismHint(parallismHint int)  {
    sd.ParallismHint = parallismHint
}
