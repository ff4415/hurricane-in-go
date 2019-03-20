package boltDeclarer

import (
    "taskDeclarer"
    "iBolt"
)

type BoltDeclarer struct {
    taskDeclarer.TaskDeclarer
    GroupField string
    bolt *iBolt.IBolt
    fields []string
    fieldsMap map[string]int
}

func NewBoltDeclarer(boltName string, bolt *iBolt.IBolt) *BoltDeclarer  {
    bd = new(BoltDeclarer)
    bd.Type = taskDeclarer.Bolt
    bd.TaskName = boltName
    bd.fields = bolt.DeclareFields()
    bd.fieldsMap = make(map[string]int)
    // bd := BoltDeclarer {
    //     Type: taskDeclarer.Bolt,
    //     TaskName: boltName,
    //     fields: bolt.DeclareFields(),
    //     fieldsMap: make(map[string]int)
    // }
    for fieldIndex, field := range bd.fields {
        bd.fieldsMap[field] = fieldIndex
    }
    return bd
}

func (bd *BoltDeclarer) GetBolt() *iBolt.IBolt {
    return bd.bolt
}

func (bd *BoltDeclarer) GetFields() []string {
    return bd.fields
}

func (bd *BoltDeclarer) GetFieldsMap() map[string]int {
    return bd.fieldsMap
}

func (bd *BoltDeclarer) ParallismHint(parallismHint int)  {
    bd.ParallismHint = parallismHint
}

func (bd *BoltDeclarer) Global(sourceTaskName string)  {
    bd.SourceTaskName = sourceTaskName
    bd.GroupMethod = taskDeclarer.Global
}

func (bd *BoltDeclarer) Field(sourceTaskName string, groupField string)  {
    bd.SourceTaskName = sourceTaskName
    bd.GroupMethod = taskDeclarer.Field
    bd.GroupField = groupField
}

func (bd *BoltDeclarer) Random(sourceTaskName string)  {
    bd.SourceTaskName = sourceTaskName
    bd.GroupMethod = taskDeclarer.Random
}
