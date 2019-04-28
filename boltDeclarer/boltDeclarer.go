package boltDeclarer

import (
    "github.com/ff4415/hurricane-in-go/taskDeclarer"
    "github.com/ff4415/hurricane-in-go/iBolt"
    "github.com/ff4415/hurricane-in-go/taskType"
    "github.com/ff4415/hurricane-in-go/groupMethodType"
)

type BoltDeclarer struct {
    taskDeclarer.TaskDeclarer
    GroupField string
    bolt *iBolt.IBolt
    fields []string
    fieldsMap map[string]int
}

func NewBoltDeclarer(boltName string, bolt iBolt.IBolt) *BoltDeclarer  {
  bd := &BoltDeclarer{}
  bd.Type = taskType.Bolt
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

func (bd *BoltDeclarer) SetParallismHint(ph int)  {
  bd.ParallismHint = ph
}

func (bd *BoltDeclarer) Global(sourceTaskName string)  {
  bd.SourceTaskName = sourceTaskName
  bd.GroupMethod = groupMethodType.Global
}

func (bd *BoltDeclarer) Field(sourceTaskName string, groupField string)  {
  bd.SourceTaskName = sourceTaskName
  bd.GroupMethod = groupMethodType.Field
  bd.GroupField = groupField
}

func (bd *BoltDeclarer) Random(sourceTaskName string)  {
  bd.SourceTaskName = sourceTaskName
  bd.GroupMethod = groupMethodType.Random
}
