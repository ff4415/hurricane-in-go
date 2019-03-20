package iSpout

import (
    "iTask"
)

type ISpout interface {
    Clone() *ISpout
    NextTuple()
}
