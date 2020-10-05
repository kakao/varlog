package vms

import (
	"sync/atomic"

	"github.com/kakao/varlog/pkg/varlog/types"
)
type LogStreamIDGenerator interface {
	Generate() types.LogStreamID
}

// TODO: seqLSIDGen does not consider the restart of VMS.
type seqLSIDGen struct {
	nextID uint32
}

func NewLogStreamIDGenerator() LogStreamIDGenerator {
	return &seqLSIDGen{}
}

func (gen *seqLSIDGen) Generate() types.LogStreamID {
	id := atomic.AddUint32(&gen.nextID, 1)
	return types.LogStreamID(id)
}
