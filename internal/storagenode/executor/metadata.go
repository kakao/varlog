package executor

import (
	"github.com/kakao/varlog/proto/varlogpb"
)

type MetadataProvider interface {
	Metadata() varlogpb.LogStreamMetadataDescriptor
	Path() string
}

func (e *executor) Path() string {
	return e.storage.Path()
}

func (e *executor) Metadata() varlogpb.LogStreamMetadataDescriptor {
	var status varlogpb.LogStreamStatus
	switch e.stateBarrier.state.load() {
	case executorMutable:
		status = varlogpb.LogStreamStatusRunning
	case executorSealing:
		status = varlogpb.LogStreamStatusSealing
	case executorSealed:
		status = varlogpb.LogStreamStatusSealed
	}
	return varlogpb.LogStreamMetadataDescriptor{
		StorageNodeID: e.storageNodeID,
		LogStreamID:   e.logStreamID,
		Status:        status,
		Path:          e.storage.Path(),
		CreatedTime:   e.tsp.Created(),
		UpdatedTime:   e.tsp.LastUpdated(),
	}
}
