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
	version, _, _ := e.lsc.reportCommitBase()
	return varlogpb.LogStreamMetadataDescriptor{
		StorageNodeID: e.storageNodeID,
		LogStreamID:   e.logStreamID,
		TopicID:       e.topicID,
		Version:       version,
		HighWatermark: e.lsc.localHighWatermark().GLSN,
		Status:        status,
		Path:          e.storage.Path(),
		CreatedTime:   e.tsp.Created(),
		UpdatedTime:   e.tsp.LastUpdated(),
	}
}

func (e *executor) LogStreamMetadata() (lsd varlogpb.LogStreamDescriptor, err error) {
	if err := e.guard(); err != nil {
		return lsd, err
	}
	defer e.unguard()

	var status varlogpb.LogStreamStatus
	switch e.stateBarrier.state.load() {
	case executorMutable:
		status = varlogpb.LogStreamStatusRunning
	case executorSealing:
		status = varlogpb.LogStreamStatusSealing
	case executorSealed:
		status = varlogpb.LogStreamStatusSealed
	}

	localLWM := e.lsc.localLowWatermark()
	localLWM.TopicID = e.topicID
	localLWM.LogStreamID = e.logStreamID

	localHWM := e.lsc.localHighWatermark()
	localHWM.TopicID = e.topicID
	localHWM.LogStreamID = e.logStreamID

	lsd = varlogpb.LogStreamDescriptor{
		TopicID:     e.topicID,
		LogStreamID: e.logStreamID,
		Status:      status,
		Head:        localLWM,
		Tail:        localHWM,
	}
	return lsd, nil
}
