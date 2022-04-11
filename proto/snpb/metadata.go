package snpb

import (
	"github.com/kakao/varlog/pkg/types"
)

func (snmd *StorageNodeMetadataDescriptor) GetLogStream(logStreamID types.LogStreamID) (LogStreamReplicaMetadataDescriptor, bool) {
	logStreams := snmd.GetLogStreamReplicas()
	for i := range logStreams {
		if logStreams[i].GetLogStreamID() == logStreamID {
			return logStreams[i], true
		}
	}
	return LogStreamReplicaMetadataDescriptor{}, false
}

func (snmd StorageNodeMetadataDescriptor) FindLogStream(logStreamID types.LogStreamID) (LogStreamReplicaMetadataDescriptor, bool) {
	for _, lsmeta := range snmd.GetLogStreamReplicas() {
		if lsmeta.GetLogStreamID() == logStreamID {
			return lsmeta, true
		}
	}
	return LogStreamReplicaMetadataDescriptor{}, false
}
