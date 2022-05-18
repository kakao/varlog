package snpb

import (
	"github.daumkakao.com/varlog/varlog/pkg/types"
	"github.daumkakao.com/varlog/varlog/proto/varlogpb"
)

func (snmd *StorageNodeMetadataDescriptor) ToStorageNodeDescriptor() *varlogpb.StorageNodeDescriptor {
	if snmd == nil {
		return nil
	}
	snd := &varlogpb.StorageNodeDescriptor{
		StorageNode: snmd.StorageNode,
		Paths:       make([]string, len(snmd.Storages)),
	}
	for i := range snmd.Storages {
		snd.Paths[i] = snmd.Storages[i].Path
	}
	return snd
}

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
