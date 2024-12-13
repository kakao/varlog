package snpb

import (
	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/proto/varlogpb"
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

func (lsrmd *LogStreamReplicaMetadataDescriptor) Head() varlogpb.LogEntryMeta {
	if lsrmd == nil {
		return varlogpb.LogEntryMeta{}
	}
	return varlogpb.LogEntryMeta{
		TopicID:     lsrmd.TopicID,
		LogStreamID: lsrmd.LogStreamID,
		GLSN:        lsrmd.LocalLowWatermark.GLSN,
		LLSN:        lsrmd.LocalLowWatermark.LLSN,
	}
}

func (lsrmd *LogStreamReplicaMetadataDescriptor) Tail() varlogpb.LogEntryMeta {
	if lsrmd == nil {
		return varlogpb.LogEntryMeta{}
	}
	return varlogpb.LogEntryMeta{
		TopicID:     lsrmd.TopicID,
		LogStreamID: lsrmd.LogStreamID,
		GLSN:        lsrmd.LocalHighWatermark.GLSN,
		LLSN:        lsrmd.LocalHighWatermark.LLSN,
	}
}
