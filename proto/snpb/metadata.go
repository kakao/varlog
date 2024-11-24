package snpb

import (
	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/proto/varlogpb"
)

// ToStorageNodeDescriptor converts a StorageNodeMetadataDescriptor to a
// varlogpb.StorageNodeDescriptor. It returns nil if the
// StorageNodeMetadataDescriptor is nil.
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

// GetLogStream retrieves a LogStreamReplicaMetadataDescriptor by its
// LogStreamID. It returns the LogStreamReplicaMetadataDescriptor and true if
// found, otherwise an empty descriptor and false.
func (snmd *StorageNodeMetadataDescriptor) GetLogStream(logStreamID types.LogStreamID) (LogStreamReplicaMetadataDescriptor, bool) {
	logStreams := snmd.GetLogStreamReplicas()
	for i := range logStreams {
		if logStreams[i].GetLogStreamID() == logStreamID {
			return logStreams[i], true
		}
	}
	return LogStreamReplicaMetadataDescriptor{}, false
}

// Head returns the varlogpb.LogEntryMeta corresponding to the local low
// watermark of the LogStreamReplicaMetadataDescriptor. The "head" represents
// the earliest log entry in the log stream replica. It returns an empty
// varlogpb.LogEntryMeta if the LogStreamReplicaMetadataDescriptor is nil.
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

// Tail returns the varlogpb.LogEntryMeta corresponding to the local high
// watermark of the LogStreamReplicaMetadataDescriptor. The "tail" represents
// the latest log entry in the log stream replica. It returns an empty
// varlogpb.LogEntryMeta if the LogStreamReplicaMetadataDescriptor is nil.
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
