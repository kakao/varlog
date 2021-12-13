package varlogpb

import "github.com/kakao/varlog/pkg/types"

func (snmd StorageNodeMetadataDescriptor) FindLogStream(logStreamID types.LogStreamID) (LogStreamMetadataDescriptor, bool) {
	for _, lsmeta := range snmd.GetLogStreams() {
		if lsmeta.GetLogStreamID() == logStreamID {
			return lsmeta, true
		}
	}
	return LogStreamMetadataDescriptor{}, false
}

func (snd *StorageNodeDescriptor) Valid() bool {
	if snd == nil ||
		len(snd.Address) == 0 ||
		len(snd.Storages) == 0 {
		return false
	}

	for _, storage := range snd.Storages {
		if !storage.valid() {
			return false
		}
	}

	return true
}
