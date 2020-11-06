package varlogpb

import "github.com/kakao/varlog/pkg/types"

func (snmeta StorageNodeMetadataDescriptor) FindLogStream(logStreamID types.LogStreamID) (LogStreamMetadataDescriptor, bool) {
	for _, lsmeta := range snmeta.GetLogStreams() {
		if lsmeta.GetLogStreamID() == logStreamID {
			return lsmeta, true
		}
	}
	return LogStreamMetadataDescriptor{}, false
}
