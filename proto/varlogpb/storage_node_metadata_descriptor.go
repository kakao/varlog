package varlogpb

import "github.daumkakao.com/varlog/varlog/pkg/varlog/types"

func (snmeta StorageNodeMetadataDescriptor) FindLogStream(logStreamID types.LogStreamID) (LogStreamMetadataDescriptor, bool) {
	for _, lsmeta := range snmeta.GetLogStreams() {
		if lsmeta.GetLogStreamID() == logStreamID {
			return lsmeta, true
		}
	}
	return LogStreamMetadataDescriptor{}, false
}
