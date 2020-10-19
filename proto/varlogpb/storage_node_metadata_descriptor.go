package varlogpb

import "github.com/kakao/varlog/pkg/varlog/types"

func (snmeta StorageNodeMetadataDescriptor) FindLogStream(logStreamID types.LogStreamID) (LogStreamMetadataDescriptor, bool) {
	lsmetaList := snmeta.GetLogStreams()
	for _, lsmeta := range lsmetaList {
		if lsmeta.GetLogStreamID() == logStreamID {
			return lsmeta, true
		}
	}
	return LogStreamMetadataDescriptor{}, false
}
