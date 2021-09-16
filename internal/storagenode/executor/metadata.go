package executor

import (
	"github.com/kakao/varlog/internal/storagenode/storage"
	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/proto/snpb"
	"github.com/kakao/varlog/proto/varlogpb"
)

type MetadataProvider interface {
	Metadata() varlogpb.LogStreamMetadataDescriptor
	Path() string
	GetPrevCommitInfo(ver types.Version) (*snpb.LogStreamCommitInfo, error)
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
		HighWatermark: e.lsc.localGLSN.localHighWatermark.Load(),
		Status:        status,
		Path:          e.storage.Path(),
		CreatedTime:   e.tsp.Created(),
		UpdatedTime:   e.tsp.LastUpdated(),
	}
}

func (e *executor) GetPrevCommitInfo(ver types.Version) (*snpb.LogStreamCommitInfo, error) {
	info := &snpb.LogStreamCommitInfo{
		LogStreamID:        e.logStreamID,
		HighestWrittenLLSN: e.lsc.uncommittedLLSNEnd.Load() - 1,
	}

	cc, err := e.storage.ReadFloorCommitContext(ver)
	switch err {
	case storage.ErrNotFoundCommitContext:
		info.Status = snpb.GetPrevCommitStatusNotFound
		return info, nil
	case storage.ErrInconsistentCommitContext:
		info.Status = snpb.GetPrevCommitStatusInconsistent
		return info, nil
	default:
		info.Status = snpb.GetPrevCommitStatusOK
	}

	info.CommittedLLSNOffset = cc.CommittedLLSNBegin
	info.CommittedGLSNOffset = cc.CommittedGLSNBegin
	info.CommittedGLSNLength = uint64(cc.CommittedGLSNEnd - cc.CommittedGLSNBegin)
	info.Version = cc.Version
	return info, nil
}
