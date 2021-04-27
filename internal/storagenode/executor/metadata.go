package executor

import (
	"github.com/pkg/errors"

	"github.com/kakao/varlog/internal/storagenode/storage"
	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/pkg/verrors"
	"github.com/kakao/varlog/proto/snpb"
	"github.com/kakao/varlog/proto/varlogpb"
)

type MetadataProvider interface {
	Metadata() varlogpb.LogStreamMetadataDescriptor
	Path() string
	GetPrevCommitInfo(hwm types.GLSN) (*snpb.LogStreamCommitInfo, error)
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
	return varlogpb.LogStreamMetadataDescriptor{
		StorageNodeID: e.storageNodeID,
		LogStreamID:   e.logStreamID,
		Status:        status,
		Path:          e.storage.Path(),
		CreatedTime:   e.tsp.Created(),
		UpdatedTime:   e.tsp.LastUpdated(),
	}
}

func (e *executor) GetPrevCommitInfo(hwm types.GLSN) (*snpb.LogStreamCommitInfo, error) {
	if hwm == types.InvalidGLSN {
		return nil, errors.WithStack(verrors.ErrInvalid)
	}

	info := &snpb.LogStreamCommitInfo{
		LogStreamID:        e.logStreamID,
		HighestWrittenLLSN: e.lsc.uncommittedLLSNEnd.Load() - 1,
	}

	cc, err := e.storage.ReadCommitContext(hwm)
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

	lastLE, err := e.storage.Read(cc.CommittedGLSNBegin)
	if err != nil {
		return info, err
	}

	info.CommittedLLSNOffset = lastLE.LLSN
	info.CommittedGLSNOffset = cc.CommittedGLSNBegin
	info.CommittedGLSNLength = uint64(cc.CommittedGLSNEnd - cc.CommittedGLSNBegin)
	info.HighWatermark = cc.HighWatermark
	info.PrevHighWatermark = cc.PrevHighWatermark
	return info, nil
}
