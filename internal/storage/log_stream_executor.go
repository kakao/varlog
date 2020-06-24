package storage

import (
	"github.com/kakao/varlog/pkg/varlog/types"
)

type LLSNRange struct {
	Begin types.LLSN
	End   types.LLSN
}

type LogStreamExecutor interface {
	GetLogStreamStatus() UncommittedLogStreamStatus
	CommitLogStreamStatusResult(CommittedLogStreamStatus)
}

type logStreamExecutor struct {
	storage Storage
}

func NewLogStreamExecutor() LogStreamExecutor {
	return &logStreamExecutor{}
}

func (e *logStreamExecutor) GetLogStreamStatus() UncommittedLogStreamStatus {
	panic("not yet implemented")
}

func (e *logStreamExecutor) CommitLogStreamStatusResult(_ CommittedLogStreamStatus) {
	panic("not yet implemented")
}
