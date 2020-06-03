package varlog

import "errors"

var (
	ErrInvalidEpoch      = errors.New("invalid epoch")
	ErrUnwrittenLogEntry = errors.New("unwritten log entry")
	ErrWrittenLogEntry   = errors.New("already written log entry")
	ErrSealedEpoch       = errors.New("sealed epoch")
	ErrTrimmedLogEntry   = errors.New("already trimmed log entry")

	ErrInvalidProjection = errors.New("invalid projection")
)
