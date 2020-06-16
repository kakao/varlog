package varlog

import "errors"

var (
	ErrUnwrittenLogEntry = errors.New("unwritten log entry")
	ErrWrittenLogEntry   = errors.New("already written log entry")
	ErrTrimmedLogEntry   = errors.New("already trimmed log entry")

	ErrInvalidProjection = errors.New("invalid projection")
)
