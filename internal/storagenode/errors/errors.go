package errors

import stderrors "errors"

var (
	ErrNotPrimary      = stderrors.New("not primary replica")
	ErrClosed          = stderrors.New("closed")
	ErrTooManyReplicas = stderrors.New("too many log stream replicas")
	ErrNotExist        = stderrors.New("not exist")
)
