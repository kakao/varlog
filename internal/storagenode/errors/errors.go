package errors

import stderrors "errors"

var (
	ErrNotPrimary = stderrors.New("not primary replica")
)
