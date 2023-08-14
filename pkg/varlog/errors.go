package varlog

import "errors"

var (
	ErrClosed      = errors.New("client: closed")
	ErrCallTimeout = errors.New("client: call timeout")
)
