package varlog

import (
	"errors"

	"github.daumkakao.com/varlog/varlog/pkg/types"
)

var (
	errNoLogStream = errors.New("no such logstream")
)

// LogStreamSelector is the interface that wraps the Select method.
//
// Select selects a log stream, but if there is no log stream to choose it returns false.
type LogStreamSelector interface {
	Select() (types.LogStreamID, bool)
}

// alsSelector implements LogStreamSelector. It uses allowlist to select an appendable log stream.
type alsSelector struct {
	allowlist Allowlist
}

var _ LogStreamSelector = (*alsSelector)(nil)

func newAppendableLogStreamSelector(allowlist Allowlist) *alsSelector {
	return &alsSelector{allowlist: allowlist}
}

// Select implements (LogStreamSelector).Select method.
func (als *alsSelector) Select() (types.LogStreamID, bool) {
	return als.allowlist.Pick()
}
