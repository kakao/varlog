package varlog

import (
	"errors"

	"github.com/kakao/varlog/pkg/types"
)

var (
	errNoLogStream = errors.New("no such logstream")
)

// LogStreamSelector is the interface that wraps the Select method.
//
// Select selects a log stream, but if there is no log stream to choose it returns false.
// GetAll returns all log steams.
type LogStreamSelector interface {
	Select(topicID types.TopicID) (types.LogStreamID, bool)
	GetAll(topicID types.TopicID) []types.LogStreamID
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
func (als *alsSelector) Select(topicID types.TopicID) (types.LogStreamID, bool) {
	return als.allowlist.Pick(topicID)
}

// GetAll implements (LogStreamSelector).GetAll method.
func (als *alsSelector) GetAll(topicID types.TopicID) []types.LogStreamID {
	return als.allowlist.GetAll(topicID)
}
