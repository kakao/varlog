package snpb

import (
	"fmt"

	"github.com/kakao/varlog/pkg/types"
)

func ValidateTopicLogStream(iface interface {
	GetTopicID() types.TopicID
	GetLogStreamID() types.LogStreamID
},
) error {
	if tpid := iface.GetTopicID(); tpid.Invalid() {
		return fmt.Errorf("invalid topic %d", tpid)
	}
	if lsid := iface.GetLogStreamID(); lsid.Invalid() {
		return fmt.Errorf("invalid log stream %d", lsid)
	}
	return nil
}

func (m *AppendRequest) ResetReuse() {
	for i := range m.Payload {
		m.Payload[i] = m.Payload[i][:0]
	}
	payload := m.Payload[:0]
	m.Reset()
	m.Payload = payload
}
