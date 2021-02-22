package logc

import (
	"context"

	"github.daumkakao.com/varlog/varlog/pkg/types"
)

type logClientProxy struct {
	client LogIOClient
	closer func() error
}

var _ LogIOClient = (*logClientProxy)(nil)

func newLogIOProxy(client LogIOClient, closer func() error) *logClientProxy {
	return &logClientProxy{
		client: client,
		closer: closer,
	}
}

func (l *logClientProxy) Append(ctx context.Context, logStreamID types.LogStreamID, data []byte, backups ...StorageNode) (types.GLSN, error) {
	return l.client.Append(ctx, logStreamID, data, backups...)
}

func (l *logClientProxy) Read(ctx context.Context, logStreamID types.LogStreamID, glsn types.GLSN) (*types.LogEntry, error) {
	return l.client.Read(ctx, logStreamID, glsn)
}

func (l *logClientProxy) Subscribe(ctx context.Context, logStreamID types.LogStreamID, begin, end types.GLSN) (<-chan SubscribeResult, error) {
	return l.client.Subscribe(ctx, logStreamID, begin, end)
}

func (l *logClientProxy) Trim(ctx context.Context, glsn types.GLSN) error {
	return l.client.Trim(ctx, glsn)
}

func (l *logClientProxy) Close() error {
	return l.closer()
}
