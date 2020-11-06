package logc

import (
	"context"

	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/pkg/util/syncutil/atomicutil"
)

type logClientProxy struct {
	client LogIOClient
	closed atomicutil.AtomicBool
}

var _ LogIOClient = (*logClientProxy)(nil)

func newLogIOProxy(client LogIOClient) *logClientProxy {
	return &logClientProxy{
		client: client,
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
	l.closed.Store(true)
	return nil
}
