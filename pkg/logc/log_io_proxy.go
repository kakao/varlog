package logc

import (
	"context"

	"github.daumkakao.com/varlog/varlog/pkg/types"
	"github.daumkakao.com/varlog/varlog/proto/snpb"
	"github.daumkakao.com/varlog/varlog/proto/varlogpb"
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

func (l *logClientProxy) Append(ctx context.Context, topicID types.TopicID, logStreamID types.LogStreamID, data [][]byte, backups ...varlogpb.StorageNode) ([]snpb.AppendResult, error) {
	return l.client.Append(ctx, topicID, logStreamID, data, backups...)
}

func (l *logClientProxy) Read(ctx context.Context, topicID types.TopicID, logStreamID types.LogStreamID, glsn types.GLSN) (*varlogpb.LogEntry, error) {
	return l.client.Read(ctx, topicID, logStreamID, glsn)
}

func (l *logClientProxy) Subscribe(ctx context.Context, topicID types.TopicID, logStreamID types.LogStreamID, begin, end types.GLSN) (<-chan SubscribeResult, error) {
	return l.client.Subscribe(ctx, topicID, logStreamID, begin, end)
}

func (l *logClientProxy) Trim(ctx context.Context, topicID types.TopicID, glsn types.GLSN) error {
	return l.client.Trim(ctx, topicID, glsn)
}

func (l *logClientProxy) Close() error {
	return l.closer()
}
