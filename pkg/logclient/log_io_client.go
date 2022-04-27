package logclient

//go:generate mockgen -build_flags -mod=vendor -self_package github.daumkakao.com/varlog/varlog/pkg/logclient -package logclient -destination log_io_client_mock.go . LogIOClient

import (
	"context"
	stderrors "errors"
	"io"

	"github.com/pkg/errors"
	"google.golang.org/grpc"

	"github.daumkakao.com/varlog/varlog/pkg/rpc"
	"github.daumkakao.com/varlog/varlog/pkg/types"
	"github.daumkakao.com/varlog/varlog/pkg/verrors"
	"github.daumkakao.com/varlog/varlog/proto/snpb"
	"github.daumkakao.com/varlog/varlog/proto/varlogpb"
)

type SubscribeResult struct {
	varlogpb.LogEntry
	Error error
}

var InvalidSubscribeResult = SubscribeResult{
	LogEntry: varlogpb.InvalidLogEntry(),
	Error:    stderrors.New("invalid subscribe result"),
}

// LogIOClient contains methods to use basic operations - append, read, subscribe, trim of
// single storage node.
type LogIOClient interface {
	Append(ctx context.Context, topicID types.TopicID, logStreamID types.LogStreamID, data [][]byte, backups ...varlogpb.StorageNode) ([]snpb.AppendResult, error)
	Read(ctx context.Context, topicID types.TopicID, logStreamID types.LogStreamID, glsn types.GLSN) (*varlogpb.LogEntry, error)
	Subscribe(ctx context.Context, topicID types.TopicID, logStreamID types.LogStreamID, begin, end types.GLSN) (<-chan SubscribeResult, error)
	SubscribeTo(ctx context.Context, topicID types.TopicID, logStreamID types.LogStreamID, begin, end types.LLSN) (<-chan SubscribeResult, error)
	TrimDeprecated(ctx context.Context, topicID types.TopicID, glsn types.GLSN) error
	LogStreamMetadata(ctx context.Context, topicID types.TopicID, logStreamID types.LogStreamID) (varlogpb.LogStreamDescriptor, error)
	io.Closer
}

type logIOClient struct {
	rpcConn   *rpc.Conn
	rpcClient snpb.LogIOClient
}

func NewLogIOClient(ctx context.Context, address string, grpcDialOptions ...grpc.DialOption) (LogIOClient, error) {
	rpcConn, err := rpc.NewConn(ctx, address, grpcDialOptions...)
	if err != nil {
		return nil, errors.WithMessage(err, "logiocl")
	}
	return NewLogIOClientFromRPCConn(rpcConn)
}

func NewLogIOClientFromRPCConn(rpcConn *rpc.Conn) (LogIOClient, error) {
	return &logIOClient{
		rpcConn:   rpcConn,
		rpcClient: snpb.NewLogIOClient(rpcConn.Conn),
	}, nil
}

// Append stores data to the log stream specified with the topicID and the logStreamID.
// The backup indicates the storage nodes that have backup replicas of that log stream.
// It returns valid GLSN if the append completes successfully.
func (c *logIOClient) Append(ctx context.Context, topicID types.TopicID, logStreamID types.LogStreamID, data [][]byte, backups ...varlogpb.StorageNode) ([]snpb.AppendResult, error) {
	req := &snpb.AppendRequest{
		TopicID:     topicID,
		LogStreamID: logStreamID,
		Payload:     data,
		Backups:     backups,
	}
	rsp, err := c.rpcClient.Append(ctx, req)
	if err != nil {
		return nil, errors.Wrap(verrors.FromStatusError(err), "logiocl")
	}
	return rsp.Results, nil
}

// Read operation asks the storage node to retrieve data at a given log position in the log stream.
func (c *logIOClient) Read(ctx context.Context, topicID types.TopicID, logStreamID types.LogStreamID, glsn types.GLSN) (*varlogpb.LogEntry, error) {
	req := &snpb.ReadRequest{
		GLSN:        glsn,
		TopicID:     topicID,
		LogStreamID: logStreamID,
	}
	rsp, err := c.rpcClient.Read(ctx, req)
	if err != nil {
		return nil, errors.Wrap(verrors.FromStatusError(err), "logiocl")
	}
	return &varlogpb.LogEntry{
		LogEntryMeta: varlogpb.LogEntryMeta{
			GLSN: rsp.GetGLSN(),
			LLSN: rsp.GetLLSN(),
		},
		Data: rsp.GetPayload(),
	}, nil
}

// Subscribe gets log entries continuously from the storage node. It guarantees that LLSNs of log
// entries taken are sequential.
func (c *logIOClient) Subscribe(ctx context.Context, topicID types.TopicID, logStreamID types.LogStreamID, begin, end types.GLSN) (<-chan SubscribeResult, error) {
	if begin >= end {
		return nil, errors.New("logiocl: invalid argument")
	}

	req := &snpb.SubscribeRequest{
		TopicID:     topicID,
		LogStreamID: logStreamID,
		GLSNBegin:   begin,
		GLSNEnd:     end,
	}
	stream, err := c.rpcClient.Subscribe(ctx, req)
	if err != nil {
		return nil, errors.Wrap(verrors.FromStatusError(err), "logiocl")
	}

	out := make(chan SubscribeResult)
	go func(ctx context.Context) {
		defer close(out)
		for {
			rsp, rpcErr := stream.Recv()
			err := verrors.FromStatusError(rpcErr)
			result := SubscribeResult{Error: err}
			if err == nil {
				result.LogEntry = varlogpb.LogEntry{
					LogEntryMeta: varlogpb.LogEntryMeta{
						GLSN: rsp.GetGLSN(),
						LLSN: rsp.GetLLSN(),
					},
					Data: rsp.GetPayload(),
				}
			}
			select {
			case out <- result:
				if err != nil {
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}(ctx)
	return out, nil
}

func (c *logIOClient) SubscribeTo(ctx context.Context, topicID types.TopicID, logStreamID types.LogStreamID, begin, end types.LLSN) (<-chan SubscribeResult, error) {
	if begin >= end {
		return nil, errors.New("logiocl: invalid argument")
	}

	req := &snpb.SubscribeToRequest{
		TopicID:     topicID,
		LogStreamID: logStreamID,
		LLSNBegin:   begin,
		LLSNEnd:     end,
	}
	stream, err := c.rpcClient.SubscribeTo(ctx, req)
	if err != nil {
		return nil, errors.Wrap(verrors.FromStatusError(err), "logiocl")
	}

	out := make(chan SubscribeResult)
	// FIXME (jun): clean up goroutines in both Subscribe and SubscribeTo
	go func(ctx context.Context) {
		defer close(out)
		for {
			rsp, rpcErr := stream.Recv()
			err := verrors.FromStatusError(rpcErr)
			result := SubscribeResult{Error: err}
			if err == nil {
				result.LogEntry = rsp.LogEntry
			}
			select {
			case out <- result:
				if err != nil {
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}(ctx)
	return out, nil
}

// TrimDeprecated deletes log entries greater than or equal to given GLSN in the storage node. The number of
// deleted log entries are returned.
func (c *logIOClient) TrimDeprecated(ctx context.Context, topicID types.TopicID, glsn types.GLSN) error {
	req := &snpb.TrimDeprecatedRequest{
		TopicID: topicID,
		GLSN:    glsn,
	}
	_, err := c.rpcClient.TrimDeprecated(ctx, req)
	return errors.Wrap(verrors.FromStatusError(err), "logiocl")
}

func (c *logIOClient) LogStreamMetadata(ctx context.Context, topicID types.TopicID, logStreamID types.LogStreamID) (varlogpb.LogStreamDescriptor, error) {
	rsp, err := c.rpcClient.LogStreamMetadata(ctx, &snpb.LogStreamMetadataRequest{
		TopicID:     topicID,
		LogStreamID: logStreamID,
	})
	return rsp.GetLogStreamDescriptor(), errors.Wrap(verrors.FromStatusError(err), "logiocl")
}

// Close closes connection to the storage node.
func (c *logIOClient) Close() error {
	return c.rpcConn.Close()
}
