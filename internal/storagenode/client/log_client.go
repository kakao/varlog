package client

import (
	"context"
	"errors"
	"fmt"

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
	Error:    errors.New("invalid subscribe result"),
}

type LogClient struct {
	rpcClient snpb.LogIOClient
	target    varlogpb.StorageNode
}

// reset creates a client that connects to the storage node specified by the
// argument target.
// TODO: Fetch metadata of the storage node to confirm whether the snid is
// correct.
func (c *LogClient) reset(rpcConn *rpc.Conn, _ types.ClusterID, target varlogpb.StorageNode) any {
	return &LogClient{
		rpcClient: snpb.NewLogIOClient(rpcConn.Conn),
		target:    target,
	}
}

// Append stores data to the log stream specified with the topicID and the logStreamID.
// The backup indicates the storage nodes that have backup replicas of that log stream.
// It returns valid GLSN if the append completes successfully.
func (c *LogClient) Append(ctx context.Context, tpid types.TopicID, lsid types.LogStreamID, data [][]byte, backups ...varlogpb.StorageNode) ([]snpb.AppendResult, error) {
	req := &snpb.AppendRequest{
		TopicID:     tpid,
		LogStreamID: lsid,
		Payload:     data,
		Backups:     backups,
	}
	rsp, err := c.rpcClient.Append(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("logclient: %w", verrors.FromStatusError(err))
	}
	return rsp.Results, nil
}

// Subscribe gets log entries continuously from the storage node. It guarantees that LLSNs of log
// entries taken are sequential.
func (c *LogClient) Subscribe(ctx context.Context, tpid types.TopicID, lsid types.LogStreamID, begin, end types.GLSN) (<-chan SubscribeResult, error) {
	if begin >= end {
		return nil, errors.New("logclient: invalid argument")
	}

	req := &snpb.SubscribeRequest{
		TopicID:     tpid,
		LogStreamID: lsid,
		GLSNBegin:   begin,
		GLSNEnd:     end,
	}
	stream, err := c.rpcClient.Subscribe(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("logclient: %w", verrors.FromStatusError(err))
	}

	out := make(chan SubscribeResult)
	go func(ctx context.Context) {
		defer func() {
			close(out)
		}()
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

func (c *LogClient) SubscribeTo(ctx context.Context, tpid types.TopicID, lsid types.LogStreamID, begin, end types.LLSN) (<-chan SubscribeResult, error) {
	if begin >= end {
		return nil, errors.New("logclient: invalid argument")
	}

	req := &snpb.SubscribeToRequest{
		TopicID:     tpid,
		LogStreamID: lsid,
		LLSNBegin:   begin,
		LLSNEnd:     end,
	}
	stream, err := c.rpcClient.SubscribeTo(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("logclient: %w", verrors.FromStatusError(err))
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

// TrimDeprecated deletes log entries greater than or equal to given GLSN in
// the storage node. The number of deleted log entries are returned.
func (c *LogClient) TrimDeprecated(ctx context.Context, tpid types.TopicID, glsn types.GLSN) error {
	req := &snpb.TrimDeprecatedRequest{
		TopicID: tpid,
		GLSN:    glsn,
	}
	if _, err := c.rpcClient.TrimDeprecated(ctx, req); err != nil {
		return fmt.Errorf("logclient: %w", verrors.FromStatusError(err))
	}
	return nil
}

func (c *LogClient) LogStreamMetadata(ctx context.Context, tpid types.TopicID, lsid types.LogStreamID) (varlogpb.LogStreamDescriptor, error) {
	rsp, err := c.rpcClient.LogStreamMetadata(ctx, &snpb.LogStreamMetadataRequest{
		TopicID:     tpid,
		LogStreamID: lsid,
	})
	if err != nil {
		return rsp.GetLogStreamDescriptor(), fmt.Errorf("logclient: %w", verrors.FromStatusError(err))
	}
	return rsp.GetLogStreamDescriptor(), nil
}

// Target returns connected storage node.
func (c *LogClient) Target() varlogpb.StorageNode {
	return c.target
}
