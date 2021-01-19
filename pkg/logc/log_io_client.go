package logc

//go:generate mockgen -build_flags -mod=vendor -self_package github.com/kakao/varlog/pkg/logc -package logc -destination log_io_client_mock.go . LogIOClient

import (
	"context"
	"io"

	"github.com/pkg/errors"

	"github.com/kakao/varlog/pkg/rpc"
	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/pkg/verrors"
	"github.com/kakao/varlog/proto/snpb"
)

// StorageNode is a structure to represent identifier and address of storage node.
type StorageNode struct {
	ID   types.StorageNodeID
	Addr string
}

type SubscribeResult struct {
	types.LogEntry
	Error error
}

var InvalidSubscribeResult = SubscribeResult{
	LogEntry: types.InvalidLogEntry,
	Error:    errors.New("invalid subscribe result"),
}

// LogIOClient contains methods to use basic operations - append, read, subscribe, trim of
// single storage node.
type LogIOClient interface {
	Append(ctx context.Context, logStreamID types.LogStreamID, data []byte, backups ...StorageNode) (types.GLSN, error)
	Read(ctx context.Context, logStreamID types.LogStreamID, glsn types.GLSN) (*types.LogEntry, error)
	Subscribe(ctx context.Context, logStreamID types.LogStreamID, begin, end types.GLSN) (<-chan SubscribeResult, error)
	Trim(ctx context.Context, glsn types.GLSN) error
	io.Closer
}

type logIOClient struct {
	rpcConn   *rpc.Conn
	rpcClient snpb.LogIOClient
	s         StorageNode
}

func NewLogIOClient(address string) (LogIOClient, error) {
	rpcConn, err := rpc.NewBlockingConn(address)
	if err != nil {
		return nil, errors.Wrap(err, "log io client")
	}
	return NewLogIOClientFromRpcConn(rpcConn)
}

func NewLogIOClientFromRpcConn(rpcConn *rpc.Conn) (LogIOClient, error) {
	return &logIOClient{
		rpcConn:   rpcConn,
		rpcClient: snpb.NewLogIOClient(rpcConn.Conn),
	}, nil
}

// Append sends given data to the log stream in the storage node. To replicate the data, it
// provides argument backups that indicate backup storage nodes. If append operation completes
// successfully,  valid GLSN is sent to the caller. When it goes wrong, zero is returned.
func (c *logIOClient) Append(ctx context.Context, logStreamID types.LogStreamID, data []byte, backups ...StorageNode) (types.GLSN, error) {
	req := &snpb.AppendRequest{
		Payload:     data,
		LogStreamID: logStreamID,
	}

	for _, b := range backups {
		req.Backups = append(req.Backups, snpb.AppendRequest_BackupNode{
			StorageNodeID: b.ID,
			Address:       b.Addr,
		})
	}
	rsp, err := c.rpcClient.Append(ctx, req)
	if err != nil {
		return types.InvalidGLSN, verrors.FromStatusError(err)
	}
	return rsp.GetGLSN(), nil
}

// Read operation asks the storage node to retrieve data at a given log position in the log stream.
func (c *logIOClient) Read(ctx context.Context, logStreamID types.LogStreamID, glsn types.GLSN) (*types.LogEntry, error) {
	req := &snpb.ReadRequest{
		GLSN:        glsn,
		LogStreamID: logStreamID,
	}
	rsp, err := c.rpcClient.Read(ctx, req)
	if err != nil {
		return nil, verrors.FromStatusError(err)
	}
	return &types.LogEntry{
		GLSN: rsp.GetGLSN(),
		LLSN: rsp.GetLLSN(),
		Data: rsp.GetPayload(),
	}, nil
}

// Subscribe gets log entries continuously from the storage node. It guarantees that LLSNs of log
// entries taken are sequential.
func (c *logIOClient) Subscribe(ctx context.Context, logStreamID types.LogStreamID, begin, end types.GLSN) (<-chan SubscribeResult, error) {
	if begin >= end {
		return nil, verrors.ErrInvalid
	}

	req := &snpb.SubscribeRequest{
		LogStreamID: logStreamID,
		GLSNBegin:   begin,
		GLSNEnd:     end,
	}
	stream, err := c.rpcClient.Subscribe(ctx, req)
	if err != nil {
		return nil, verrors.FromStatusError(err)
	}

	out := make(chan SubscribeResult)
	go func(ctx context.Context) {
		defer close(out)
		for {
			rsp, rpcErr := stream.Recv()
			err := verrors.FromStatusError(rpcErr)
			result := SubscribeResult{Error: err}
			if err == nil {
				result.LogEntry = types.LogEntry{
					GLSN: rsp.GetGLSN(),
					LLSN: rsp.GetLLSN(),
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

// Trim deletes log entries greater than or equal to given GLSN in the storage node. The number of
// deleted log entries are returned.
func (c *logIOClient) Trim(ctx context.Context, glsn types.GLSN) error {
	req := &snpb.TrimRequest{GLSN: glsn}
	_, err := c.rpcClient.Trim(ctx, req)
	return verrors.FromStatusError(err)
}

// Close closes connection to the storage node.
func (c *logIOClient) Close() error {
	return c.rpcConn.Close()
}
