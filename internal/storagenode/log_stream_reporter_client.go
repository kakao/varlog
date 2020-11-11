package storagenode

//go:generate mockgen -build_flags -mod=vendor -self_package github.daumkakao.com/varlog/varlog/internal/storagenode -package storagenode -destination log_stream_reporter_client_mock.go . LogStreamReporterClient

import (
	"context"

	"github.com/gogo/protobuf/types"

	"github.daumkakao.com/varlog/varlog/pkg/rpc"
	"github.daumkakao.com/varlog/varlog/proto/snpb"
)

// LogStreamReporterClient contains the functionality of bi-directional communication about local
// log stream and global log stream.
type LogStreamReporterClient interface {
	GetReport(context.Context) (*snpb.LocalLogStreamDescriptor, error)
	Commit(context.Context, *snpb.GlobalLogStreamDescriptor) error
	Close() error
}

type logStreamReporterClient struct {
	rpcConn   *rpc.Conn
	rpcClient snpb.LogStreamReporterServiceClient
}

func NewLogStreamReporterClient(address string) (LogStreamReporterClient, error) {
	rpcConn, err := rpc.NewConn(address)
	if err != nil {
		return nil, err
	}
	return NewLogStreamReporterClientFromRpcConn(rpcConn)
}

func NewLogStreamReporterClientFromRpcConn(rpcConn *rpc.Conn) (LogStreamReporterClient, error) {
	return &logStreamReporterClient{
		rpcConn:   rpcConn,
		rpcClient: snpb.NewLogStreamReporterServiceClient(rpcConn.Conn),
	}, nil
}

func (c *logStreamReporterClient) GetReport(ctx context.Context) (*snpb.LocalLogStreamDescriptor, error) {
	return c.rpcClient.GetReport(ctx, &types.Empty{})
}

func (c *logStreamReporterClient) Commit(ctx context.Context, gls *snpb.GlobalLogStreamDescriptor) error {
	_, err := c.rpcClient.Commit(ctx, gls)
	return err
}

func (c *logStreamReporterClient) Close() error {
	return c.rpcConn.Close()
}
