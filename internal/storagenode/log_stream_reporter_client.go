package storagenode

//go:generate mockgen -build_flags -mod=vendor -self_package github.com/kakao/varlog/internal/storagenode -package storagenode -destination log_stream_reporter_client_mock.go . LogStreamReporterClient

import (
	"context"

	"github.com/pkg/errors"

	"github.com/kakao/varlog/pkg/rpc"
	"github.com/kakao/varlog/pkg/verrors"
	"github.com/kakao/varlog/proto/snpb"
)

// LogStreamReporterClient contains the functionality of bi-directional communication about local
// log stream and global log stream.
type LogStreamReporterClient interface {
	GetReport(context.Context) (*snpb.GetReportResponse, error)
	Commit(context.Context, *snpb.CommitRequest) error
	Close() error
}

type logStreamReporterClient struct {
	rpcConn   *rpc.Conn
	rpcClient snpb.LogStreamReporterClient
}

func NewLogStreamReporterClient(ctx context.Context, address string) (LogStreamReporterClient, error) {
	rpcConn, err := rpc.NewConn(ctx, address)
	if err != nil {
		return nil, err
	}
	return NewLogStreamReporterClientFromRpcConn(rpcConn)
}

func NewLogStreamReporterClientFromRpcConn(rpcConn *rpc.Conn) (LogStreamReporterClient, error) {
	return &logStreamReporterClient{
		rpcConn:   rpcConn,
		rpcClient: snpb.NewLogStreamReporterClient(rpcConn.Conn),
	}, nil
}

func (c *logStreamReporterClient) GetReport(ctx context.Context) (*snpb.GetReportResponse, error) {
	rsp, err := c.rpcClient.GetReport(ctx, &snpb.GetReportRequest{})
	return rsp, errors.Wrap(verrors.FromStatusError(err), "lsrcl")

}

func (c *logStreamReporterClient) Commit(ctx context.Context, cr *snpb.CommitRequest) error {
	_, err := c.rpcClient.Commit(ctx, cr)
	return errors.Wrap(verrors.FromStatusError(err), "lsrcl")
}

func (c *logStreamReporterClient) Close() error {
	return c.rpcConn.Close()
}
