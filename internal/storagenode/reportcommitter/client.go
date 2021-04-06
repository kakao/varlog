package reportcommitter

//go:generate mockgen -build_flags -mod=vendor -self_package github.com/kakao/varlog/internal/storagenode/reportcommitter -package reportcommitter -destination client_mock.go . Client

import (
	"context"

	"github.com/pkg/errors"

	"github.com/kakao/varlog/pkg/rpc"
	"github.com/kakao/varlog/pkg/verrors"
	"github.com/kakao/varlog/proto/snpb"
)

// Client contains the functionality of bi-directional communication about local
// log stream and global log stream.
type Client interface {
	GetReport(context.Context) (*snpb.GetReportResponse, error)
	Commit(context.Context, *snpb.CommitRequest) error
	Close() error
}

type client struct {
	rpcConn   *rpc.Conn
	rpcClient snpb.LogStreamReporterClient
}

func NewClient(ctx context.Context, address string) (Client, error) {
	rpcConn, err := rpc.NewConn(ctx, address)
	if err != nil {
		return nil, err
	}
	return &client{
		rpcConn:   rpcConn,
		rpcClient: snpb.NewLogStreamReporterClient(rpcConn.Conn),
	}, nil
}

func (c *client) GetReport(ctx context.Context) (*snpb.GetReportResponse, error) {
	rsp, err := c.rpcClient.GetReport(ctx, &snpb.GetReportRequest{})
	return rsp, errors.WithStack(verrors.FromStatusError(err))

}

func (c *client) Commit(ctx context.Context, cr *snpb.CommitRequest) error {
	_, err := c.rpcClient.Commit(ctx, cr)
	return errors.WithStack(verrors.FromStatusError(err))
}

func (c *client) Close() error {
	return c.rpcConn.Close()
}
