package reportcommitter

//go:generate mockgen -build_flags -mod=vendor -self_package github.com/kakao/varlog/internal/storagenode/reportcommitter -package reportcommitter -destination client_mock.go . Client

import (
	"context"
	"io"
	"sync"

	"go.uber.org/multierr"

	"github.com/kakao/varlog/pkg/rpc"
	"github.com/kakao/varlog/proto/snpb"
)

// Client contains the functionality of bi-directional communication about local
// log stream and global log stream.
type Client interface {
	GetReport() (*snpb.GetReportResponse, error)
	Commit(snpb.CommitRequest) error
	Close() error
}

type client struct {
	rpcConn   *rpc.Conn
	rpcClient snpb.LogStreamReporterClient

	// TODO(jun): If each reportCollectorExecutor that belongs to the same storage node
	// instantiates the client by using NewClientWithConn with the same rpc.Conn, mutex
	// muCommitStream can be removed.
	reportStream   snpb.LogStreamReporter_GetReportClient
	muReportStream sync.Mutex
	getReportReq   snpb.GetReportRequest

	commitStream   snpb.LogStreamReporter_CommitClient
	muCommitStream sync.Mutex
}

func NewClient(ctx context.Context, address string) (cl Client, err error) {
	rpcConn, err := rpc.NewConn(ctx, address)
	if err != nil {
		return nil, err
	}

	defer func() {
		if err != nil {
			err = multierr.Append(err, rpcConn.Close())
		}
	}()

	cl, err = NewClientWithConn(context.Background(), rpcConn)
	return cl, err
}

// Clients connecting the same SN can use this method to multiplex streams. By doing that, it can
// decrease the use of channel buffer.
func NewClientWithConn(ctx context.Context, rpcConn *rpc.Conn) (Client, error) {
	rpcClient := snpb.NewLogStreamReporterClient(rpcConn.Conn)

	reportStream, err := rpcClient.GetReport(ctx)
	if err != nil {
		return nil, err
	}

	commitStream, err := rpcClient.Commit(ctx)
	if err != nil {
		return nil, err
	}

	cl := &client{
		rpcConn:      rpcConn,
		rpcClient:    rpcClient,
		reportStream: reportStream,
		commitStream: commitStream,
	}

	return cl, nil
}

// FIXME(jun): add response parameter to return the response without creating a new object.
func (c *client) GetReport() (*snpb.GetReportResponse, error) {
	c.muReportStream.Lock()
	defer c.muReportStream.Unlock()

	if err := c.reportStream.Send(&c.getReportReq); err != nil {
		return nil, err
	}
	rsp, err := c.reportStream.Recv()
	if err != nil {
		if err == io.EOF {
			// NOTE(jun,pharrell): Zero value of GetReportResponse should be handled.
			return &snpb.GetReportResponse{}, nil
		}
		return nil, err
	}
	return rsp, nil
}

func (c *client) Commit(cr snpb.CommitRequest) (err error) {
	c.muCommitStream.Lock()
	defer c.muCommitStream.Unlock()

	// Do not handle io.EOF
	err = c.commitStream.Send(&cr)
	if err != nil {
		return c.commitStream.CloseSend()
	}
	return nil
}

func (c *client) Close() error {
	return c.rpcConn.Close()
}
