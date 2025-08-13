package reportcommitter

//go:generate go tool mockgen -build_flags -mod=vendor -self_package github.com/kakao/varlog/internal/reportcommitter -package reportcommitter -destination client_mock.go . Client

import (
	"context"
	"errors"

	"google.golang.org/grpc"

	"github.com/kakao/varlog/pkg/rpc"
	"github.com/kakao/varlog/proto/snpb"
)

// Client establishes a connection to a storage node for retrieving reports and
// sending commit results. GetReport and CommitBatch can be invoked
// concurrently by different goroutines. However, GetReport must not be invoked
// concurrently by multiple goroutines, and the same applies to CommitBatch.
type Client interface {
	GetReport() (*snpb.GetReportResponse, error)
	CommitBatch(snpb.CommitBatchRequest) error
	Close() error
}

type client struct {
	rpcConn   *rpc.Conn
	rpcClient snpb.LogStreamReporterClient

	reportStream snpb.LogStreamReporter_GetReportClient
	getReportReq snpb.GetReportRequest

	commitBatchStream snpb.LogStreamReporter_CommitBatchClient
}

func NewClient(ctx context.Context, address string, grpcDialOptions ...grpc.DialOption) (cl Client, err error) {
	rpcConn, err := rpc.NewConn(ctx, address, grpcDialOptions...)
	if err != nil {
		return nil, err
	}

	defer func() {
		if err != nil {
			err = errors.Join(err, rpcConn.Close())
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

	commitBatchStream, err := rpcClient.CommitBatch(ctx)
	if err != nil {
		return nil, err
	}

	cl := &client{
		rpcConn:           rpcConn,
		rpcClient:         rpcClient,
		reportStream:      reportStream,
		commitBatchStream: commitBatchStream,
	}

	return cl, nil
}

// GetReport retrieves log stream reports from the connected storage node. This
// method is not safe for concurrent use by multiple goroutines. Multiple
// goroutines must not call GetReport concurrently.
//
// FIXME(jun): add response parameter to return the response without creating a new object.
func (c *client) GetReport() (*snpb.GetReportResponse, error) {
	if err := c.reportStream.Send(&c.getReportReq); err != nil {
		_ = c.reportStream.CloseSend()
		return nil, err
	}
	rsp, err := c.reportStream.Recv()
	if err != nil {
		_ = c.reportStream.CloseSend()
		return nil, err
	}
	return rsp, nil
}

// CommitBatch sends commit results to the connected storage node. This method
// is not safe for concurrent use by multiple goroutines. Multiple goroutines
// must not call CommitBatch concurrently.
func (c *client) CommitBatch(cr snpb.CommitBatchRequest) (err error) {
	// Do not handle io.EOF
	err = c.commitBatchStream.Send(&cr)
	if err != nil {
		return c.commitBatchStream.CloseSend()
	}
	return nil
}

func (c *client) Close() error {
	return c.rpcConn.Close()
}
