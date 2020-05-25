package solar

import (
	"context"

	pb "github.daumkakao.com/wokl/solar/proto/sequencer"
)

type SequencerClient interface {
	Close() error
	Next(ctx context.Context) (uint64, error)
}

type sequencerClient struct {
	rpcConn *rpcConn
	client  pb.SequencerServiceClient
}

func NewSequencerClient(address string) (SequencerClient, error) {
	rpcConn, err := newRpcConn(address)
	if err != nil {
		return nil, err
	}
	return NewSequencerClientFromRpcConn(rpcConn)
}

func NewSequencerClientFromRpcConn(rpcConn *rpcConn) (SequencerClient, error) {
	client := &sequencerClient{
		rpcConn: rpcConn,
		client:  pb.NewSequencerServiceClient(rpcConn.conn),
	}
	return client, nil
}

func (c *sequencerClient) Close() error {
	return c.rpcConn.close()
}

func (c *sequencerClient) Next(ctx context.Context) (uint64, error) {
	rsp, err := c.client.Next(ctx, &pb.SequencerRequest{})
	if err != nil {
		return 0, err
	}
	return rsp.GetGlsn(), nil
}
