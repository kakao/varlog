package varlog

import (
	"context"

	pb "github.daumkakao.com/varlog/varlog/proto/sequencer"
)

type SequencerClient interface {
	Id() string
	Close() error
	Next(ctx context.Context) (uint64, error)
}

type sequencerClient struct {
	id      string
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
		// FIXME: To set a sequencer identifier, address of rpc server is used temporarily
		id:      rpcConn.conn.Target(),
		rpcConn: rpcConn,
		client:  pb.NewSequencerServiceClient(rpcConn.conn),
	}
	return client, nil
}

func (c sequencerClient) Id() string {
	return c.id
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
