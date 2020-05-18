package libsequencer

import (
	"context"

	pb "github.daumkakao.com/wokl/wokl/proto/sequencer"
	"google.golang.org/grpc"
)

type SequencerClient struct {
	grpcConn *grpc.ClientConn
}

type SequencerConnection struct {
	client pb.SequencerServiceClient
}

func NewSequencerClient(address string) (*SequencerClient, error) {
	conn, err := grpc.Dial(address, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		return nil, err
	}
	return &SequencerClient{grpcConn: conn}, nil
}

func (c *SequencerClient) Close() error {
	return c.grpcConn.Close()
}

func (c *SequencerClient) Connect() *SequencerConnection {
	if c.grpcConn == nil {
		return nil
	}
	client := pb.NewSequencerServiceClient(c.grpcConn)
	return &SequencerConnection{client: client}
}

func (c *SequencerConnection) Next(ctx context.Context, glsn *uint64) error {
	rsp, err := c.client.Next(ctx, &pb.SequencerRequest{})
	if err != nil {
		return err
	}
	*glsn = rsp.GetGlsn()
	return nil
}
