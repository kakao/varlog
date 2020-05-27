package solar

import (
	"context"

	pb "github.daumkakao.com/solar/solar/proto/metadata_repository"
	solarpb "github.daumkakao.com/solar/solar/proto/solar"
)

type MetadataRepositoryClient interface {
	Propose(context.Context) error
	Get(context.Context) (*solarpb.ProjectionDescriptor, error)
}

type metadataRepositoryClient struct {
	rpcConn *rpcConn
	client  pb.MetadataRepositoryServiceClient
}

func NewMetadataRepositoryClient(address string) (MetadataRepositoryClient, error) {
	rpcConn, err := newRpcConn(address)
	if err != nil {
		return nil, err
	}
	return NewMetadataRepositoryClientFromRpcConn(rpcConn)
}

func NewMetadataRepositoryClientFromRpcConn(rpcConn *rpcConn) (MetadataRepositoryClient, error) {
	client := &metadataRepositoryClient{
		rpcConn: rpcConn,
		client:  pb.NewMetadataRepositoryServiceClient(rpcConn.conn),
	}
	return client, nil
}

func (c *metadataRepositoryClient) Close() error {
	return c.rpcConn.close()
}

func (c *metadataRepositoryClient) Propose(ctx context.Context) error {
	_, err := c.client.Propose(ctx, &pb.ProposeRequest{})
	if err != nil {
		return err
	}
	return nil
}

func (c *metadataRepositoryClient) Get(ctx context.Context) (*solarpb.ProjectionDescriptor, error) {
	rsp, err := c.client.Get(ctx, &pb.GetRequest{})
	if err != nil {
		return nil, err
	}
	//projection := NewProjectionFromProto(rsp.GetProjection())
	return rsp.GetProjection(), nil
}
