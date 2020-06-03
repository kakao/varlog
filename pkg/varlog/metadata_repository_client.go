package varlog

import (
	"context"

	pb "github.daumkakao.com/varlog/varlog/proto/metadata_repository"
	varlogpb "github.daumkakao.com/varlog/varlog/proto/varlog"
)

type MetadataRepositoryClient interface {
	Propose(context.Context) error
	Get(context.Context) (*varlogpb.ProjectionDescriptor, error)
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

func (c *metadataRepositoryClient) Get(ctx context.Context) (*varlogpb.ProjectionDescriptor, error) {
	rsp, err := c.client.Get(ctx, &pb.GetRequest{})
	if err != nil {
		return nil, err
	}
	//projection := NewProjectionFromProto(rsp.GetProjection())
	return rsp.GetProjection(), nil
}
