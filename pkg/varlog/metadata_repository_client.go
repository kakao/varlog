package varlog

import (
	"context"

	pb "github.com/kakao/varlog/proto/metadata_repository"
	varlogpb "github.com/kakao/varlog/proto/varlog"
)

type MetadataRepositoryClient interface {
	Propose(context.Context, uint64, *varlogpb.ProjectionDescriptor) error
	GetMetadata(context.Context) (*varlogpb.MetadataDescriptor, error)
	GetProjection(context.Context, uint64) (*varlogpb.ProjectionDescriptor, error)
	Close() error
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

func (c *metadataRepositoryClient) Propose(ctx context.Context, epoch uint64, projection *varlogpb.ProjectionDescriptor) error {
	_, err := c.client.Propose(ctx, &pb.ProposeRequest{Epoch: epoch, Projection: projection})
	if err != nil {
		return err
	}
	return nil
}

func (c *metadataRepositoryClient) GetProjection(ctx context.Context, epoch uint64) (*varlogpb.ProjectionDescriptor, error) {
	rsp, err := c.client.GetProjection(ctx, &pb.GetProjectionRequest{Epoch: epoch})
	if err != nil {
		return nil, err
	}
	return rsp.GetProjection(), nil
}

func (c *metadataRepositoryClient) GetMetadata(ctx context.Context) (*varlogpb.MetadataDescriptor, error) {
	rsp, err := c.client.GetMetadata(ctx, &pb.GetMetadataRequest{})
	if err != nil {
		return nil, err
	}
	return rsp.GetMetadata(), nil
}
