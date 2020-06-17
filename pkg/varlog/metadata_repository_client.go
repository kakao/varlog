package varlog

import (
	"context"

	pb "github.daumkakao.com/varlog/varlog/proto/metadata_repository"
	varlogpb "github.daumkakao.com/varlog/varlog/proto/varlog"
)

type MetadataRepositoryClient interface {
	RegisterStorageNode(context.Context, *varlogpb.StorageNodeDescriptor) error
	CreateLogStream(context.Context, *varlogpb.LogStreamDescriptor) error
	GetMetadata(context.Context) (*varlogpb.MetadataDescriptor, error)
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

func (c *metadataRepositoryClient) RegisterStorageNode(ctx context.Context, sn *varlogpb.StorageNodeDescriptor) error {
	req := &pb.RegisterStorageNodeRequest{
		StorageNode: sn,
	}

	_, err := c.client.RegisterStorageNode(ctx, req)
	return toErr(ctx, err)
}

func (c *metadataRepositoryClient) CreateLogStream(ctx context.Context, ls *varlogpb.LogStreamDescriptor) error {
	req := &pb.CreateLogStreamRequest{
		LogStream: ls,
	}
	_, err := c.client.CreateLogStream(ctx, req)
	return toErr(ctx, err)
}

func (c *metadataRepositoryClient) GetMetadata(ctx context.Context) (*varlogpb.MetadataDescriptor, error) {
	rsp, err := c.client.GetMetadata(ctx, &pb.GetMetadataRequest{})
	if err != nil {
		return nil, toErr(ctx, err)
	}
	return rsp.GetMetadata(), nil
}
