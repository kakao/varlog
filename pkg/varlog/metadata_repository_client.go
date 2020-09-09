package varlog

import (
	"context"

	types "github.daumkakao.com/varlog/varlog/pkg/varlog/types"
	pb "github.daumkakao.com/varlog/varlog/proto/metadata_repository"
	varlogpb "github.daumkakao.com/varlog/varlog/proto/varlog"
)

type MetadataRepositoryClient interface {
	RegisterStorageNode(context.Context, *varlogpb.StorageNodeDescriptor) error
	UnregisterStorageNode(context.Context, types.StorageNodeID) error
	RegisterLogStream(context.Context, *varlogpb.LogStreamDescriptor) error
	UnregisterLogStream(context.Context, types.LogStreamID) error
	UpdateLogStream(context.Context, *varlogpb.LogStreamDescriptor) error
	GetMetadata(context.Context) (*varlogpb.MetadataDescriptor, error)
	Seal(context.Context, types.LogStreamID) (types.GLSN, error)
	Unseal(context.Context, types.LogStreamID) error
	Close() error
}

type metadataRepositoryClient struct {
	rpcConn *RpcConn
	client  pb.MetadataRepositoryServiceClient
}

func NewMetadataRepositoryClient(address string) (MetadataRepositoryClient, error) {
	rpcConn, err := NewRpcConn(address)
	if err != nil {
		return nil, err
	}
	return NewMetadataRepositoryClientFromRpcConn(rpcConn)
}

func NewMetadataRepositoryClientFromRpcConn(rpcConn *RpcConn) (MetadataRepositoryClient, error) {
	client := &metadataRepositoryClient{
		rpcConn: rpcConn,
		client:  pb.NewMetadataRepositoryServiceClient(rpcConn.Conn),
	}
	return client, nil
}

func (c *metadataRepositoryClient) Close() error {
	return c.rpcConn.Close()
}

func (c *metadataRepositoryClient) RegisterStorageNode(ctx context.Context, sn *varlogpb.StorageNodeDescriptor) error {
	if !sn.Valid() {
		return ErrInvalid
	}

	req := &pb.StorageNodeRequest{
		StorageNode: sn,
	}

	_, err := c.client.RegisterStorageNode(ctx, req)
	return ToErr(ctx, err)
}

func (c *metadataRepositoryClient) UnregisterStorageNode(ctx context.Context, snID types.StorageNodeID) error {
	req := &pb.StorageNodeRequest{
		StorageNode: &varlogpb.StorageNodeDescriptor{
			StorageNodeID: snID,
		},
	}

	_, err := c.client.UnregisterStorageNode(ctx, req)
	return ToErr(ctx, err)
}

func (c *metadataRepositoryClient) RegisterLogStream(ctx context.Context, ls *varlogpb.LogStreamDescriptor) error {
	if !ls.Valid() {
		return ErrInvalid
	}

	req := &pb.LogStreamRequest{
		LogStream: ls,
	}
	_, err := c.client.RegisterLogStream(ctx, req)
	return ToErr(ctx, err)
}

func (c *metadataRepositoryClient) UnregisterLogStream(ctx context.Context, lsID types.LogStreamID) error {
	req := &pb.LogStreamRequest{
		LogStream: &varlogpb.LogStreamDescriptor{
			LogStreamID: lsID,
		},
	}
	_, err := c.client.UnregisterLogStream(ctx, req)
	return ToErr(ctx, err)
}

func (c *metadataRepositoryClient) UpdateLogStream(ctx context.Context, ls *varlogpb.LogStreamDescriptor) error {
	if !ls.Valid() {
		return ErrInvalid
	}

	req := &pb.LogStreamRequest{
		LogStream: ls,
	}
	_, err := c.client.UpdateLogStream(ctx, req)
	return ToErr(ctx, err)
}

func (c *metadataRepositoryClient) GetMetadata(ctx context.Context) (*varlogpb.MetadataDescriptor, error) {
	rsp, err := c.client.GetMetadata(ctx, &pb.GetMetadataRequest{})
	if err != nil {
		return nil, ToErr(ctx, err)
	}
	return rsp.GetMetadata(), nil
}

func (c *metadataRepositoryClient) Seal(ctx context.Context, lsID types.LogStreamID) (types.GLSN, error) {
	rsp, err := c.client.Seal(ctx, &pb.SealRequest{LogStreamID: lsID})
	if err != nil {
		return types.InvalidGLSN, ToErr(ctx, err)
	}

	return rsp.LastCommittedGLSN, nil
}

func (c *metadataRepositoryClient) Unseal(ctx context.Context, lsID types.LogStreamID) error {
	_, err := c.client.Unseal(ctx, &pb.UnsealRequest{})
	if err != nil {
		return ToErr(ctx, err)
	}
	return nil
}
