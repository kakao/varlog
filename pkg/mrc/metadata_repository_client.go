package mrc

import (
	"context"

	"github.com/pkg/errors"
	"go.uber.org/multierr"
	"google.golang.org/grpc/health/grpc_health_v1"

	"github.com/kakao/varlog/pkg/rpc"
	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/pkg/verrors"
	"github.com/kakao/varlog/proto/mrpb"
	"github.com/kakao/varlog/proto/varlogpb"
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
	rpcConn *rpc.Conn
	client  mrpb.MetadataRepositoryServiceClient
}

func NewMetadataRepositoryClient(address string) (MetadataRepositoryClient, error) {
	rpcConn, err := rpc.NewBlockingConn(address)
	if err != nil {
		return nil, err
	}

	client := grpc_health_v1.NewHealthClient(rpcConn.Conn)
	// TODO (jun): use context
	rsp, healthErr := client.Check(context.TODO(), &grpc_health_v1.HealthCheckRequest{})
	if healthErr != nil {
		return nil, multierr.Append(multierr.Append(err, healthErr), rpcConn.Close())
	}
	status := rsp.GetStatus()
	if status != grpc_health_v1.HealthCheckResponse_SERVING {
		return nil, multierr.Append(multierr.Append(err, errors.Errorf("mrcl: not ready (%+v)", status)), rpcConn.Close())
	}
	return NewMetadataRepositoryClientFromRpcConn(rpcConn)
}

func NewMetadataRepositoryClientFromRpcConn(rpcConn *rpc.Conn) (MetadataRepositoryClient, error) {
	client := &metadataRepositoryClient{
		rpcConn: rpcConn,
		client:  mrpb.NewMetadataRepositoryServiceClient(rpcConn.Conn),
	}
	return client, nil
}

func (c *metadataRepositoryClient) Close() error {
	return c.rpcConn.Close()
}

func (c *metadataRepositoryClient) RegisterStorageNode(ctx context.Context, sn *varlogpb.StorageNodeDescriptor) error {
	if !sn.Valid() {
		return verrors.ErrInvalid
	}

	req := &mrpb.StorageNodeRequest{
		StorageNode: sn,
	}

	_, err := c.client.RegisterStorageNode(ctx, req)
	return verrors.ToErr(ctx, err)
}

func (c *metadataRepositoryClient) UnregisterStorageNode(ctx context.Context, snID types.StorageNodeID) error {
	req := &mrpb.StorageNodeRequest{
		StorageNode: &varlogpb.StorageNodeDescriptor{
			StorageNodeID: snID,
		},
	}

	_, err := c.client.UnregisterStorageNode(ctx, req)
	return verrors.ToErr(ctx, err)
}

func (c *metadataRepositoryClient) RegisterLogStream(ctx context.Context, ls *varlogpb.LogStreamDescriptor) error {
	if !ls.Valid() {
		return verrors.ErrInvalid
	}

	req := &mrpb.LogStreamRequest{
		LogStream: ls,
	}
	_, err := c.client.RegisterLogStream(ctx, req)
	return verrors.ToErr(ctx, err)
}

func (c *metadataRepositoryClient) UnregisterLogStream(ctx context.Context, lsID types.LogStreamID) error {
	req := &mrpb.LogStreamRequest{
		LogStream: &varlogpb.LogStreamDescriptor{
			LogStreamID: lsID,
		},
	}
	_, err := c.client.UnregisterLogStream(ctx, req)
	return verrors.ToErr(ctx, err)
}

func (c *metadataRepositoryClient) UpdateLogStream(ctx context.Context, ls *varlogpb.LogStreamDescriptor) error {
	if !ls.Valid() {
		return verrors.ErrInvalid
	}

	req := &mrpb.LogStreamRequest{
		LogStream: ls,
	}
	_, err := c.client.UpdateLogStream(ctx, req)
	return verrors.ToErr(ctx, err)
}

func (c *metadataRepositoryClient) GetMetadata(ctx context.Context) (*varlogpb.MetadataDescriptor, error) {
	rsp, err := c.client.GetMetadata(ctx, &mrpb.GetMetadataRequest{})
	if err != nil {
		return nil, verrors.ToErr(ctx, err)
	}
	return rsp.GetMetadata(), nil
}

func (c *metadataRepositoryClient) Seal(ctx context.Context, lsID types.LogStreamID) (types.GLSN, error) {
	rsp, err := c.client.Seal(ctx, &mrpb.SealRequest{LogStreamID: lsID})
	if err != nil {
		return types.InvalidGLSN, verrors.ToErr(ctx, err)
	}

	return rsp.LastCommittedGLSN, nil
}

func (c *metadataRepositoryClient) Unseal(ctx context.Context, lsID types.LogStreamID) error {
	_, err := c.client.Unseal(ctx, &mrpb.UnsealRequest{LogStreamID: lsID})
	if err != nil {
		return verrors.ToErr(ctx, err)
	}
	return nil
}
