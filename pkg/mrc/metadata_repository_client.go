package mrc

//go:generate go tool mockgen -build_flags -mod=vendor -self_package github.com/kakao/varlog/pkg/mrc -package mrc -destination metadata_repository_client_mock.go . MetadataRepositoryClient

import (
	"context"

	"github.com/pkg/errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/kakao/varlog/pkg/rpc"
	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/pkg/verrors"
	"github.com/kakao/varlog/proto/mrpb"
	"github.com/kakao/varlog/proto/varlogpb"
)

type MetadataRepositoryClient interface {
	RegisterStorageNode(context.Context, *varlogpb.StorageNodeDescriptor) error
	UnregisterStorageNode(context.Context, types.StorageNodeID) error
	RegisterTopic(context.Context, types.TopicID) error
	UnregisterTopic(context.Context, types.TopicID) error
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

// FIXME (jun, pharrell): Use context or timeout, or remove health check.
func NewMetadataRepositoryClient(ctx context.Context, address string) (MetadataRepositoryClient, error) {
	rpcConn, err := rpc.NewConn(ctx, address)
	if err != nil {
		return nil, err
	}
	return NewMetadataRepositoryClientFromRPCConn(rpcConn)
}

func NewMetadataRepositoryClientFromRPCConn(rpcConn *rpc.Conn) (MetadataRepositoryClient, error) {
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
		return errors.WithStack(verrors.ErrInvalid)
	}

	req := &mrpb.StorageNodeRequest{
		StorageNode: sn,
	}

	_, err := c.client.RegisterStorageNode(ctx, req)
	return verrors.FromStatusError(errors.WithStack(err))
}

func (c *metadataRepositoryClient) UnregisterStorageNode(ctx context.Context, snID types.StorageNodeID) error {
	req := &mrpb.StorageNodeRequest{
		StorageNode: &varlogpb.StorageNodeDescriptor{
			StorageNode: varlogpb.StorageNode{
				StorageNodeID: snID,
			},
		},
	}

	_, err := c.client.UnregisterStorageNode(ctx, req)
	return verrors.FromStatusError(errors.WithStack(err))
}

func (c *metadataRepositoryClient) RegisterTopic(ctx context.Context, topicID types.TopicID) error {
	req := &mrpb.TopicRequest{
		TopicID: topicID,
	}

	_, err := c.client.RegisterTopic(ctx, req)
	if err != nil {
		if code := status.Code(err); code == codes.ResourceExhausted {
			return err
		}
		return verrors.FromStatusError(errors.WithStack(err))
	}
	return nil
}

func (c *metadataRepositoryClient) UnregisterTopic(ctx context.Context, topicID types.TopicID) error {
	req := &mrpb.TopicRequest{
		TopicID: topicID,
	}

	_, err := c.client.UnregisterTopic(ctx, req)
	return verrors.FromStatusError(errors.WithStack(err))
}

func (c *metadataRepositoryClient) RegisterLogStream(ctx context.Context, ls *varlogpb.LogStreamDescriptor) error {
	if !ls.Valid() {
		return errors.WithStack(verrors.ErrInvalid)
	}

	req := &mrpb.LogStreamRequest{
		LogStream: ls,
	}
	_, err := c.client.RegisterLogStream(ctx, req)
	if err != nil {
		if code := status.Code(err); code == codes.ResourceExhausted {
			return err
		}
		return verrors.FromStatusError(errors.WithStack(err))
	}
	return nil
}

func (c *metadataRepositoryClient) UnregisterLogStream(ctx context.Context, lsID types.LogStreamID) error {
	req := &mrpb.LogStreamRequest{
		LogStream: &varlogpb.LogStreamDescriptor{
			LogStreamID: lsID,
		},
	}
	_, err := c.client.UnregisterLogStream(ctx, req)
	return verrors.FromStatusError(errors.WithStack(err))
}

func (c *metadataRepositoryClient) UpdateLogStream(ctx context.Context, ls *varlogpb.LogStreamDescriptor) error {
	if !ls.Valid() {
		return errors.WithStack(verrors.ErrInvalid)
	}

	req := &mrpb.LogStreamRequest{
		LogStream: ls,
	}
	_, err := c.client.UpdateLogStream(ctx, req)
	return verrors.FromStatusError(errors.WithStack(err))
}

func (c *metadataRepositoryClient) GetMetadata(ctx context.Context) (*varlogpb.MetadataDescriptor, error) {
	rsp, err := c.client.GetMetadata(ctx, &mrpb.GetMetadataRequest{})
	if err != nil {
		return nil, verrors.FromStatusError(errors.WithStack(err))
	}
	return rsp.GetMetadata(), nil
}

func (c *metadataRepositoryClient) Seal(ctx context.Context, lsID types.LogStreamID) (types.GLSN, error) {
	rsp, err := c.client.Seal(ctx, &mrpb.SealRequest{LogStreamID: lsID})
	if err != nil {
		return types.InvalidGLSN, verrors.FromStatusError(errors.WithStack(err))
	}

	return rsp.LastCommittedGLSN, nil
}

func (c *metadataRepositoryClient) Unseal(ctx context.Context, lsID types.LogStreamID) error {
	_, err := c.client.Unseal(ctx, &mrpb.UnsealRequest{LogStreamID: lsID})
	if err != nil {
		return verrors.FromStatusError(errors.WithStack(err))
	}
	return nil
}
