package mrc

//go:generate mockgen -build_flags -mod=vendor -self_package github.com/kakao/varlog/pkg/mrc -package mrc -destination metadata_repository_management_client_mock.go . MetadataRepositoryManagementClient

import (
	"context"

	"github.com/pkg/errors"
	"go.uber.org/multierr"
	"google.golang.org/grpc/health/grpc_health_v1"

	"github.com/kakao/varlog/pkg/rpc"
	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/pkg/verrors"
	"github.com/kakao/varlog/proto/mrpb"
)

type MetadataRepositoryManagementClient interface {
	AddPeer(ctx context.Context, clusterID types.ClusterID, nodeID types.NodeID, url string) error
	RemovePeer(ctx context.Context, clusterID types.ClusterID, nodeID types.NodeID) error
	GetClusterInfo(ctx context.Context, clusterID types.ClusterID) (*mrpb.GetClusterInfoResponse, error)
	Close() error
}

type metadataRepositoryManagementClient struct {
	rpcConn *rpc.Conn
	client  mrpb.ManagementClient
}

func NewMetadataRepositoryManagementClient(ctx context.Context, address string) (MetadataRepositoryManagementClient, error) {
	rpcConn, err := rpc.NewConn(ctx, address)
	if err != nil {
		return nil, err
	}

	client := grpc_health_v1.NewHealthClient(rpcConn.Conn)
	rsp, err := client.Check(ctx, &grpc_health_v1.HealthCheckRequest{})
	if err != nil {
		err = errors.Wrapf(err, "mrmcl: addr = %s", address)
		return nil, multierr.Append(err, rpcConn.Close())
	}
	status := rsp.GetStatus()
	if status != grpc_health_v1.HealthCheckResponse_SERVING {
		err := errors.Errorf("mrmcl: not ready (%+v)", status)
		return nil, multierr.Append(err, rpcConn.Close())
	}

	return NewMetadataRepositoryManagementClientFromRPCConn(rpcConn)
}

func NewMetadataRepositoryManagementClientFromRPCConn(rpcConn *rpc.Conn) (MetadataRepositoryManagementClient, error) {
	c := &metadataRepositoryManagementClient{
		rpcConn: rpcConn,
		client:  mrpb.NewManagementClient(rpcConn.Conn),
	}
	return c, nil
}

func (c *metadataRepositoryManagementClient) Close() error {
	return c.rpcConn.Close()
}

func (c *metadataRepositoryManagementClient) AddPeer(ctx context.Context, clusterID types.ClusterID, nodeID types.NodeID, url string) error {
	if len(url) == 0 || nodeID == types.InvalidNodeID {
		return errors.Wrap(verrors.ErrInvalid, "mrmcl")
	}

	if nodeID != types.NewNodeIDFromURL(url) {
		return errors.Wrap(verrors.ErrInvalid, "mrmcl")
	}

	req := &mrpb.AddPeerRequest{
		ClusterID: clusterID,
		NodeID:    nodeID,
		Url:       url,
	}

	_, err := c.client.AddPeer(ctx, req)
	return errors.Wrap(verrors.FromStatusError(err), "mrmcl")
}

func (c *metadataRepositoryManagementClient) RemovePeer(ctx context.Context, clusterID types.ClusterID, nodeID types.NodeID) error {
	if nodeID == types.InvalidNodeID {
		return errors.Wrap(verrors.ErrInvalid, "mrmcl")
	}

	req := &mrpb.RemovePeerRequest{
		ClusterID: clusterID,
		NodeID:    nodeID,
	}

	_, err := c.client.RemovePeer(ctx, req)
	return errors.Wrap(verrors.FromStatusError(err), "mrmcl")
}

func (c *metadataRepositoryManagementClient) GetClusterInfo(ctx context.Context, clusterID types.ClusterID) (*mrpb.GetClusterInfoResponse, error) {
	req := &mrpb.GetClusterInfoRequest{
		ClusterID: clusterID,
	}

	rsp, err := c.client.GetClusterInfo(ctx, req)
	return rsp, errors.Wrap(verrors.FromStatusError(err), "mrmcl")
}
