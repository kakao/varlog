package mrc

//go:generate go tool mockgen -build_flags -mod=vendor -self_package github.com/kakao/varlog/pkg/mrc -package mrc -destination metadata_repository_management_client_mock.go . MetadataRepositoryManagementClient

import (
	"context"
	"fmt"

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
		return fmt.Errorf("mrmcl: %w", verrors.ErrInvalid)
	}

	if nodeID != types.NewNodeIDFromURL(url) {
		return fmt.Errorf("mrmcl: %w", verrors.ErrInvalid)
	}

	req := &mrpb.AddPeerRequest{
		// For backward compatibility, we allow the request to contain Peer and ClusterID/NodeID/Url.
		//
		// TODO: Remove this backward compatibility code in the future.
		ClusterID: clusterID,
		NodeID:    nodeID,
		Url:       url,
		Peer: mrpb.PeerInfo{
			ClusterID: clusterID,
			NodeID:    nodeID,
			URL:       url,
		},
	}

	if _, err := c.client.AddPeer(ctx, req); err != nil {
		return fmt.Errorf("mrmcl: %w", verrors.FromStatusError(err))
	}
	return nil
}

func (c *metadataRepositoryManagementClient) RemovePeer(ctx context.Context, clusterID types.ClusterID, nodeID types.NodeID) error {
	if nodeID == types.InvalidNodeID {
		return fmt.Errorf("mrmcl: %w", verrors.ErrInvalid)
	}

	req := &mrpb.RemovePeerRequest{
		ClusterID: clusterID,
		NodeID:    nodeID,
	}

	if _, err := c.client.RemovePeer(ctx, req); err != nil {
		return fmt.Errorf("mrmcl: %w", verrors.FromStatusError(err))
	}
	return nil
}

func (c *metadataRepositoryManagementClient) GetClusterInfo(ctx context.Context, clusterID types.ClusterID) (*mrpb.GetClusterInfoResponse, error) {
	req := &mrpb.GetClusterInfoRequest{
		ClusterID: clusterID,
	}

	rsp, err := c.client.GetClusterInfo(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("mrmcl: %w", verrors.FromStatusError(err))
	}
	return rsp, nil
}
