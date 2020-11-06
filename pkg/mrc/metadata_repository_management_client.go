package mrc

import (
	"context"

	"github.daumkakao.com/varlog/varlog/pkg/rpc"
	"github.daumkakao.com/varlog/varlog/pkg/types"
	"github.daumkakao.com/varlog/varlog/pkg/verrors"
	"github.daumkakao.com/varlog/varlog/proto/mrpb"
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

func NewMetadataRepositoryManagementClient(address string) (MetadataRepositoryManagementClient, error) {
	rpcConn, err := rpc.NewConn(address)
	if err != nil {
		return nil, err
	}
	return NewMetadataRepositoryManagementClientFromRpcConn(rpcConn)
}

func NewMetadataRepositoryManagementClientFromRpcConn(rpcConn *rpc.Conn) (MetadataRepositoryManagementClient, error) {
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
		return verrors.ErrInvalid
	}

	if nodeID != types.NewNodeIDFromURL(url) {
		return verrors.ErrInvalid
	}

	req := &mrpb.AddPeerRequest{
		ClusterID: clusterID,
		NodeID:    nodeID,
		Url:       url,
	}

	_, err := c.client.AddPeer(ctx, req)
	return verrors.ToErr(ctx, err)
}

func (c *metadataRepositoryManagementClient) RemovePeer(ctx context.Context, clusterID types.ClusterID, nodeID types.NodeID) error {
	if nodeID == types.InvalidNodeID {
		return verrors.ErrInvalid
	}

	req := &mrpb.RemovePeerRequest{
		ClusterID: clusterID,
		NodeID:    nodeID,
	}

	_, err := c.client.RemovePeer(ctx, req)
	return verrors.ToErr(ctx, err)
}

func (c *metadataRepositoryManagementClient) GetClusterInfo(ctx context.Context, clusterID types.ClusterID) (*mrpb.GetClusterInfoResponse, error) {
	req := &mrpb.GetClusterInfoRequest{
		ClusterID: clusterID,
	}

	return c.client.GetClusterInfo(ctx, req)
}
