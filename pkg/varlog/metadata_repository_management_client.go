package varlog

import (
	"context"

	"github.com/kakao/varlog/pkg/varlog/types"
	pb "github.com/kakao/varlog/proto/metadata_repository"
)

type MetadataRepositoryManagementClient interface {
	AddPeer(ctx context.Context, clusterID types.ClusterID, nodeID types.NodeID, url string) error
	RemovePeer(ctx context.Context, clusterID types.ClusterID, nodeID types.NodeID) error
	GetClusterInfo(ctx context.Context, clusterID types.ClusterID) (types.NodeID, []string, error)
	Close() error
}

type metadataRepositoryManagementClient struct {
	rpcConn *RpcConn
	client  pb.ManagementClient
}

func NewMetadataRepositoryManagementClient(address string) (MetadataRepositoryManagementClient, error) {
	rpcConn, err := NewRpcConn(address)
	if err != nil {
		return nil, err
	}
	return NewMetadataRepositoryManagementClientFromRpcConn(rpcConn)
}

func NewMetadataRepositoryManagementClientFromRpcConn(rpcConn *RpcConn) (MetadataRepositoryManagementClient, error) {
	c := &metadataRepositoryManagementClient{
		rpcConn: rpcConn,
		client:  pb.NewManagementClient(rpcConn.Conn),
	}
	return c, nil
}

func (c *metadataRepositoryManagementClient) Close() error {
	return c.rpcConn.Close()
}

func (c *metadataRepositoryManagementClient) AddPeer(ctx context.Context, clusterID types.ClusterID, nodeID types.NodeID, url string) error {
	if len(url) == 0 || nodeID == types.InvalidNodeID {
		return ErrInvalid
	}

	if nodeID != types.NewNodeIDFromURL(url) {
		return ErrInvalid
	}

	req := &pb.AddPeerRequest{
		ClusterID: clusterID,
		NodeID:    nodeID,
		Url:       url,
	}

	_, err := c.client.AddPeer(ctx, req)
	return ToErr(ctx, err)
}

func (c *metadataRepositoryManagementClient) RemovePeer(ctx context.Context, clusterID types.ClusterID, nodeID types.NodeID) error {
	if nodeID == types.InvalidNodeID {
		return ErrInvalid
	}

	req := &pb.RemovePeerRequest{
		ClusterID: clusterID,
		NodeID:    nodeID,
	}

	_, err := c.client.RemovePeer(ctx, req)
	return ToErr(ctx, err)
}

func (c *metadataRepositoryManagementClient) GetClusterInfo(ctx context.Context, clusterID types.ClusterID) (types.NodeID, []string, error) {
	req := &pb.GetClusterInfoRequest{
		ClusterID: clusterID,
	}

	rsp, err := c.client.GetClusterInfo(ctx, req)
	if err != nil {
		return types.InvalidNodeID, nil, ToErr(ctx, err)
	}

	return rsp.NodeID, rsp.GetUrls(), nil
}
