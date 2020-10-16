package varlog

import (
	"context"

	types "github.daumkakao.com/varlog/varlog/pkg/varlog/types"
	vpb "github.daumkakao.com/varlog/varlog/proto/varlog"
	"github.daumkakao.com/varlog/varlog/proto/vmspb"
)

type ClusterManagerClient interface {
	AddStorageNode(ctx context.Context, addr string) (*vpb.StorageNodeMetadataDescriptor, error)
	AddLogStream(ctx context.Context, logStreamReplicas []*vpb.ReplicaDescriptor) (*vpb.LogStreamDescriptor, error)
	Seal(ctx context.Context, logStreamID types.LogStreamID) ([]vpb.LogStreamMetadataDescriptor, error)
	Close() error
}

var _ ClusterManagerClient = (*clusterManagerClient)(nil)

type clusterManagerClient struct {
	rpcConn   *RpcConn
	rpcClient vmspb.ClusterManagerClient
}

func NewClusterManagerClient(addr string) (ClusterManagerClient, error) {
	rpcConn, err := NewRpcConn(addr)
	if err != nil {
		return nil, err
	}
	cli := &clusterManagerClient{
		rpcConn:   rpcConn,
		rpcClient: vmspb.NewClusterManagerClient(rpcConn.Conn),
	}
	return cli, nil
}

func (c *clusterManagerClient) Close() error {
	return c.rpcConn.Close()
}

func (c *clusterManagerClient) AddStorageNode(ctx context.Context, addr string) (*vpb.StorageNodeMetadataDescriptor, error) {
	rsp, err := c.rpcClient.AddStorageNode(ctx, &vmspb.AddStorageNodeRequest{Address: addr})
	if err != nil {
		return nil, FromStatusError(ctx, err)
	}
	return rsp.StorageNode, nil
}

func (c *clusterManagerClient) AddLogStream(ctx context.Context, logStreamReplicas []*vpb.ReplicaDescriptor) (*vpb.LogStreamDescriptor, error) {
	rsp, err := c.rpcClient.AddLogStream(ctx, &vmspb.AddLogStreamRequest{Replicas: logStreamReplicas})
	if err != nil {
		return nil, FromStatusError(ctx, err)
	}
	return rsp.GetLogStream(), nil
}

func (c *clusterManagerClient) Seal(ctx context.Context, logStreamID types.LogStreamID) ([]vpb.LogStreamMetadataDescriptor, error) {
	rsp, err := c.rpcClient.Seal(ctx, &vmspb.SealRequest{LogStreamID: logStreamID})
	if err != nil {
		return nil, FromStatusError(ctx, err)
	}
	return rsp.GetLogStreams(), nil
}
