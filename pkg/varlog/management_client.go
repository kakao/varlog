package varlog

import (
	"context"

	"github.daumkakao.com/varlog/varlog/pkg/varlog/types"
	"github.daumkakao.com/varlog/varlog/pkg/varlog/util/stringsutil"
	pb "github.daumkakao.com/varlog/varlog/proto/storage_node"
	vpb "github.daumkakao.com/varlog/varlog/proto/varlog"
)

type ManagementClient interface {
}

type managementClient struct {
	rpcConn   *RpcConn
	rpcClient pb.ManagementClient
}

func (c *managementClient) GetMetadata(ctx context.Context) error {
	panic("not yet implemented")
}

func (c *managementClient) AddLogStream(ctx context.Context, cid types.ClusterID, snid types.StorageNodeID, lsid types.LogStreamID, path string) error {
	// TODO(jun): Check ranges CID, SNID and LSID
	if stringsutil.Empty(path) {
		return ErrInvalid // FIXME: ErrInvalid ErrInvalidArgument
	}
	// FIXME(jun): Does the return value of AddLogStream need?
	_, err := c.rpcClient.AddLogStream(ctx, &pb.AddLogStreamRequest{
		ClusterID:     cid,
		StorageNodeID: snid,
		LogStreamID:   lsid,
		Storage: &vpb.StorageDescriptor{
			Path: path,
		},
	})
	return err

}

func (c *managementClient) RemoveLogStream(ctx context.Context, cid types.ClusterID, snid types.StorageNodeID, lsid types.LogStreamID) error {
	// TODO(jun): Check ranges CID, SNID and LSID
	_, err := c.rpcClient.RemoveLogStream(ctx, &pb.RemoveLogStreamRequest{
		ClusterID:     cid,
		StorageNodeID: snid,
		LogStreamID:   lsid,
	})
	return err
}

func (c *managementClient) Seal(ctx context.Context) error {
	panic("not yet implemented")
}

func (c *managementClient) Unseal(ctx context.Context) error {
	panic("not yet implemented")
}

func (c *managementClient) Sync(ctx context.Context) error {
	panic("not yet implemented")
}
