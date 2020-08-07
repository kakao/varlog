package varlog

import (
	"context"

	"github.com/kakao/varlog/pkg/varlog/types"
	"github.com/kakao/varlog/pkg/varlog/util/stringsutil"
	pb "github.com/kakao/varlog/proto/storage_node"
	vpb "github.com/kakao/varlog/proto/varlog"
)

type ManagementClient interface {
}

type managementClient struct {
	rpcConn   *RpcConn
	rpcClient pb.ManagementClient
}

func (c *managementClient) GetMetadata(ctx context.Context, metadataType pb.MetadataType) (*vpb.StorageNodeMetadataDescriptor, error) {
	rsp, err := c.rpcClient.GetMetadata(ctx, &pb.GetMetadataRequest{
		MetadataType: metadataType,
	})
	if err != nil {
		return nil, err
	}
	return rsp.GetStorageNodeMetadata(), nil
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

func (c *managementClient) Seal(ctx context.Context, cid types.ClusterID, snid types.StorageNodeID, lsid types.LogStreamID, lastCommittedGLSN types.GLSN) (vpb.LogStreamStatus, types.GLSN, error) {
	// TODO(jun): Check ranges CID, SNID and LSID
	rsp, err := c.rpcClient.Seal(ctx, &pb.SealRequest{
		ClusterID:         cid,
		StorageNodeID:     snid,
		LogStreamID:       lsid,
		LastCommittedGLSN: lastCommittedGLSN,
	})
	return rsp.GetStatus(), rsp.GetLastCommittedGLSN(), err
}

func (c *managementClient) Unseal(ctx context.Context, cid types.ClusterID, snid types.StorageNodeID, lsid types.LogStreamID) error {
	// TODO(jun): Check ranges CID, SNID and LSID
	_, err := c.rpcClient.Unseal(ctx, &pb.UnsealRequest{
		ClusterID:     cid,
		StorageNodeID: snid,
		LogStreamID:   lsid,
	})
	return err
}

func (c *managementClient) Sync(ctx context.Context) error {
	panic("not yet implemented")
}
