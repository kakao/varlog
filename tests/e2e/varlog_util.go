package e2e

import (
	"context"

	"go.uber.org/zap"

	"github.daumkakao.com/varlog/varlog/pkg/mrc"
	"github.daumkakao.com/varlog/varlog/pkg/snc"
	"github.daumkakao.com/varlog/varlog/pkg/types"
)

type VarlogIDGetter interface {
	MetadataRepositoryID(ctx context.Context, addr string) (types.ClusterID, types.NodeID, error)
	StorageNodeID(ctx context.Context, addr string, clusterID types.ClusterID) (types.StorageNodeID, error)
}

type varlogIDGetter struct{}

func (v *varlogIDGetter) MetadataRepositoryID(ctx context.Context, addr string) (types.ClusterID, types.NodeID, error) {
	cli, err := mrc.NewMetadataRepositoryManagementClient(ctx, addr)
	if err != nil {
		return types.ClusterID(0), types.InvalidNodeID, err
	}
	defer cli.Close()

	cinfo, err := cli.GetClusterInfo(ctx, types.ClusterID(0))
	if err != nil {
		return types.ClusterID(0), types.InvalidNodeID, err
	}

	return cinfo.ClusterInfo.ClusterID, cinfo.ClusterInfo.NodeID, nil
}

func (v *varlogIDGetter) StorageNodeID(ctx context.Context, addr string, clusterID types.ClusterID) (types.StorageNodeID, error) {
	cli, err := snc.NewManagementClient(ctx, clusterID, addr, zap.NewNop())
	if err != nil {
		return types.StorageNodeID(0), err
	}
	defer cli.Close()

	return cli.PeerStorageNodeID(), nil
}
