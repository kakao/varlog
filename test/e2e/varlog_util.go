package e2e

import (
	"context"

	"go.uber.org/zap"

	"github.com/kakao/varlog/pkg/mrc"
	"github.com/kakao/varlog/pkg/snc"
	vtypes "github.com/kakao/varlog/pkg/types"
)

type VarlogIDGetter interface {
	MetadataRepositoryID(ctx context.Context, addr string) (vtypes.ClusterID, vtypes.NodeID, error)
	StorageNodeID(ctx context.Context, addr string, clusterID vtypes.ClusterID) (vtypes.StorageNodeID, error)
}

type varlogIDGetter struct{}

func (v *varlogIDGetter) MetadataRepositoryID(ctx context.Context, addr string) (vtypes.ClusterID, vtypes.NodeID, error) {
	cli, err := mrc.NewMetadataRepositoryManagementClient(addr)
	if err != nil {
		return vtypes.ClusterID(0), vtypes.InvalidNodeID, err
	}
	defer cli.Close()

	cinfo, err := cli.GetClusterInfo(ctx, vtypes.ClusterID(0))
	if err != nil {
		return vtypes.ClusterID(0), vtypes.InvalidNodeID, err
	}

	return cinfo.ClusterInfo.ClusterID, cinfo.ClusterInfo.NodeID, nil
}

func (v *varlogIDGetter) StorageNodeID(ctx context.Context, addr string, clusterID vtypes.ClusterID) (vtypes.StorageNodeID, error) {
	cli, err := snc.NewManagementClient(ctx, clusterID, addr, zap.NewNop())
	if err != nil {
		return vtypes.StorageNodeID(0), err
	}
	defer cli.Close()

	return cli.PeerStorageNodeID(), nil
}
