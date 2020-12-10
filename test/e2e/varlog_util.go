package e2e

import (
	"context"

	"go.uber.org/zap"

	"github.daumkakao.com/varlog/varlog/pkg/mrc"
	"github.daumkakao.com/varlog/varlog/pkg/snc"
	vtypes "github.daumkakao.com/varlog/varlog/pkg/types"
)

type VarlogIDGetter interface {
	MetadataRepositoryID(string) (vtypes.ClusterID, vtypes.NodeID, error)
	StorageNodeID(string, vtypes.ClusterID) (vtypes.StorageNodeID, error)
}

type varlogIDGetter struct{}

func (v *varlogIDGetter) MetadataRepositoryID(addr string) (vtypes.ClusterID, vtypes.NodeID, error) {
	cli, err := mrc.NewMetadataRepositoryManagementClient(addr)
	if err != nil {
		return vtypes.ClusterID(0), vtypes.InvalidNodeID, err
	}
	defer cli.Close()

	cinfo, err := cli.GetClusterInfo(context.TODO(), vtypes.ClusterID(0))
	if err != nil {
		return vtypes.ClusterID(0), vtypes.InvalidNodeID, err
	}

	return cinfo.ClusterInfo.ClusterID, cinfo.ClusterInfo.NodeID, nil
}

func (v *varlogIDGetter) StorageNodeID(addr string, clusterID vtypes.ClusterID) (vtypes.StorageNodeID, error) {
	cli, err := snc.NewManagementClient(context.TODO(), clusterID, addr, zap.NewNop())
	if err != nil {
		return vtypes.StorageNodeID(0), err
	}
	defer cli.Close()

	return cli.PeerStorageNodeID(), nil
}
