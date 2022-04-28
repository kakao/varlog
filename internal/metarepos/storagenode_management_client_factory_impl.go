package metarepos

import (
	"context"

	"go.uber.org/zap"

	"github.daumkakao.com/varlog/varlog/pkg/snc"
	"github.daumkakao.com/varlog/varlog/pkg/types"
)

type storageNodeManagementClientFactory struct {
}

func NewStorageNodeManagementClientFactory() *storageNodeManagementClientFactory {
	return &storageNodeManagementClientFactory{}
}

func (rcf *storageNodeManagementClientFactory) GetManagementClient(ctx context.Context, clusterID types.ClusterID, address string, logger *zap.Logger) (snc.StorageNodeManagementClient, error) {
	return snc.NewManagementClient(ctx, clusterID, address, logger)
}
