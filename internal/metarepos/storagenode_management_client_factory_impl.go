package metarepos

import (
	"context"

	"go.uber.org/zap"

	"github.com/kakao/varlog/pkg/snc"
	"github.com/kakao/varlog/pkg/types"
)

type storageNodeManagementClientFactory struct {
}

func NewStorageNodeManagementClientFactory() *storageNodeManagementClientFactory {
	return &storageNodeManagementClientFactory{}
}

func (rcf *storageNodeManagementClientFactory) GetManagementClient(ctx context.Context, clusterID types.ClusterID, address string, logger *zap.Logger) (snc.StorageNodeManagementClient, error) {
	return snc.NewManagementClient(ctx, clusterID, address, logger)
}
