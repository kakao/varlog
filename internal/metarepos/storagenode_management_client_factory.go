package metarepos

import (
	"context"

	"go.uber.org/zap"

	"github.com/kakao/varlog/pkg/snc"
	"github.com/kakao/varlog/pkg/types"
)

type StorageNodeManagementClientFactory interface {
	GetManagementClient(context.Context, types.ClusterID, string, *zap.Logger) (snc.StorageNodeManagementClient, error)
}
