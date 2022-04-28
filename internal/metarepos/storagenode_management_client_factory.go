package metarepos

import (
	"context"

	"go.uber.org/zap"

	"github.daumkakao.com/varlog/varlog/pkg/snc"
	"github.daumkakao.com/varlog/varlog/pkg/types"
)

type StorageNodeManagementClientFactory interface {
	GetManagementClient(context.Context, types.ClusterID, string, *zap.Logger) (snc.StorageNodeManagementClient, error)
}
