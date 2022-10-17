package cluster

import (
	"context"
	"testing"

	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/tests/ee/controller"
)

type Cluster interface {
	Setup(ctx context.Context, t *testing.T)

	StartAdminServer(ctx context.Context, t *testing.T)
	StopAdminServer(ctx context.Context, t *testing.T)
	AdminServerAddress(ctx context.Context, t *testing.T) string

	StartMetadataRepositoryNodes(ctx context.Context, t *testing.T, desired int)
	StopMetadataRepositoryNodes(ctx context.Context, t *testing.T)
	SetNumMetadataRepositories(ctx context.Context, t *testing.T, desired int)
	MetadataRepositoryAddress(ctx context.Context, t *testing.T) string

	StartStorageNodes(ctx context.Context, t *testing.T, desired int)
	StopStorageNodes(ctx context.Context, t *testing.T)
	StartStorageNode(ctx context.Context, t *testing.T, nodeName string) bool
	StopStorageNode(ctx context.Context, t *testing.T, snid types.StorageNodeID) (nodeName string)

	Controller() *controller.Controller

	String() string

	Close(ctx context.Context, t *testing.T)
}
