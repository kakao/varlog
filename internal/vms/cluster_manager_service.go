package vms

import (
	"context"

	"github.com/kakao/varlog/pkg/varlog"
	"github.com/kakao/varlog/proto/vmspb"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

type clusterManagerService struct {
	clusManager ClusterManager
	logger      *zap.Logger
}

var _ vmspb.ClusterManagerServer = (*clusterManagerService)(nil)

func NewClusterManagerService(clusterManager ClusterManager, logger *zap.Logger) *clusterManagerService {
	if logger == nil {
		logger = zap.NewNop()
	}
	logger = logger.Named("vmsservice")
	return &clusterManagerService{
		clusManager: clusterManager,
		logger:      logger,
	}
}

func (s *clusterManagerService) Register(server *grpc.Server) {
	s.logger.Info("register to rpc server")
	vmspb.RegisterClusterManagerServer(server, s)
}

func (s *clusterManagerService) AddStorageNode(ctx context.Context, req *vmspb.AddStorageNodeRequest) (*vmspb.AddStorageNodeResponse, error) {
	snmeta, err := s.clusManager.AddStorageNode(ctx, req.GetAddress())
	return &vmspb.AddStorageNodeResponse{StorageNode: snmeta}, varlog.ToStatusError(err)
}

func (s *clusterManagerService) AddLogStream(context.Context, *vmspb.AddLogStreamRequest) (*vmspb.AddLogStreamResponse, error) {
	panic("not implemented")
}

func (s *clusterManagerService) Seal(context.Context, *vmspb.SealRequest) (*vmspb.SealResponse, error) {
	panic("not implemented")
}

func (s *clusterManagerService) Sync(context.Context, *vmspb.SyncRequest) (*vmspb.SyncResponse, error) {
	panic("not implemented")
}

func (s *clusterManagerService) Unseal(context.Context, *vmspb.UnsealRequest) (*vmspb.UnsealResponse, error) {
	panic("not implemented")
}
