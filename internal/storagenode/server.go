package storagenode

import (
	"context"
	"fmt"

	pbtypes "github.com/gogo/protobuf/types"
	"github.com/pkg/errors"
	"go.opentelemetry.io/otel/codes"
	oteltrace "go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"google.golang.org/grpc"

	"github.com/kakao/varlog/internal/storagenode/rpcserver"
	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/pkg/util/telemetry/attribute"
	"github.com/kakao/varlog/pkg/verrors"
	"github.com/kakao/varlog/proto/snpb"
	"github.com/kakao/varlog/proto/varlogpb"
)

// Server is the interface that wraps methods for managing StorageNode.
type server struct {
	serverConfig
}

var _ snpb.ManagementServer = (*server)(nil)
var _ rpcserver.Registrable = (*server)(nil)

func NewServer(opts ...ServerOption) *server {
	return &server{serverConfig: newServerConfig(opts)}
}

func (s *server) Register(server *grpc.Server) {
	s.logger.Info("register to rpc server")
	snpb.RegisterManagementServer(server, s)
}

func (s *server) withTelemetry(ctx context.Context, spanName string, req interface{}, h rpcserver.Handler) (rsp interface{}, err error) {
	// TODO: use resource to tag storage node id
	ctx, span := s.tmStub.StartSpan(ctx, spanName,
		oteltrace.WithAttributes(attribute.StorageNodeID(s.storageNode.StorageNodeID())),
		oteltrace.WithSpanKind(oteltrace.SpanKindServer),
	)

	if cidGetter, ok := req.(interface{ GetClusterID() types.ClusterID }); ok {
		if cidGetter.GetClusterID() != s.storageNode.ClusterID() {
			err = errors.New("storagenode: invalid ClusterID")
			goto out
		}
	}
	if snidGetter, ok := req.(interface{ GetStorageNodeID() types.StorageNodeID }); ok {
		if snidGetter.GetStorageNodeID() != s.storageNode.StorageNodeID() {
			err = errors.New("storagenode: invalid StorageNodeID")
			goto out
		}
	}
	rsp, err = h(ctx, req)
out:
	if err == nil {
		span.SetStatus(codes.Ok, "")
		s.logger.Info(spanName,
			zap.Stringer("req", req.(fmt.Stringer)),
			zap.Stringer("rsp", rsp.(fmt.Stringer)),
		)
	} else {
		span.RecordError(err)
		s.logger.Error(spanName,
			zap.Error(err),
			zap.Stringer("req", req.(fmt.Stringer)),
		)
	}

	span.End()
	return rsp, err
}

// GetMetadata implements the ManagementServer GetMetadata method.
func (s *server) GetMetadata(ctx context.Context, req *snpb.GetMetadataRequest) (*snpb.GetMetadataResponse, error) {
	// NOTE: GetMetadata RPC is called very frequently since it plays the role of heartbeats as
	// well as metadata. So its telemetry and logging are suppressed.
	metadata, err := s.storageNode.GetMetadata(ctx)
	rsp := &snpb.GetMetadataResponse{StorageNodeMetadata: metadata}
	return rsp, verrors.ToStatusError(err)
}

// AddLogStream implements the ManagementServer AddLogStream method.
func (s *server) AddLogStreamReplica(ctx context.Context, req *snpb.AddLogStreamReplicaRequest) (*snpb.AddLogStreamReplicaResponse, error) {
	rspI, err := s.withTelemetry(ctx, "varlog.snpb.Server/AddLogStream", req,
		func(ctx context.Context, _ interface{}) (interface{}, error) {
			path, err := s.storageNode.AddLogStream(ctx, req.GetTopicID(), req.GetLogStreamID(), req.GetStorage().GetPath())
			return &snpb.AddLogStreamReplicaResponse{
				LogStream: &varlogpb.LogStreamDescriptor{
					TopicID:     req.GetTopicID(),
					LogStreamID: req.GetLogStreamID(),
					Status:      varlogpb.LogStreamStatusRunning,
					Replicas: []*varlogpb.ReplicaDescriptor{{
						StorageNodeID: req.GetStorageNodeID(),
						Path:          path,
					}},
				},
			}, err
		},
	)
	if err != nil {
		return nil, verrors.ToStatusError(err)
	}
	return rspI.(*snpb.AddLogStreamReplicaResponse), nil
}

// RemoveLogStream implements the ManagementServer RemoveLogStream method.
func (s *server) RemoveLogStream(ctx context.Context, req *snpb.RemoveLogStreamRequest) (*pbtypes.Empty, error) {
	rspI, err := s.withTelemetry(ctx, "varlog.snpb.Server/RemoveLogStream", req,
		func(ctx context.Context, _ interface{}) (interface{}, error) {
			err := s.storageNode.RemoveLogStream(ctx, req.GetTopicID(), req.GetLogStreamID())
			return &pbtypes.Empty{}, err
		},
	)
	if err != nil {
		return nil, verrors.ToStatusError(err)
	}
	return rspI.(*pbtypes.Empty), nil
}

// Seal implements the ManagementServer Seal method.
func (s *server) Seal(ctx context.Context, req *snpb.SealRequest) (*snpb.SealResponse, error) {
	rspI, err := s.withTelemetry(ctx, "varlog.snpb.Server/Seal", req,
		func(ctx context.Context, _ interface{}) (interface{}, error) {
			status, maxGLSN, err := s.storageNode.Seal(ctx, req.GetTopicID(), req.GetLogStreamID(), req.GetLastCommittedGLSN())
			return &snpb.SealResponse{
				Status:            status,
				LastCommittedGLSN: maxGLSN,
			}, err
		},
	)
	if err != nil {
		return nil, verrors.ToStatusError(err)
	}
	return rspI.(*snpb.SealResponse), nil
}

// Unseal implements the ManagementServer Unseal method.
func (s *server) Unseal(ctx context.Context, req *snpb.UnsealRequest) (*pbtypes.Empty, error) {
	rspI, err := s.withTelemetry(ctx, "varlog.snpb.Server/Unseal", req,
		func(ctx context.Context, _ interface{}) (interface{}, error) {
			err := s.storageNode.Unseal(ctx, req.GetTopicID(), req.GetLogStreamID(), req.GetReplicas())
			return &pbtypes.Empty{}, err
		},
	)
	if err != nil {
		return nil, verrors.ToStatusError(err)
	}
	return rspI.(*pbtypes.Empty), nil
}

// Sync implements the ManagementServer Sync method.
func (s *server) Sync(ctx context.Context, req *snpb.SyncRequest) (*snpb.SyncResponse, error) {
	rspI, err := s.withTelemetry(ctx, "varlog.snpb.Server/Sync", req,
		func(ctx context.Context, _ interface{}) (interface{}, error) {
			replica := varlogpb.Replica{
				StorageNode: varlogpb.StorageNode{
					StorageNodeID: req.GetBackup().GetStorageNodeID(),
					Address:       req.GetBackup().GetAddress(),
				},
				TopicID:     req.GetTopicID(),
				LogStreamID: req.GetLogStreamID(),
			}
			status, err := s.storageNode.Sync(ctx, req.GetTopicID(), req.GetLogStreamID(), replica)
			return &snpb.SyncResponse{Status: status}, err
		},
	)
	if err != nil {
		return nil, verrors.ToStatusError(err)
	}
	return rspI.(*snpb.SyncResponse), nil
}

func (s *server) GetPrevCommitInfo(ctx context.Context, req *snpb.GetPrevCommitInfoRequest) (*snpb.GetPrevCommitInfoResponse, error) {
	rspI, err := s.withTelemetry(ctx, "varlog.snpb.Server/GetPrevCommitInfo", req,
		func(ctx context.Context, _ interface{}) (interface{}, error) {
			info, err := s.storageNode.GetPrevCommitInfo(ctx, req.GetPrevVersion())
			rsp := &snpb.GetPrevCommitInfoResponse{
				StorageNodeID: s.storageNode.StorageNodeID(),
				CommitInfos:   info,
			}
			return rsp, err
		},
	)
	if err != nil {
		return nil, verrors.ToStatusError(err)
	}
	return rspI.(*snpb.GetPrevCommitInfoResponse), nil
}
