package storagenode

import (
	"context"
	"errors"

	pbtypes "github.com/gogo/protobuf/types"
	"go.opentelemetry.io/otel/codes"
	oteltrace "go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"google.golang.org/grpc"

	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/pkg/util/telemetry/trace"
	"github.com/kakao/varlog/pkg/verrors"
	"github.com/kakao/varlog/proto/snpb"
	"github.com/kakao/varlog/proto/varlogpb"
)

type managementService struct {
	m      Management
	tmStub *telemetryStub
	logger *zap.Logger
}

var _ snpb.ManagementServer = (*managementService)(nil)

func NewManagementService(m Management, tmStub *telemetryStub, logger *zap.Logger) *managementService {
	if logger == nil {
		logger = zap.NewNop()
	}
	logger = logger.Named("managementservice")
	return &managementService{
		logger: logger,
		tmStub: tmStub,
		m:      m,
	}
}

func (s *managementService) Register(server *grpc.Server) {
	s.logger.Info("register to rpc server")
	snpb.RegisterManagementServer(server, s)
}

type handler func(ctx context.Context, req interface{}) (rsp interface{}, err error)

func (s *managementService) withTelemetry(ctx context.Context, spanName string, req interface{}, h handler) (rsp interface{}, err error) {
	// TODO: use resource to tag storage node id
	ctx, span := s.tmStub.startSpan(ctx, spanName,
		oteltrace.WithAttributes(trace.StorageNodeIDLabel(s.m.StorageNodeID())),
		oteltrace.WithSpanKind(oteltrace.SpanKindServer),
	)
	if cidGetter, ok := req.(interface{ GetClusterID() types.ClusterID }); ok {
		if cidGetter.GetClusterID() != s.m.ClusterID() {
			err = errors.New("storagenode: invalid ClusterID")
			goto out
		}
	}
	if snidGetter, ok := req.(interface{ GetStorageNodeID() types.StorageNodeID }); ok {
		if snidGetter.GetStorageNodeID() != s.m.StorageNodeID() {
			err = errors.New("storagenode: invalid StorageNodeID")
			goto out
		}
	}
	rsp, err = h(ctx, req)
	// FIXME (jun): This is meaningless line.
	s.tmStub.metrics().requests.Add(ctx, 1)
out:
	if err == nil {
		span.SetStatus(codes.Ok, "")
	} else {
		span.RecordError(err)
		s.logger.Error(spanName, zap.Error(err))
	}
	span.End()
	return rsp, err
}

// GetMetadata implements the ManagementServer GetMetadata method.
func (s *managementService) GetMetadata(ctx context.Context, req *snpb.GetMetadataRequest) (*snpb.GetMetadataResponse, error) {
	rspI, err := s.withTelemetry(ctx, "varlog.snpb.Management/GetMetadata", req,
		func(ctx context.Context, _ interface{}) (interface{}, error) {
			metadata, err := s.m.GetMetadata(ctx)
			rsp := &snpb.GetMetadataResponse{StorageNodeMetadata: metadata}
			return rsp, err
		},
	)
	if err != nil {
		return nil, verrors.ToStatusError(err)
	}
	return rspI.(*snpb.GetMetadataResponse), nil
}

// AddLogStream implements the ManagementServer AddLogStream method.
func (s *managementService) AddLogStream(ctx context.Context, req *snpb.AddLogStreamRequest) (*snpb.AddLogStreamResponse, error) {
	rspI, err := s.withTelemetry(ctx, "varlog.snpb.Management/AddLogStream", req,
		func(ctx context.Context, reqI interface{}) (interface{}, error) {
			req := reqI.(*snpb.AddLogStreamRequest)
			path, err := s.m.AddLogStream(ctx, req.GetLogStreamID(), req.GetStorage().GetPath())
			return &snpb.AddLogStreamResponse{
				LogStream: &varlogpb.LogStreamDescriptor{
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
	return rspI.(*snpb.AddLogStreamResponse), nil
}

// RemoveLogStream implements the ManagementServer RemoveLogStream method.
func (s *managementService) RemoveLogStream(ctx context.Context, req *snpb.RemoveLogStreamRequest) (*pbtypes.Empty, error) {
	rspI, err := s.withTelemetry(ctx, "varlog.snpb.Management/RemoveLogStream", req,
		func(ctx context.Context, reqI interface{}) (interface{}, error) {
			req := reqI.(*snpb.RemoveLogStreamRequest)
			err := s.m.RemoveLogStream(ctx, req.GetLogStreamID())
			return &pbtypes.Empty{}, err
		},
	)
	if err != nil {
		return nil, verrors.ToStatusError(err)
	}
	return rspI.(*pbtypes.Empty), nil
}

// Seal implements the ManagementServer Seal method.
func (s *managementService) Seal(ctx context.Context, req *snpb.SealRequest) (*snpb.SealResponse, error) {
	rspI, err := s.withTelemetry(ctx, "varlog.snpb.Management/Seal", req,
		func(ctx context.Context, reqI interface{}) (interface{}, error) {
			req := reqI.(*snpb.SealRequest)
			status, maxGLSN, err := s.m.Seal(ctx, req.GetLogStreamID(), req.GetLastCommittedGLSN())
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
func (s *managementService) Unseal(ctx context.Context, req *snpb.UnsealRequest) (*pbtypes.Empty, error) {
	rspI, err := s.withTelemetry(ctx, "varlog.snpb.Management/Unseal", req,
		func(ctx context.Context, reqI interface{}) (interface{}, error) {
			req := reqI.(*snpb.UnsealRequest)
			err := s.m.Unseal(ctx, req.GetLogStreamID())
			return &pbtypes.Empty{}, err
		},
	)
	if err != nil {
		return nil, verrors.ToStatusError(err)
	}
	return rspI.(*pbtypes.Empty), nil
}

// Sync implements the ManagementServer Sync method.
func (s *managementService) Sync(ctx context.Context, req *snpb.SyncRequest) (*snpb.SyncResponse, error) {
	rspI, err := s.withTelemetry(ctx, "varlog.snpb.Management/Sync", req,
		func(ctx context.Context, reqI interface{}) (interface{}, error) {
			req := reqI.(*snpb.SyncRequest)
			replica := Replica{
				StorageNodeID: req.GetBackup().GetStorageNodeID(),
				LogStreamID:   req.GetLogStreamID(),
				Address:       req.GetBackup().GetAddress(),
			}
			status, err := s.m.Sync(ctx, req.GetLogStreamID(), replica, req.GetLastGLSN())
			return &snpb.SyncResponse{Status: status}, err
		},
	)
	if err != nil {
		return nil, verrors.ToStatusError(err)
	}
	return rspI.(*snpb.SyncResponse), nil
}
