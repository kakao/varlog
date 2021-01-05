package storagenode

import (
	"context"

	pbtypes "github.com/gogo/protobuf/types"
	"go.opentelemetry.io/otel/codes"
	oteltrace "go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"google.golang.org/grpc"

	"github.daumkakao.com/varlog/varlog/pkg/util/telemetry/trace"
	"github.daumkakao.com/varlog/varlog/pkg/verrors"
	"github.daumkakao.com/varlog/varlog/proto/snpb"
	"github.daumkakao.com/varlog/varlog/proto/varlogpb"
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
	rsp, err = h(ctx, req)
	if err == nil {
		span.SetStatus(codes.Ok, "")
	} else {
		span.RecordError(err)
	}
	s.logger.Info(spanName, zap.Error(err))
	span.End()
	s.tmStub.metrics().requests.Add(ctx, 1)
	return rsp, err
}

// GetMetadata implements the ManagementServer GetMetadata method.
func (s *managementService) GetMetadata(ctx context.Context, req *snpb.GetMetadataRequest) (*snpb.GetMetadataResponse, error) {
	rspI, err := s.withTelemetry(ctx, "varlog.snpb.Management/GetMetadata", req,
		func(ctx context.Context, reqI interface{}) (interface{}, error) {
			req := reqI.(*snpb.GetMetadataRequest)
			metadata, err := s.m.GetMetadata(ctx, req.GetClusterID(), req.GetMetadataType())
			rsp := &snpb.GetMetadataResponse{StorageNodeMetadata: metadata}
			return rsp, err
		},
	)
	return rspI.(*snpb.GetMetadataResponse), verrors.ToStatusError(err)
}

// AddLogStream implements the ManagementServer AddLogStream method.
func (s *managementService) AddLogStream(ctx context.Context, req *snpb.AddLogStreamRequest) (*snpb.AddLogStreamResponse, error) {
	rspI, err := s.withTelemetry(ctx, "varlog.snpb.Management/AddLogStream", req,
		func(ctx context.Context, reqI interface{}) (interface{}, error) {
			req := reqI.(*snpb.AddLogStreamRequest)
			// TODO: too many arguments!!
			path, err := s.m.AddLogStream(ctx, req.GetClusterID(), req.GetStorageNodeID(), req.GetLogStreamID(), req.GetStorage().GetPath())
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
	return rspI.(*snpb.AddLogStreamResponse), verrors.ToStatusError(err)
}

// RemoveLogStream implements the ManagementServer RemoveLogStream method.
func (s *managementService) RemoveLogStream(ctx context.Context, req *snpb.RemoveLogStreamRequest) (*pbtypes.Empty, error) {
	rspI, err := s.withTelemetry(ctx, "varlog.snpb.Management/RemoveLogStream", req,
		func(ctx context.Context, reqI interface{}) (interface{}, error) {
			req := reqI.(*snpb.RemoveLogStreamRequest)
			err := s.m.RemoveLogStream(ctx, req.GetClusterID(), req.GetStorageNodeID(), req.GetLogStreamID())
			return &pbtypes.Empty{}, err
		},
	)
	return rspI.(*pbtypes.Empty), verrors.ToStatusError(err)
}

// Seal implements the ManagementServer Seal method.
func (s *managementService) Seal(ctx context.Context, req *snpb.SealRequest) (*snpb.SealResponse, error) {
	rspI, err := s.withTelemetry(ctx, "varlog.snpb.Management/Seal", req,
		func(ctx context.Context, reqI interface{}) (interface{}, error) {
			req := reqI.(*snpb.SealRequest)
			// TODO: too many arguments
			status, maxGLSN, err := s.m.Seal(ctx, req.GetClusterID(), req.GetStorageNodeID(), req.GetLogStreamID(), req.GetLastCommittedGLSN())
			return &snpb.SealResponse{
				Status:            status,
				LastCommittedGLSN: maxGLSN,
			}, err
		},
	)
	return rspI.(*snpb.SealResponse), verrors.ToStatusError(err)
}

// Unseal implements the ManagementServer Unseal method.
func (s *managementService) Unseal(ctx context.Context, req *snpb.UnsealRequest) (*pbtypes.Empty, error) {
	rspI, err := s.withTelemetry(ctx, "varlog.snpb.Management/Unseal", req,
		func(ctx context.Context, reqI interface{}) (interface{}, error) {
			req := reqI.(*snpb.UnsealRequest)
			err := s.m.Unseal(ctx, req.GetClusterID(), req.GetStorageNodeID(), req.GetLogStreamID())
			return &pbtypes.Empty{}, err
		},
	)
	return rspI.(*pbtypes.Empty), verrors.ToStatusError(err)
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
			status, err := s.m.Sync(ctx, req.GetClusterID(), req.GetStorageNodeID(), req.GetLogStreamID(), replica, req.GetLastGLSN())
			return &snpb.SyncResponse{Status: status}, err
		},
	)
	return rspI.(*snpb.SyncResponse), verrors.ToStatusError(err)
}
