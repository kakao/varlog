package storagenode

import (
	"context"
	"errors"
	"fmt"

	pbtypes "github.com/gogo/protobuf/types"
	"go.opentelemetry.io/otel/codes"
	oteltrace "go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"google.golang.org/grpc"

	"github.daumkakao.com/varlog/varlog/internal/storagenode/rpcserver"
	"github.daumkakao.com/varlog/varlog/pkg/types"
	"github.daumkakao.com/varlog/varlog/pkg/verrors"
	"github.daumkakao.com/varlog/varlog/proto/snpb"
	"github.daumkakao.com/varlog/varlog/proto/varlogpb"
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
		// oteltrace.WithAttributes(label.StorageNodeIDLabel(s.m.StorageNodeID())),
		oteltrace.WithSpanKind(oteltrace.SpanKindServer),
	)
	/*
		labels := []label.KeyValue{
			label.RPCName(spanName),
			label.StorageNodeIDLabel(s.m.StorageNodeID()),
		}
		s.tmStub.mt.RecordBatch(ctx, labels,
			s.tmStub.metrics().totalRequests.Measurement(1),
			s.tmStub.metrics().activeRequests.Measurement(1),
		)
	*/

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
			zap.Stringer("request", req.(fmt.Stringer)),
			zap.Stringer("response", rsp.(fmt.Stringer)),
		)
	} else {
		span.RecordError(err)
		s.logger.Error(spanName,
			zap.Error(err),
			zap.Stringer("request", req.(fmt.Stringer)),
		)
	}

	//s.tmStub.metrics().activeRequests.Add(ctx, -1, labels...)
	span.End()
	return rsp, err
}

// GetMetadata implements the ManagementServer GetMetadata method.
func (s *server) GetMetadata(ctx context.Context, req *snpb.GetMetadataRequest) (*snpb.GetMetadataResponse, error) {
	rspI, err := s.withTelemetry(ctx, "varlog.snpb.Server/GetMetadata", req,
		func(ctx context.Context, _ interface{}) (interface{}, error) {
			metadata, err := s.storageNode.GetMetadata(ctx)
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
func (s *server) AddLogStream(ctx context.Context, req *snpb.AddLogStreamRequest) (*snpb.AddLogStreamResponse, error) {
	rspI, err := s.withTelemetry(ctx, "varlog.snpb.Server/AddLogStream", req,
		func(ctx context.Context, reqI interface{}) (interface{}, error) {
			req := reqI.(*snpb.AddLogStreamRequest)
			path, err := s.storageNode.AddLogStream(ctx, req.GetLogStreamID(), req.GetStorage().GetPath())
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
func (s *server) RemoveLogStream(ctx context.Context, req *snpb.RemoveLogStreamRequest) (*pbtypes.Empty, error) {
	rspI, err := s.withTelemetry(ctx, "varlog.snpb.Server/RemoveLogStream", req,
		func(ctx context.Context, reqI interface{}) (interface{}, error) {
			req := reqI.(*snpb.RemoveLogStreamRequest)
			err := s.storageNode.RemoveLogStream(ctx, req.GetLogStreamID())
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
		func(ctx context.Context, reqI interface{}) (interface{}, error) {
			req := reqI.(*snpb.SealRequest)
			status, maxGLSN, err := s.storageNode.Seal(ctx, req.GetLogStreamID(), req.GetLastCommittedGLSN())
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
		func(ctx context.Context, reqI interface{}) (interface{}, error) {
			req := reqI.(*snpb.UnsealRequest)
			err := s.storageNode.Unseal(ctx, req.GetLogStreamID())
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
		func(ctx context.Context, reqI interface{}) (interface{}, error) {
			req := reqI.(*snpb.SyncRequest)
			replica := snpb.Replica{
				StorageNodeID: req.GetBackup().GetStorageNodeID(),
				LogStreamID:   req.GetLogStreamID(),
				Address:       req.GetBackup().GetAddress(),
			}
			status, err := s.storageNode.Sync(ctx, req.GetLogStreamID(), replica, req.GetLastGLSN())
			return &snpb.SyncResponse{Status: status}, err
		},
	)
	if err != nil {
		return nil, verrors.ToStatusError(err)
	}
	return rspI.(*snpb.SyncResponse), nil
}
