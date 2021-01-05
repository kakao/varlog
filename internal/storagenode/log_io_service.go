package storagenode

import (
	"context"
	"errors"
	"fmt"

	pbtypes "github.com/gogo/protobuf/types"
	oteltrace "go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"

	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/pkg/util/telemetry/trace"
	"github.com/kakao/varlog/pkg/verrors"
	"github.com/kakao/varlog/proto/snpb"
)

type LogIOService struct {
	snpb.UnimplementedLogIOServer
	storageNodeID types.StorageNodeID
	lseGetter     LogStreamExecutorGetter
	tmStub        *telemetryStub
	logger        *zap.Logger
}

func NewLogIOService(storageNodeID types.StorageNodeID, lseGetter LogStreamExecutorGetter, tmStub *telemetryStub, logger *zap.Logger) *LogIOService {
	if logger == nil {
		logger = zap.NewNop()
	}
	logger = logger.Named("logioservice")
	return &LogIOService{
		storageNodeID: storageNodeID,
		lseGetter:     lseGetter,
		tmStub:        tmStub,
		logger:        logger,
	}
}

func (s *LogIOService) Register(server *grpc.Server) {
	s.logger.Info("register to rpc server")
	snpb.RegisterLogIOServer(server, s)
}

func (s *LogIOService) withTelemetry(ctx context.Context, spanName string, req interface{}, h handler) (rsp interface{}, err error) {
	ctx, span := s.tmStub.startSpan(ctx, spanName,
		oteltrace.WithAttributes(trace.StorageNodeIDLabel(s.storageNodeID)),
		oteltrace.WithSpanKind(oteltrace.SpanKindServer),
	)
	s.tmStub.metrics().requests.Add(ctx, 1)
	rsp, err = h(ctx, req)
	if err != nil {
		span.RecordError(err)
	}
	s.logger.Info(spanName, zap.Error(err))
	s.tmStub.metrics().requests.Add(ctx, -1)
	span.End()
	return rsp, err
}

func (s *LogIOService) Append(ctx context.Context, req *snpb.AppendRequest) (*snpb.AppendResponse, error) {
	rspI, err := s.withTelemetry(ctx, "varlog.snpb.LogIO.Append", req,
		func(ctx context.Context, reqI interface{}) (interface{}, error) {
			req := reqI.(*snpb.AppendRequest)
			var rsp *snpb.AppendResponse
			lse, ok := s.lseGetter.GetLogStreamExecutor(req.GetLogStreamID())
			if !ok {
				return rsp, errors.New("storagenode: no such log stream")
			}

			backups := make([]Replica, 0, len(req.Backups))
			for i := range req.Backups {
				backups = append(backups, Replica{
					StorageNodeID: req.Backups[i].GetStorageNodeID(),
					Address:       req.Backups[i].GetAddress(),
					LogStreamID:   req.GetLogStreamID(),
				})
			}

			glsn, err := lse.Append(ctx, req.GetPayload(), backups...)
			if err != nil {
				return rsp, fmt.Errorf("storagenode: append failed: %w", err)
			}
			return &snpb.AppendResponse{GLSN: glsn}, nil
		},
	)
	return rspI.(*snpb.AppendResponse), verrors.ToStatusError(err)
}

func (s *LogIOService) Read(ctx context.Context, req *snpb.ReadRequest) (*snpb.ReadResponse, error) {
	rspI, err := s.withTelemetry(ctx, "varlog.snpb.LogIO.Read", req,
		func(ctx context.Context, reqI interface{}) (interface{}, error) {
			req := reqI.(*snpb.ReadRequest)
			var rsp *snpb.ReadResponse
			lse, ok := s.lseGetter.GetLogStreamExecutor(req.GetLogStreamID())
			if !ok {
				return rsp, errors.New("storagenode: no such log stream")
			}

			logEntry, err := lse.Read(ctx, req.GetGLSN())
			if err != nil {
				return rsp, fmt.Errorf("storagenode: read failed: %w", err)
			}
			return &snpb.ReadResponse{
				Payload: logEntry.Data,
				GLSN:    req.GetGLSN(),
				LLSN:    logEntry.LLSN,
			}, nil
		},
	)
	return rspI.(*snpb.ReadResponse), verrors.ToStatusError(err)
}

func (s *LogIOService) Subscribe(req *snpb.SubscribeRequest, stream snpb.LogIO_SubscribeServer) error {
	_, err := s.withTelemetry(stream.Context(), "varlog.snpb.LogIO.Subscribe", req,
		func(ctx context.Context, reqI interface{}) (interface{}, error) {
			req := reqI.(*snpb.SubscribeRequest)

			if req.GetGLSNBegin() >= req.GetGLSNEnd() {
				return nil, errors.New("storagenode: invalid subscription range")
			}
			lse, ok := s.lseGetter.GetLogStreamExecutor(req.GetLogStreamID())
			if !ok {
				return nil, errors.New("storagenode: no such log stream")
			}

			resultC, err := lse.Subscribe(ctx, req.GetGLSNBegin(), req.GetGLSNEnd())
			if err != nil {
				return nil, err
				// return nil, fmt.Errorf("storagenode: subscribe failed: %w", err)
			}

			for result := range resultC {
				if result.Err != nil {
					// TODO: Can ErrEndOfRange be replaced with io.EOF?
					if result.Err == ErrEndOfRange {
						return nil, nil
					}
					return nil, fmt.Errorf("storagenode: subscribe failed: %w", result.Err)
				}
				if err := stream.Send(&snpb.SubscribeResponse{
					GLSN:    result.LogEntry.GLSN,
					LLSN:    result.LogEntry.LLSN,
					Payload: result.LogEntry.Data,
				}); err != nil {
					return nil, fmt.Errorf("storagenode: subscribe failed: %w", err)
				}
			}
			return nil, nil
		},
	)
	return verrors.ToStatusError(err)
}

func (s *LogIOService) Trim(ctx context.Context, req *snpb.TrimRequest) (*pbtypes.Empty, error) {
	rspI, err := s.withTelemetry(ctx, "varlog.snpb.LogIO.Trim", req,
		func(ctx context.Context, reqI interface{}) (interface{}, error) {
			req := reqI.(*snpb.TrimRequest)

			lses := s.lseGetter.GetLogStreamExecutors()
			errs := make([]error, len(lses))
			g, ctx := errgroup.WithContext(ctx)
			for i := range lses {
				idx := i
				g.Go(func() error {
					errs[idx] = lses[idx].Trim(ctx, req.GetGLSN())
					return nil
				})
			}
			g.Wait()
			for i := range errs {
				if errs[i] != nil {
					return &pbtypes.Empty{}, errs[i]
				}
			}
			return &pbtypes.Empty{}, nil
		},
	)
	return rspI.(*pbtypes.Empty), verrors.ToStatusError(err)
}
