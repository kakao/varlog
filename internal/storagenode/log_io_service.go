package storagenode

import (
	"context"
	"fmt"

	pbtypes "github.com/gogo/protobuf/types"
	"github.com/pkg/errors"
	oteltrace "go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"

	"github.daumkakao.com/varlog/varlog/pkg/types"
	"github.daumkakao.com/varlog/varlog/pkg/util/telemetry/trace"
	"github.daumkakao.com/varlog/varlog/pkg/verrors"
	"github.daumkakao.com/varlog/varlog/proto/snpb"
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
	if err == nil {
		var rspMsg fmt.Stringer
		if rsp != nil {
			rspMsg = rsp.(fmt.Stringer)
		}
		s.logger.Info(spanName,
			zap.Stringer("request", req.(fmt.Stringer)),
			zap.Stringer("response", rspMsg),
		)
	} else {
		span.RecordError(err)
		s.logger.Error(spanName,
			zap.Error(err),
			zap.Stringer("request", req.(fmt.Stringer)),
		)
	}
	s.tmStub.metrics().requests.Add(ctx, -1)
	span.End()
	return rsp, err
}

func (s *LogIOService) Append(ctx context.Context, req *snpb.AppendRequest) (*snpb.AppendResponse, error) {
	code := codes.Internal
	rspI, err := s.withTelemetry(ctx, "varlog.snpb.LogIO.Append", req,
		func(ctx context.Context, reqI interface{}) (interface{}, error) {
			req := reqI.(*snpb.AppendRequest)
			var rsp *snpb.AppendResponse
			lse, ok := s.lseGetter.GetLogStreamExecutor(req.GetLogStreamID())
			if !ok {
				code = codes.NotFound
				return rsp, errors.WithStack(errNoLogStream)
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
				code = codes.Internal
				return rsp, err
			}
			return &snpb.AppendResponse{GLSN: glsn}, nil
		},
	)
	return rspI.(*snpb.AppendResponse), verrors.ToStatusErrorWithCode(err, code)
}

func (s *LogIOService) Read(ctx context.Context, req *snpb.ReadRequest) (*snpb.ReadResponse, error) {
	code := codes.Internal
	rspI, err := s.withTelemetry(ctx, "varlog.snpb.LogIO.Read", req,
		func(ctx context.Context, reqI interface{}) (interface{}, error) {
			req := reqI.(*snpb.ReadRequest)
			var rsp *snpb.ReadResponse
			lse, ok := s.lseGetter.GetLogStreamExecutor(req.GetLogStreamID())
			if !ok {
				code = codes.NotFound
				return rsp, errors.WithStack(errNoLogStream)
			}

			logEntry, err := lse.Read(ctx, req.GetGLSN())
			if err != nil {
				// TODO: Check whether these are safe.
				switch errors.Cause(err) {
				case verrors.ErrNoEntry:
					code = codes.NotFound
				case verrors.ErrTrimmed:
					code = codes.OutOfRange
				case verrors.ErrUndecidable:
					code = codes.Unavailable
				default:
					code = codes.Internal
				}
				/*
					if errors.Is(err, verrors.ErrNoEntry) {
						code = codes.NotFound
					} else if errors.Is(err, verrors.ErrTrimmed) {
						code = codes.OutOfRange
					} else if errors.Is(err, verrors.ErrUndecidable) {
						// TODO (jun): consider codes.FailedPrecondition
						code = codes.Unavailable
					} else {
						code = codes.Internal
					}
				*/
				return rsp, errors.Wrap(err, "storagenode")
			}
			return &snpb.ReadResponse{
				Payload: logEntry.Data,
				GLSN:    req.GetGLSN(),
				LLSN:    logEntry.LLSN,
			}, nil
		},
	)
	return rspI.(*snpb.ReadResponse), verrors.ToStatusErrorWithCode(err, code)
}

func (s *LogIOService) Subscribe(req *snpb.SubscribeRequest, stream snpb.LogIO_SubscribeServer) error {
	code := codes.Internal
	_, err := s.withTelemetry(stream.Context(), "varlog.snpb.LogIO.Subscribe", req,
		func(ctx context.Context, reqI interface{}) (interface{}, error) {
			req := reqI.(*snpb.SubscribeRequest)

			if req.GetGLSNBegin() >= req.GetGLSNEnd() {
				code = codes.InvalidArgument
				return nil, errors.New("storagenode: invalid subscription range")
			}
			lse, ok := s.lseGetter.GetLogStreamExecutor(req.GetLogStreamID())
			if !ok {
				code = codes.NotFound
				return nil, errors.WithStack(errNoLogStream)
			}

			resultC, err := lse.Subscribe(ctx, req.GetGLSNBegin(), req.GetGLSNEnd())
			if err != nil {
				return nil, err
			}

			for result := range resultC {
				if result.Err != nil {
					// TODO: Can ErrEndOfRange be replaced with io.EOF?
					if result.Err == ErrEndOfRange {
						return nil, nil
					}
					return nil, errors.Wrap(result.Err, "storagenode")
				}
				if err := stream.Send(&snpb.SubscribeResponse{
					GLSN:    result.LogEntry.GLSN,
					LLSN:    result.LogEntry.LLSN,
					Payload: result.LogEntry.Data,
				}); err != nil {
					return nil, errors.Wrap(err, "storagenode")
				}
			}
			return nil, nil
		},
	)
	return verrors.ToStatusErrorWithCode(err, code)
}

func (s *LogIOService) Trim(ctx context.Context, req *snpb.TrimRequest) (*pbtypes.Empty, error) {
	code := codes.Internal
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
	return rspI.(*pbtypes.Empty), verrors.ToStatusErrorWithCode(err, code)
}
