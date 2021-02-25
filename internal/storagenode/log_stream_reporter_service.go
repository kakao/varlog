package storagenode

import (
	"context"
	"fmt"

	"github.com/pkg/errors"
	oteltrace "go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"

	"github.daumkakao.com/varlog/varlog/pkg/util/telemetry/trace"
	"github.daumkakao.com/varlog/varlog/pkg/verrors"
	"github.daumkakao.com/varlog/varlog/proto/snpb"
)

type LogStreamReporterService struct {
	lsr    LogStreamReporter
	tmStub *telemetryStub
	logger *zap.Logger
}

var _ snpb.LogStreamReporterServer = (*LogStreamReporterService)(nil)

func NewLogStreamReporterService(lsr LogStreamReporter, tmStub *telemetryStub, logger *zap.Logger) *LogStreamReporterService {
	if logger == nil {
		logger = zap.NewNop()
	}
	logger = logger.Named("logstreamreporterservice")
	return &LogStreamReporterService{
		lsr:    lsr,
		tmStub: tmStub,
		logger: logger,
	}
}

func (s *LogStreamReporterService) Register(server *grpc.Server) {
	snpb.RegisterLogStreamReporterServer(server, s)
	s.logger.Info("register to rpc server")
}

func (s *LogStreamReporterService) withTelemetry(ctx context.Context, spanName string, req interface{}, h handler) (rsp interface{}, err error) {
	ctx, span := s.tmStub.startSpan(ctx, spanName,
		oteltrace.WithAttributes(trace.StorageNodeIDLabel(s.lsr.StorageNodeID())),
		oteltrace.WithSpanKind(oteltrace.SpanKindServer),
	)
	s.tmStub.metrics().requests.Add(ctx, 1)
	rsp, err = h(ctx, req)
	if err == nil {
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
	s.tmStub.metrics().requests.Add(ctx, -1)
	span.End()
	return rsp, err
}

func (s *LogStreamReporterService) GetReport(ctx context.Context, req *snpb.GetReportRequest) (*snpb.GetReportResponse, error) {
	rspI, err := s.withTelemetry(ctx, "varlog.snpb.LogStreamReporter.GetReport", req,
		func(ctx context.Context, reqI interface{}) (interface{}, error) {
			var rsp *snpb.GetReportResponse
			reports, err := s.lsr.GetReport(ctx)
			if err != nil {
				return rsp, err
			}
			rsp = &snpb.GetReportResponse{
				StorageNodeID:   s.lsr.StorageNodeID(),
				UncommitReports: make([]*snpb.LogStreamUncommitReport, 0, len(reports)),
			}
			for _, report := range reports {
				rsp.UncommitReports = append(rsp.UncommitReports,
					&snpb.LogStreamUncommitReport{
						LogStreamID:           report.LogStreamID,
						HighWatermark:         report.KnownHighWatermark,
						UncommittedLLSNOffset: report.UncommittedLLSNOffset,
						UncommittedLLSNLength: report.UncommittedLLSNLength,
					},
				)
			}
			return rsp, nil
		},
	)
	return rspI.(*snpb.GetReportResponse), verrors.ToStatusErrorWithCode(err, codes.Internal)
}

func (s *LogStreamReporterService) Commit(ctx context.Context, req *snpb.CommitRequest) (*snpb.CommitResponse, error) {
	code := codes.Internal
	rspI, err := s.withTelemetry(ctx, "varlog.snpb.LogStreamReporter.Commit", req,
		func(ctx context.Context, reqI interface{}) (interface{}, error) {
			req := reqI.(*snpb.CommitRequest)
			rsp := &snpb.CommitResponse{}

			if len(req.CommitResults) == 0 {
				// s.logger.Error("no commit result in Commit")
				// return &snpb.CommitResponse{}, nil
				code = codes.InvalidArgument
				return rsp, errors.New("no commit result")
			}

			commitResults := make([]CommittedLogStreamStatus, len(req.CommitResults))
			for i, cr := range req.CommitResults {
				commitResults[i].LogStreamID = cr.LogStreamID
				commitResults[i].HighWatermark = cr.HighWatermark
				commitResults[i].PrevHighWatermark = cr.PrevHighWatermark
				commitResults[i].CommittedGLSNOffset = cr.CommittedGLSNOffset
				commitResults[i].CommittedGLSNLength = cr.CommittedGLSNLength
			}
			err := s.lsr.Commit(ctx, commitResults)
			// TODO: specify code according to the type of err
			return rsp, err
		},
	)
	return rspI.(*snpb.CommitResponse), verrors.ToStatusErrorWithCode(err, code)
}
