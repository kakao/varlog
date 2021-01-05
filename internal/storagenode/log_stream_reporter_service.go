package storagenode

import (
	"context"

	oteltrace "go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"google.golang.org/grpc"

	"github.com/kakao/varlog/pkg/util/telemetry/trace"
	"github.com/kakao/varlog/proto/snpb"
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
	s.logger.Info("register to rpc server")
	snpb.RegisterLogStreamReporterServer(server, s)
}

func (s *LogStreamReporterService) withTelemetry(ctx context.Context, spanName string, req interface{}, h handler) (rsp interface{}, err error) {
	ctx, span := s.tmStub.startSpan(ctx, spanName,
		oteltrace.WithAttributes(trace.StorageNodeIDLabel(s.lsr.StorageNodeID())),
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

func (s *LogStreamReporterService) GetReport(ctx context.Context, _ *snpb.GetReportRequest) (*snpb.GetReportResponse, error) {
	/*
		rspI, err := s.withTelemetry(ctx, "varlog.snpb.LogStreamReporter.GetReport", req,
			func(ctx context.Context, reqI interface{}) (interface{}, error) {
			},
		)
	*/

	rsp := &snpb.GetReportResponse{
		StorageNodeID: s.lsr.StorageNodeID(),
	}
	reports, err := s.lsr.GetReport(ctx)
	if err != nil {
		s.logger.Error("could not get report", zap.Error(err))
		return nil, err
	}
	rsp.UncommitReports = make([]*snpb.LogStreamUncommitReport, 0, len(reports))
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
}

func (s *LogStreamReporterService) Commit(ctx context.Context, req *snpb.CommitRequest) (*snpb.CommitResponse, error) {
	if len(req.CommitResults) == 0 {
		s.logger.Error("no commit result in Commit")
		return &snpb.CommitResponse{}, nil
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
	return &snpb.CommitResponse{}, err
}
