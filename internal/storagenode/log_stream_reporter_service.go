package storagenode

import (
	"context"

	"go.uber.org/zap"
	"google.golang.org/grpc"

	"github.com/kakao/varlog/proto/snpb"
)

type LogStreamReporterService struct {
	logger *zap.Logger
	LogStreamReporter
}

func NewLogStreamReporterService(lsr LogStreamReporter, logger *zap.Logger) *LogStreamReporterService {
	if logger == nil {
		logger = zap.NewNop()
	}
	logger = logger.Named("logstreamreporterservice")
	return &LogStreamReporterService{LogStreamReporter: lsr, logger: logger}
}

func (s *LogStreamReporterService) Register(server *grpc.Server) {
	s.logger.Info("register to rpc server")
	snpb.RegisterLogStreamReporterServiceServer(server, s)
}

func (s *LogStreamReporterService) GetReport(ctx context.Context, _ *snpb.GetReportRequest) (*snpb.GetReportResponse, error) {
	rsp := &snpb.GetReportResponse{
		StorageNodeID: s.StorageNodeID(),
	}
	reports, err := s.LogStreamReporter.GetReport(ctx)
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
	err := s.LogStreamReporter.Commit(ctx, commitResults)
	return &snpb.CommitResponse{}, err
}
