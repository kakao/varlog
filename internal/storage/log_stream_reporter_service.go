package storage

import (
	"context"

	"github.com/gogo/protobuf/types"
	"github.com/kakao/varlog/proto/snpb"
	"go.uber.org/zap"
	"google.golang.org/grpc"
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

func (s *LogStreamReporterService) GetReport(ctx context.Context, _ *types.Empty) (*snpb.LocalLogStreamDescriptor, error) {
	rsp := &snpb.LocalLogStreamDescriptor{
		StorageNodeID: s.StorageNodeID(),
	}
	knownHighWatermark, reports, err := s.LogStreamReporter.GetReport(ctx)
	if err != nil {
		s.logger.Error("could not get report", zap.Error(err))
		return nil, err
	}
	rsp.Uncommit = make([]*snpb.LocalLogStreamDescriptor_LogStreamUncommitReport, 0, len(reports))
	for _, report := range reports {
		rsp.Uncommit = append(rsp.Uncommit,
			&snpb.LocalLogStreamDescriptor_LogStreamUncommitReport{
				LogStreamID:           report.LogStreamID,
				UncommittedLLSNOffset: report.UncommittedLLSNOffset,
				UncommittedLLSNLength: report.UncommittedLLSNLength,
			},
		)
	}
	rsp.HighWatermark = knownHighWatermark
	return rsp, nil
}

func (s *LogStreamReporterService) Commit(ctx context.Context, req *snpb.GlobalLogStreamDescriptor) (*types.Empty, error) {
	if len(req.CommitResult) == 0 {
		s.logger.Error("no commit result in Commit")
		return &types.Empty{}, nil
	}
	hwm := req.GetHighWatermark()
	prevHWM := req.GetPrevHighWatermark()
	commitResults := make([]CommittedLogStreamStatus, len(req.CommitResult))
	for i, cr := range req.CommitResult {
		commitResults[i].LogStreamID = cr.LogStreamID
		commitResults[i].HighWatermark = hwm
		commitResults[i].PrevHighWatermark = prevHWM
		commitResults[i].CommittedGLSNOffset = cr.CommittedGLSNOffset
		commitResults[i].CommittedGLSNLength = cr.CommittedGLSNLength
	}
	err := s.LogStreamReporter.Commit(ctx, hwm, prevHWM, commitResults)
	return &types.Empty{}, err
}
