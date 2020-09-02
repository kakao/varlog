package storage

import (
	"context"

	"github.com/gogo/protobuf/types"
	pb "github.daumkakao.com/varlog/varlog/proto/storage_node"
	"google.golang.org/grpc"
)

type LogStreamReporterService struct {
	LogStreamReporter
}

func NewLogStreamReporterService(lsr LogStreamReporter) *LogStreamReporterService {
	return &LogStreamReporterService{lsr}
}

func (s *LogStreamReporterService) Register(server *grpc.Server) {
	pb.RegisterLogStreamReporterServiceServer(server, s)
}

func (s *LogStreamReporterService) GetReport(ctx context.Context, _ *types.Empty) (*pb.LocalLogStreamDescriptor, error) {
	reportPb := &pb.LocalLogStreamDescriptor{
		StorageNodeID: s.StorageNodeID(),
	}
	knownNextGLSN, reports, err := s.LogStreamReporter.GetReport(ctx)
	if err != nil {
		return nil, err
	}
	reportPb.Uncommit = make([]*pb.LocalLogStreamDescriptor_LogStreamUncommitReport, len(reports))
	for i, rpt := range reports {
		reportPb.Uncommit[i] = &pb.LocalLogStreamDescriptor_LogStreamUncommitReport{
			LogStreamID:           rpt.LogStreamID,
			UncommittedLLSNOffset: rpt.UncommittedLLSNOffset,
			UncommittedLLSNLength: rpt.UncommittedLLSNLength,
		}
	}
	reportPb.NextGLSN = knownNextGLSN
	return reportPb, nil
}

func (s *LogStreamReporterService) Commit(ctx context.Context, req *pb.GlobalLogStreamDescriptor) (*types.Empty, error) {
	if len(req.CommitResult) == 0 {
		return &types.Empty{}, nil
	}
	nextGLSN := req.GetNextGLSN()
	prevNextGLSN := req.GetPrevNextGLSN()
	commitResults := make([]CommittedLogStreamStatus, len(req.CommitResult))
	for i, cr := range req.CommitResult {
		commitResults[i].LogStreamID = cr.LogStreamID
		commitResults[i].HighWatermark = nextGLSN
		commitResults[i].PrevHighWatermark = prevNextGLSN
		commitResults[i].CommittedGLSNOffset = cr.CommittedGLSNOffset
		commitResults[i].CommittedGLSNLength = cr.CommittedGLSNLength
	}
	err := s.LogStreamReporter.Commit(ctx, nextGLSN, prevNextGLSN, commitResults)
	return &types.Empty{}, err
}
