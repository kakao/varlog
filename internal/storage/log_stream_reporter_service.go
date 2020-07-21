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
	knownNextGLSN, reports := s.LogStreamReporter.GetReport()
	reportPb.Uncommit = make([]*pb.LocalLogStreamDescriptor_LogStreamUncommitReport, len(reports))
	for i, rpt := range reports {
		reportPb.Uncommit[i] = &pb.LocalLogStreamDescriptor_LogStreamUncommitReport{
			LogStreamID:          rpt.LogStreamID,
			UncommittedLLSNBegin: rpt.UncommittedLLSNBegin,
			UncommittedLLSNEnd:   rpt.UncommittedLLSNEnd,
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
		commitResults[i].NextGLSN = nextGLSN
		commitResults[i].PrevNextGLSN = prevNextGLSN
		commitResults[i].CommittedGLSNBegin = cr.CommittedGLSNBegin
		commitResults[i].CommittedGLSNEnd = cr.CommittedGLSNEnd
	}
	s.LogStreamReporter.Commit(nextGLSN, prevNextGLSN, commitResults)
	return &types.Empty{}, nil
}
