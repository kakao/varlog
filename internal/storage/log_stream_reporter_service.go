package storage

import (
	"context"

	"github.com/gogo/protobuf/types"
	pb "github.com/kakao/varlog/proto/storage_node"
	"google.golang.org/grpc"
)

type LogStreamReporterService struct {
	*LogStreamReporter
}

func NewLogStreamReporterService() *LogStreamReporterService {
	return &LogStreamReporterService{}
}

func (s *LogStreamReporterService) Register(server *grpc.Server) {
	pb.RegisterLogStreamReporterServer(server, s)
}

func (s *LogStreamReporterService) GetReport(ctx context.Context, _ *types.Empty) (*pb.LocalLogStreamDescriptor, error) {
	reportPb := &pb.LocalLogStreamDescriptor{
		StorageNodeID: s.storageNodeID,
	}
	knownNextGLSN, reports := s.LogStreamReporter.GetReport()
	reportPb.Uncommit = make([]*pb.LocalLogStreamDescriptor_LogStreamUncommitReport, len(reports))
	for i, status := range reports {
		rpt := &pb.LocalLogStreamDescriptor_LogStreamUncommitReport{
			LogStreamID:          status.LogStreamID,
			UncommittedLLSNBegin: status.UncommittedLLSNBegin,
			UncommittedLLSNEnd:   status.UncommittedLLSNEnd,
		}
		reportPb.Uncommit[i] = rpt
	}
	reportPb.NextGLSN = knownNextGLSN
	return reportPb, nil
}

func (s *LogStreamReporterService) Commit(ctx context.Context, req *pb.GlobalLogStreamDescriptor) (*types.Empty, error) {
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
