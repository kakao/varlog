package storage

import (
	"context"

	"github.com/gogo/protobuf/types"
	pb "github.com/kakao/varlog/proto/storage_node"
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
	rsp := &pb.LocalLogStreamDescriptor{
		StorageNodeID: s.StorageNodeID(),
	}
	knownHighWatermark, reports, err := s.LogStreamReporter.GetReport(ctx)
	if err != nil {
		return nil, err
	}
	rsp.Uncommit = make([]*pb.LocalLogStreamDescriptor_LogStreamUncommitReport, len(reports))
	for i, report := range reports {
		rsp.Uncommit[i] = &pb.LocalLogStreamDescriptor_LogStreamUncommitReport{
			LogStreamID:           report.LogStreamID,
			UncommittedLLSNOffset: report.UncommittedLLSNOffset,
			UncommittedLLSNLength: report.UncommittedLLSNLength,
		}
	}
	rsp.HighWatermark = knownHighWatermark
	return rsp, nil
}

func (s *LogStreamReporterService) Commit(ctx context.Context, req *pb.GlobalLogStreamDescriptor) (*types.Empty, error) {
	if len(req.CommitResult) == 0 {
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
