package reportcommitter

//go:generate mockgen -build_flags -mod=vendor -self_package github.com/kakao/varlog/internal/storagenode_deprecated/reportcommitter -package reportcommitter -destination server_mock.go . Server

import (
	"io"

	"go.uber.org/zap"
	"google.golang.org/grpc"

	"github.com/kakao/varlog/internal/storagenode_deprecated/rpcserver"
	"github.com/kakao/varlog/internal/storagenode_deprecated/telemetry"
	"github.com/kakao/varlog/proto/snpb"
)

const defaultReportsCapacity = 32

type Server interface {
	snpb.LogStreamReporterServer
	rpcserver.Registrable
}

type server struct {
	lsr     Reporter
	metrics *telemetry.Metrics
	logger  *zap.Logger
}

var _ Server = (*server)(nil)

func NewServer(lsr Reporter, metrics *telemetry.Metrics) *server {
	return &server{
		lsr:     lsr,
		metrics: metrics,
		logger:  zap.NewNop(),
	}
}

func (s *server) Register(server *grpc.Server) {
	snpb.RegisterLogStreamReporterServer(server, s)
	s.logger.Info("register to rpc server")
}

func (s *server) GetReport(stream snpb.LogStreamReporter_GetReportServer) (err error) {
	req := snpb.GetReportRequest{}
	rsp := snpb.GetReportResponse{
		StorageNodeID:   s.lsr.StorageNodeID(),
		UncommitReports: make([]snpb.LogStreamUncommitReport, 0, defaultReportsCapacity),
	}

	for {
		err = stream.RecvMsg(&req)
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}

		err = s.lsr.GetReport(stream.Context(), &rsp)
		if err != nil {
			return err
		}

		err = stream.SendMsg(&rsp)
		if err != nil {
			return err
		}
	}
}

func (s *server) Commit(stream snpb.LogStreamReporter_CommitServer) (err error) {
	// NOTE: Is it necessary to trace Commit RPCs?
	var req snpb.CommitRequest
	for {
		err = stream.RecvMsg(&req)
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}

		err = s.lsr.Commit(stream.Context(), req.CommitResult)
		if err != nil {
			return err
		}
	}
}
