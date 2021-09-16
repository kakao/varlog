package reportcommitter

//go:generate mockgen -build_flags -mod=vendor -self_package github.daumkakao.com/varlog/varlog/internal/storagenode/reportcommitter -package reportcommitter -destination server_mock.go . Server

import (
	"io"

	"go.uber.org/zap"
	"google.golang.org/grpc"

	"github.daumkakao.com/varlog/varlog/internal/storagenode/rpcserver"
	"github.daumkakao.com/varlog/varlog/internal/storagenode/telemetry"
	"github.daumkakao.com/varlog/varlog/proto/snpb"
)

const defaultReportsCapacity = 32

type Server interface {
	snpb.LogStreamReporterServer
	rpcserver.Registrable
}

type server struct {
	lsr     Reporter
	measure telemetry.Measurable
	logger  *zap.Logger
}

var _ Server = (*server)(nil)

func NewServer(lsr Reporter, m telemetry.Measurable) *server {
	return &server{
		lsr:     lsr,
		measure: m,
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
