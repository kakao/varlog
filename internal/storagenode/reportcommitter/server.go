package reportcommitter

//go:generate mockgen -build_flags -mod=vendor -self_package github.com/kakao/varlog/internal/storagenode/reportcommitter -package reportcommitter -destination server_mock.go . Server

import (
	"context"
	"fmt"
	"io"

	oteltrace "go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"

	"github.com/kakao/varlog/internal/storagenode/rpcserver"
	"github.com/kakao/varlog/internal/storagenode/telemetry"
	"github.com/kakao/varlog/pkg/util/telemetry/attribute"
	"github.com/kakao/varlog/pkg/verrors"
	"github.com/kakao/varlog/proto/snpb"
)

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

func (s *server) withTelemetry(ctx context.Context, spanName string, req interface{}, h rpcserver.Handler) (rsp interface{}, err error) {
	ctx, span := s.measure.Stub().StartSpan(ctx, spanName,
		oteltrace.WithAttributes(attribute.StorageNodeID(s.lsr.StorageNodeID())),
		oteltrace.WithSpanKind(oteltrace.SpanKindServer),
	)

	rsp, err = h(ctx, req)
	if err == nil {
		s.logger.Debug(spanName,
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

	// s.measure.Stub().Metrics().ActiveRequests.Add(ctx, -1, attributes...)
	span.End()
	return rsp, err
}

func (s *server) GetReport(stream snpb.LogStreamReporter_GetReportServer) (err error) {
	var (
		req snpb.GetReportRequest
		rsp snpb.GetReportResponse
	)
	rsp.StorageNodeID = s.lsr.StorageNodeID()
	for {
		err = stream.RecvMsg(&req)
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}

		rsp.UncommitReports, err = s.lsr.GetReport(stream.Context())
		if err != nil {
			return err
		}

		err = stream.SendMsg(&rsp)
		if err != nil {
			return err
		}
	}
}

func (s *server) Commit(ctx context.Context, req *snpb.CommitRequest) (*snpb.CommitResponse, error) {
	code := codes.Internal
	rspI, err := s.withTelemetry(ctx, "varlog.snpb.Reporter/Commit", req,
		func(ctx context.Context, reqI interface{}) (interface{}, error) {
			req := reqI.(*snpb.CommitRequest)
			rsp := &snpb.CommitResponse{}
			return rsp, s.lsr.Commit(ctx, req.GetCommitResults())
		},
	)
	return rspI.(*snpb.CommitResponse), verrors.ToStatusErrorWithCode(err, code)
}
