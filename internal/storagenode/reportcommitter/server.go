package reportcommitter

//go:generate mockgen -build_flags -mod=vendor -self_package github.com/kakao/varlog/internal/storagenode/reportcommitter -package reportcommitter -destination server_mock.go . Server

import (
	"context"
	"fmt"

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
	lsr    Reporter
	tmStub *telemetry.TelemetryStub
	logger *zap.Logger
}

var _ Server = (*server)(nil)

func NewServer(lsr Reporter) *server {
	return &server{
		lsr:    lsr,
		tmStub: telemetry.NewNopTelmetryStub(),
		logger: zap.NewNop(),
	}
}

func (s *server) Register(server *grpc.Server) {
	snpb.RegisterLogStreamReporterServer(server, s)
	s.logger.Info("register to rpc server")
}

func (s *server) withTelemetry(ctx context.Context, spanName string, req interface{}, h rpcserver.Handler) (rsp interface{}, err error) {
	ctx, span := s.tmStub.StartSpan(ctx, spanName,
		oteltrace.WithAttributes(attribute.StorageNodeID(s.lsr.StorageNodeID())),
		oteltrace.WithSpanKind(oteltrace.SpanKindServer),
	)
	/*
		attributes := []label.KeyValue{
			attribute.RPCName(spanName),
			attribute.StorageNodeIDLabel(s.lsr.StorageNodeID()),
		}
			s.tmStub.mt.RecordBatch(ctx, attributes,
				s.tmStub.metrics().totalRequests.Measurement(1),
				s.tmStub.metrics().activeRequests.Measurement(1),
			)
	*/

	rsp, err = h(ctx, req)
	if err == nil {
		s.logger.Info(spanName,
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

	//s.tmStub.metrics().activeRequests.Add(ctx, -1, attributes...)
	span.End()
	return rsp, err
}

func (s *server) GetReport(ctx context.Context, req *snpb.GetReportRequest) (*snpb.GetReportResponse, error) {
	rspI, err := s.withTelemetry(ctx, "varlog.snpb.Reporter/GetReport", req,
		func(ctx context.Context, reqI interface{}) (interface{}, error) {
			var rsp *snpb.GetReportResponse
			reports, err := s.lsr.GetReport(ctx)
			if err != nil {
				return rsp, err
			}
			rsp = &snpb.GetReportResponse{
				StorageNodeID:   s.lsr.StorageNodeID(),
				UncommitReports: make([]*snpb.LogStreamUncommitReport, 0, len(reports)),
			}
			for _, report := range reports {
				rsp.UncommitReports = append(rsp.UncommitReports, report)
			}
			return rsp, nil
		},
	)
	return rspI.(*snpb.GetReportResponse), verrors.ToStatusErrorWithCode(err, codes.Internal)
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

func (s *server) GetPrevCommitResult(ctx context.Context, req *snpb.GetPrevCommitResultRequest) (*snpb.GetPrevCommitResultResponse, error) {
	panic("not yet implemented")
}
