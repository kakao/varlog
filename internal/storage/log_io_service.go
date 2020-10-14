package storage

import (
	"context"

	pbtypes "github.com/gogo/protobuf/types"
	"github.daumkakao.com/varlog/varlog/pkg/varlog"
	"github.daumkakao.com/varlog/varlog/pkg/varlog/types"
	snpb "github.daumkakao.com/varlog/varlog/proto/storage_node"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

type LogIOService struct {
	snpb.UnimplementedLogIOServer
	storageNodeID types.StorageNodeID
	lseGetter     LogStreamExecutorGetter
	logger        *zap.Logger
}

func NewLogIOService(storageNodeID types.StorageNodeID, lseGetter LogStreamExecutorGetter, logger *zap.Logger) *LogIOService {
	if logger == nil {
		logger = zap.NewNop()
	}
	logger = logger.Named("logioservice")
	return &LogIOService{
		storageNodeID: storageNodeID,
		lseGetter:     lseGetter,
		logger:        logger,
	}
}

func (s *LogIOService) Register(server *grpc.Server) {
	s.logger.Info("register to rpc server")
	snpb.RegisterLogIOServer(server, s)
}

func (s *LogIOService) Append(ctx context.Context, req *snpb.AppendRequest) (*snpb.AppendResponse, error) {
	lse, ok := s.lseGetter.GetLogStreamExecutor(req.GetLogStreamID())
	if !ok {
		s.logger.Error("no logstreamexecutor", zap.Any("request", req))
		return nil, varlog.ErrInvalidArgument
	}
	// TODO: create child context by using operation timeout
	var backups []Replica
	for _, b := range req.Backups {
		backups = append(backups, Replica{
			StorageNodeID: b.StorageNodeID,
			Address:       b.Address,
			LogStreamID:   req.GetLogStreamID(),
		})
	}
	glsn, err := lse.Append(ctx, req.GetPayload(), backups...)
	if err != nil {
		s.logger.Error("could not append", zap.Any("request", req), zap.Error(err))
		return nil, varlog.ToStatusError(err)
	}
	return &snpb.AppendResponse{GLSN: glsn}, nil
}

func (s *LogIOService) Read(ctx context.Context, req *snpb.ReadRequest) (*snpb.ReadResponse, error) {
	lse, ok := s.lseGetter.GetLogStreamExecutor(req.GetLogStreamID())
	if !ok {
		s.logger.Error("no logstreamexecutor", zap.Any("request", req))
		return nil, varlog.ErrInvalid
	}

	// TODO: create child context by using operation timeout
	logEntry, err := lse.Read(ctx, req.GetGLSN())
	if err != nil {
		s.logger.Error("could not read", zap.Any("request", req), zap.Error(err))
		return nil, varlog.ToStatusError(err)
	}
	return &snpb.ReadResponse{Payload: logEntry.Data, GLSN: req.GetGLSN(), LLSN: logEntry.LLSN}, nil
}

func (s *LogIOService) Subscribe(req *snpb.SubscribeRequest, stream snpb.LogIO_SubscribeServer) error {
	if req.GetGLSNBegin() >= req.GetGLSNEnd() {
		return varlog.ErrInvalidArgument
	}
	lse, ok := s.lseGetter.GetLogStreamExecutor(req.GetLogStreamID())
	if !ok {
		s.logger.Error("no logstreamexecutor", zap.Any("request", req))
		return varlog.ErrInvalid
	}
	ctx, cancel := context.WithCancel(context.TODO())
	defer cancel()
	resultC, err := lse.Subscribe(ctx, req.GetGLSNBegin(), req.GetGLSNEnd())
	if err != nil {
		s.logger.Error("could not subscribe", zap.Any("request", req), zap.Error(err))
		return varlog.ToStatusError(err)
	}
	for result := range resultC {
		if result.Err != nil {
			if result.Err == errEndOfRange {
				return nil
			}
			return result.Err
		}
		err := stream.Send(&snpb.SubscribeResponse{
			GLSN:    result.LogEntry.GLSN,
			LLSN:    result.LogEntry.LLSN,
			Payload: result.LogEntry.Data,
		})
		if err != nil {
			return err
		}
	}
	return ctx.Err()
}

func (s *LogIOService) Trim(ctx context.Context, req *snpb.TrimRequest) (*pbtypes.Empty, error) {
	var err error
	for _, lse := range s.lseGetter.GetLogStreamExecutors() {
		if e := lse.Trim(ctx, req.GetGLSN()); e != nil {
			err = e
		}
	}
	return &pbtypes.Empty{}, varlog.ToStatusError(err)
}
