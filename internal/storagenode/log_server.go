package storagenode

import (
	"context"
	"errors"

	pbtypes "github.com/gogo/protobuf/types"
	"go.uber.org/multierr"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	snerrors "github.com/kakao/varlog/internal/storagenode/errors"
	"github.com/kakao/varlog/internal/storagenode/logstream"
	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/pkg/verrors"
	"github.com/kakao/varlog/proto/snpb"
)

type logServer struct {
	sn *StorageNode
}

var _ snpb.LogIOServer = (*logServer)(nil)

func (ls logServer) Append(ctx context.Context, req *snpb.AppendRequest) (*snpb.AppendResponse, error) {
	err := snpb.ValidateTopicLogStream(req)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	payload := req.GetPayload()
	req.Payload = nil
	lse, loaded := ls.sn.executors.Load(req.TopicID, req.LogStreamID)
	if !loaded {
		return nil, status.Error(codes.NotFound, "no such log stream")
	}

	res, err := lse.Append(ctx, payload)
	if err != nil {
		var code codes.Code
		switch err {
		case verrors.ErrSealed:
			code = codes.FailedPrecondition
		case snerrors.ErrNotPrimary:
			code = codes.Unavailable
		default:
			code = status.FromContextError(err).Code()
		}
		return nil, status.Error(code, err.Error())
	}
	return &snpb.AppendResponse{Results: res}, nil
}

func (ls logServer) Read(context.Context, *snpb.ReadRequest) (*snpb.ReadResponse, error) {
	return nil, status.Error(codes.Unimplemented, "deprecated")
}

func (ls logServer) Subscribe(req *snpb.SubscribeRequest, stream snpb.LogIO_SubscribeServer) error {
	if err := snpb.ValidateTopicLogStream(req); err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}

	lse, loaded := ls.sn.executors.Load(req.TopicID, req.LogStreamID)
	if !loaded {
		return status.Error(codes.NotFound, "no such log stream")
	}

	ctx := stream.Context()
	sr, err := lse.SubscribeWithGLSN(req.GLSNBegin, req.GLSNEnd)
	if err != nil {
		var code codes.Code
		if errors.Is(err, verrors.ErrClosed) {
			code = codes.Unavailable
		} else if errors.Is(err, verrors.ErrInvalid) {
			code = codes.InvalidArgument
		} else if errors.Is(err, verrors.ErrTrimmed) {
			code = codes.OutOfRange
		} else {
			code = status.FromContextError(err).Code()
		}
		return verrors.ToStatusErrorWithCode(err, code)
	}

	rsp := &snpb.SubscribeResponse{}
Loop:
	for {
		select {
		case <-ctx.Done():
			err = ctx.Err()
			break Loop
		case le, ok := <-sr.Result():
			if !ok {
				break Loop
			}
			rsp.GLSN = le.GLSN
			rsp.LLSN = le.LLSN
			rsp.Payload = le.Data
			err = stream.SendMsg(rsp)
			if err != nil {
				break Loop
			}
		}
	}
	sr.Stop()
	// FIXME: error propagation via gRPC is awkward.
	if err == nil && sr.Err() == nil {
		return nil
	}
	if err != nil {
		return status.Error(status.FromContextError(err).Code(), multierr.Append(err, sr.Err()).Error())
	}
	return status.Error(status.FromContextError(sr.Err()).Code(), sr.Err().Error())
}

func (ls logServer) SubscribeTo(req *snpb.SubscribeToRequest, stream snpb.LogIO_SubscribeToServer) (err error) {
	err = snpb.ValidateTopicLogStream(req)
	if err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}

	lse, loaded := ls.sn.executors.Load(req.TopicID, req.LogStreamID)
	if !loaded {
		return status.Error(codes.NotFound, "no such log stream")
	}

	ctx := stream.Context()
	sr, err := lse.SubscribeWithLLSN(req.LLSNBegin, req.LLSNEnd)
	if err != nil {
		var code codes.Code
		if errors.Is(err, verrors.ErrClosed) {
			code = codes.Unavailable
		} else if errors.Is(err, verrors.ErrInvalid) {
			code = codes.InvalidArgument
		} else if errors.Is(err, verrors.ErrTrimmed) {
			code = codes.OutOfRange
		} else {
			code = status.FromContextError(err).Code()
		}
		return verrors.ToStatusErrorWithCode(err, code)
	}

	rsp := &snpb.SubscribeToResponse{}
Loop:
	for {
		select {
		case <-ctx.Done():
			err = ctx.Err()
			break Loop
		case le, ok := <-sr.Result():
			if !ok {
				break Loop
			}
			rsp.LogEntry = le
			err = stream.SendMsg(rsp)
			if err != nil {
				break Loop
			}
		}
	}
	sr.Stop()
	return multierr.Append(err, sr.Err())
}

func (ls logServer) TrimDeprecated(ctx context.Context, req *snpb.TrimDeprecatedRequest) (*pbtypes.Empty, error) {
	ls.sn.executors.Range(func(_ types.LogStreamID, tpid types.TopicID, lse *logstream.Executor) bool {
		if req.TopicID != tpid {
			return true
		}
		_ = lse.Trim(ctx, req.GLSN)
		return true
	})
	return &pbtypes.Empty{}, nil
}

func (ls logServer) LogStreamReplicaMetadata(_ context.Context, req *snpb.LogStreamReplicaMetadataRequest) (*snpb.LogStreamReplicaMetadataResponse, error) {
	if err := snpb.ValidateTopicLogStream(req); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	lse, loaded := ls.sn.executors.Load(req.TopicID, req.LogStreamID)
	if !loaded {
		return nil, status.Error(codes.NotFound, "no such log stream")
	}

	lsrmd, err := lse.Metadata()
	if err != nil {
		if err == verrors.ErrClosed {
			return nil, status.Error(codes.Unavailable, err.Error())
		}
		return nil, status.Error(status.FromContextError(err).Code(), err.Error())
	}
	return &snpb.LogStreamReplicaMetadataResponse{LogStreamReplica: lsrmd}, nil
}
