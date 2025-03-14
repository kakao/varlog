package storagenode

import (
	"context"
	"errors"
	"io"
	"time"

	pbtypes "github.com/gogo/protobuf/types"
	"go.uber.org/multierr"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	snerrors "github.com/kakao/varlog/internal/storagenode/errors"
	"github.com/kakao/varlog/internal/storagenode/logstream"
	"github.com/kakao/varlog/internal/storagenode/telemetry"
	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/pkg/verrors"
	"github.com/kakao/varlog/proto/snpb"
)

type logServer struct {
	sn *StorageNode
}

var _ snpb.LogIOServer = (*logServer)(nil)

func (ls *logServer) Append(stream snpb.LogIO_AppendServer) error {
	// Avoid race of Add and Wait of wgAppenders.
	rt := ls.sn.mu.RLock()
	ls.sn.wgAppenders.Add(2)
	ls.sn.mu.RUnlock(rt)

	cq := make(chan *logstream.AppendTask, ls.sn.appendPipelineSize)

	go ls.appendStreamRecvLoop(stream, cq)

	var eg errgroup.Group
	eg.Go(func() error {
		return ls.appendStreamSendLoop(stream, cq)
	})
	err := eg.Wait()
	// The stream is finished by the client, which invokes CloseSend.
	// That result from appendStreamSendLoop is nil means follows:
	// - RecvMsg's return value is io.EOF.
	// - Completion queue is closed.
	// - AppendTasks in the completion queue are exhausted.
	if err == nil {
		ls.sn.wgAppenders.Done()
		return nil
	}

	// Drain completion queue.
	go ls.appendStreamDrainCQLoop(cq)

	// The stream is finished by returning io.EOF after calling SendMsg.
	if err == io.EOF {
		return nil
	}

	var code codes.Code
	switch err {
	case verrors.ErrSealed:
		code = codes.FailedPrecondition
	case snerrors.ErrNotPrimary:
		code = codes.Unavailable
	default:
		code = status.Code(err)
		if code == codes.Unknown {
			code = status.FromContextError(err).Code()
		}

	}
	return status.Error(code, err.Error())
}

func (ls *logServer) appendStreamRecvLoop(stream snpb.LogIO_AppendServer, cq chan<- *logstream.AppendTask) {
	defer func() {
		close(cq)
		ls.sn.wgAppenders.Done()
	}()

	var (
		appendTask *logstream.AppendTask
		lse        *logstream.Executor
		err        error
		loaded     bool
		tpid       types.TopicID
		lsid       types.LogStreamID
	)
	req := &snpb.AppendRequest{}
	ctx := stream.Context()

	for {
		req.Reset()
		err = stream.RecvMsg(req)
		if err == io.EOF {
			return
		}
		appendTask = logstream.NewAppendTask()
		if err != nil {
			goto Out
		}

		if len(req.Payload) == 0 {
			err = status.Error(codes.InvalidArgument, "no payload")
			goto Out
		}

		if tpid.Invalid() && lsid.Invalid() {
			err = snpb.ValidateTopicLogStream(req)
			if err != nil {
				err = status.Error(codes.InvalidArgument, err.Error())
				goto Out
			}
			tpid = req.TopicID
			lsid = req.LogStreamID
		}

		appendTask.LogStreamID = lsid
		appendTask.RPCStartTime = time.Now()

		if req.TopicID != tpid || req.LogStreamID != lsid {
			err = status.Error(codes.InvalidArgument, "unmatched topic or logstream")
			goto Out
		}

		if lse == nil {
			lse, loaded = ls.sn.executors.Load(tpid, lsid)
			if !loaded {
				err = status.Error(codes.NotFound, "no such log stream")
				goto Out
			}
		}

		err = lse.AppendAsync(ctx, req.Payload, appendTask)
	Out:
		if err != nil {
			appendTask.SetError(err)
		}
		cq <- appendTask
		if err != nil {
			return
		}
	}
}

func (ls *logServer) appendStreamSendLoop(stream snpb.LogIO_AppendServer, cq <-chan *logstream.AppendTask) (err error) {
	var res []snpb.AppendResult
	rsp := &snpb.AppendResponse{}
	ctx := stream.Context()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case appendTask, ok := <-cq:
			if !ok {
				return nil
			}

			lsid := appendTask.LogStreamID
			res, err = appendTask.WaitForCompletion(ctx)
			elapsed := time.Since(appendTask.RPCStartTime)
			if err != nil {
				appendTask.Release()
				goto RecordMetric
			}
			appendTask.ReleaseWriteWaitGroup()
			appendTask.Release()

			rsp.Results = res
			err = stream.Send(rsp)

		RecordMetric:
			code := codes.OK
			if err != nil {
				code = codes.Internal
				if errors.Is(err, context.DeadlineExceeded) {
					code = codes.DeadlineExceeded
				}
				if errors.Is(err, context.Canceled) {
					code = codes.Canceled
				}
				if errors.Is(err, verrors.ErrSealed) {
					code = codes.FailedPrecondition
				}
			}
			if !lsid.Invalid() {
				metrics, ok := ls.sn.metrics.GetLogStreamMetrics(lsid)
				if ok {
					metrics.LogRPCServerDuration.Record(ctx, telemetry.RPCKindAppend, code, elapsed.Microseconds())
				}
			}
			if err != nil {
				return err
			}
		}
	}
}

func (ls *logServer) appendStreamDrainCQLoop(cq <-chan *logstream.AppendTask) {
	defer ls.sn.wgAppenders.Done()
	for appendTask := range cq {
		appendTask.Release()
	}
}

func (ls *logServer) Read(context.Context, *snpb.ReadRequest) (*snpb.ReadResponse, error) {
	return nil, status.Error(codes.Unimplemented, "deprecated")
}

func (ls *logServer) Subscribe(req *snpb.SubscribeRequest, stream snpb.LogIO_SubscribeServer) error {
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

func (ls *logServer) SubscribeTo(req *snpb.SubscribeToRequest, stream snpb.LogIO_SubscribeToServer) (err error) {
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

func (ls *logServer) TrimDeprecated(ctx context.Context, req *snpb.TrimDeprecatedRequest) (*pbtypes.Empty, error) {
	ls.sn.executors.Range(func(_ types.LogStreamID, tpid types.TopicID, lse *logstream.Executor) bool {
		if req.TopicID != tpid {
			return true
		}
		_ = lse.Trim(ctx, req.GLSN)
		return true
	})
	return &pbtypes.Empty{}, nil
}

func (ls *logServer) LogStreamReplicaMetadata(_ context.Context, req *snpb.LogStreamReplicaMetadataRequest) (*snpb.LogStreamReplicaMetadataResponse, error) {
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
