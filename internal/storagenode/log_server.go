package storagenode

import (
	"context"
	"errors"

	pbtypes "github.com/gogo/protobuf/types"
	"go.uber.org/multierr"

	"github.daumkakao.com/varlog/varlog/internal/storagenode/logstream"
	"github.daumkakao.com/varlog/varlog/pkg/types"
	"github.daumkakao.com/varlog/varlog/pkg/verrors"
	"github.daumkakao.com/varlog/varlog/proto/snpb"
)

type logServer struct {
	sn *StorageNode
}

var _ snpb.LogIOServer = (*logServer)(nil)

func (ls logServer) Append(ctx context.Context, req *snpb.AppendRequest) (*snpb.AppendResponse, error) {
	payload := req.GetPayload()
	req.Payload = nil
	lse, loaded := ls.sn.executors.Load(req.TopicID, req.LogStreamID)
	if !loaded {
		return nil, errors.New("storage node: no such logstream")
	}
	res, err := lse.Append(ctx, payload)
	if err != nil {
		return nil, err
	}
	return &snpb.AppendResponse{Results: res}, nil
}

func (ls logServer) Read(context.Context, *snpb.ReadRequest) (*snpb.ReadResponse, error) {
	panic("not implemented")
}

func (ls logServer) Subscribe(req *snpb.SubscribeRequest, stream snpb.LogIO_SubscribeServer) error {
	lse, loaded := ls.sn.executors.Load(req.TopicID, req.LogStreamID)
	if !loaded {
		return errors.New("storage: no such logstream")
	}

	ctx := stream.Context()
	sr, err := lse.SubscribeWithGLSN(req.GLSNBegin, req.GLSNEnd)
	if err != nil {
		// FIXME: error propagation via gRPC is awkward.
		return verrors.ToStatusError(err)
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
	return verrors.ToStatusError(multierr.Append(err, sr.Err()))
}

func (ls logServer) SubscribeTo(req *snpb.SubscribeToRequest, stream snpb.LogIO_SubscribeToServer) (err error) {
	lse, loaded := ls.sn.executors.Load(req.TopicID, req.LogStreamID)
	if !loaded {
		return errors.New("storage: no such logstream")
	}

	ctx := stream.Context()
	sr, err := lse.SubscribeWithLLSN(req.LLSNBegin, req.LLSNEnd)
	if err != nil {
		return err
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

func (ls logServer) Trim(ctx context.Context, req *snpb.TrimRequest) (*pbtypes.Empty, error) {
	ls.sn.executors.Range(func(_ types.LogStreamID, tpid types.TopicID, lse *logstream.Executor) bool {
		if req.TopicID != tpid {
			return true
		}
		_ = lse.Trim(ctx, req.GLSN)
		return true
	})
	return &pbtypes.Empty{}, nil
}

func (ls logServer) LogStreamMetadata(_ context.Context, req *snpb.LogStreamMetadataRequest) (*snpb.LogStreamMetadataResponse, error) {
	lse, loaded := ls.sn.executors.Load(req.TopicID, req.LogStreamID)
	if !loaded {
		return nil, errors.New("storage: no such logstream")
	}

	lsd, err := lse.LogStreamMetadata()
	return &snpb.LogStreamMetadataResponse{LogStreamDescriptor: lsd}, err
}
