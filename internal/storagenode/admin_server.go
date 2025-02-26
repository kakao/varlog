package storagenode

import (
	"context"

	pbtypes "github.com/gogo/protobuf/types"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	snerrors "github.com/kakao/varlog/internal/storagenode/errors"
	"github.com/kakao/varlog/proto/snpb"
	"github.com/kakao/varlog/proto/varlogpb"
)

type adminServer struct {
	sn *StorageNode
}

var _ snpb.ManagementServer = (*adminServer)(nil)

func (as *adminServer) GetMetadata(ctx context.Context, _ *snpb.GetMetadataRequest) (*snpb.GetMetadataResponse, error) {
	snmd, err := as.sn.getMetadata(ctx)
	if err != nil {
		var code codes.Code
		switch err {
		case snerrors.ErrClosed:
			code = codes.Unavailable
		default:
			code = status.FromContextError(err).Code()
		}
		return nil, status.Error(code, err.Error())
	}
	return &snpb.GetMetadataResponse{StorageNodeMetadata: snmd}, nil
}

func (as *adminServer) AddLogStreamReplica(ctx context.Context, req *snpb.AddLogStreamReplicaRequest) (*snpb.AddLogStreamReplicaResponse, error) {
	lsrmd, err := as.sn.addLogStreamReplica(ctx, req.TopicID, req.LogStreamID, req.StorageNodePath)
	if err != nil {
		var code codes.Code
		switch err {
		case snerrors.ErrTooManyReplicas:
			code = codes.ResourceExhausted
		default:
			code = status.FromContextError(err).Code()
		}
		return nil, status.Error(code, err.Error())
	}
	rsp := &snpb.AddLogStreamReplicaResponse{
		LogStreamReplica: lsrmd,
	}
	return rsp, nil
}

func (as *adminServer) RemoveLogStream(ctx context.Context, req *snpb.RemoveLogStreamRequest) (*pbtypes.Empty, error) {
	err := as.sn.removeLogStreamReplica(ctx, req.TopicID, req.LogStreamID)
	if err != nil {
		var code codes.Code
		switch err {
		case snerrors.ErrClosed:
			code = codes.Unavailable
		case snerrors.ErrNotExist:
			code = codes.NotFound
		default:
			code = status.FromContextError(err).Code()
		}
		return nil, status.Error(code, err.Error())
	}
	return &pbtypes.Empty{}, nil
}

func (as *adminServer) Seal(ctx context.Context, req *snpb.SealRequest) (*snpb.SealResponse, error) {
	if err := snpb.ValidateTopicLogStream(req); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	lss, localHWM, err := as.sn.seal(ctx, req.TopicID, req.LogStreamID, req.LastCommittedGLSN)
	if err != nil {
		var code codes.Code
		switch err {
		case snerrors.ErrClosed:
			code = codes.Unavailable
		case snerrors.ErrNotExist:
			code = codes.NotFound
		default:
			code = status.FromContextError(err).Code()
		}
		as.sn.logger.Error("could not seal", zap.Any("code", code.String()), zap.Error(err))
		return nil, status.Error(code, err.Error())
	}
	return &snpb.SealResponse{
		Status:            lss,
		LastCommittedGLSN: localHWM,
	}, nil
}

func (as *adminServer) Unseal(ctx context.Context, req *snpb.UnsealRequest) (*pbtypes.Empty, error) {
	if err := snpb.ValidateTopicLogStream(req); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	err := as.sn.unseal(ctx, req.TopicID, req.LogStreamID, req.Replicas)
	if err != nil {
		var code codes.Code
		switch err {
		case snerrors.ErrClosed:
			code = codes.Unavailable
		case snerrors.ErrNotExist:
			code = codes.NotFound
		default:
			code = status.FromContextError(err).Code()
		}
		as.sn.logger.Error("could not unseal", zap.Any("code", code.String()), zap.Error(err))
		return nil, status.Error(code, err.Error())
	}
	return &pbtypes.Empty{}, nil
}

func (as *adminServer) Sync(ctx context.Context, req *snpb.SyncRequest) (*snpb.SyncResponse, error) {
	syncStatus, err := as.sn.sync(ctx, req.TopicID, req.LogStreamID, varlogpb.LogStreamReplica{
		StorageNode: varlogpb.StorageNode{
			StorageNodeID: req.Backup.StorageNodeID,
			Address:       req.Backup.Address,
		},
		TopicLogStream: varlogpb.TopicLogStream{
			TopicID:     req.TopicID,
			LogStreamID: req.LogStreamID,
		},
	})
	if err != nil {
		var code codes.Code
		switch err {
		case snerrors.ErrClosed:
			code = codes.Unavailable
		case snerrors.ErrNotExist:
			code = codes.NotFound
		default:
			code = status.FromContextError(err).Code()
		}
		return nil, status.Error(code, err.Error())
	}
	return &snpb.SyncResponse{Status: syncStatus}, nil
}

func (as *adminServer) Trim(ctx context.Context, req *snpb.TrimRequest) (*snpb.TrimResponse, error) {
	results := as.sn.trim(ctx, req.TopicID, req.LastGLSN)
	return &snpb.TrimResponse{Results: results}, nil
}
