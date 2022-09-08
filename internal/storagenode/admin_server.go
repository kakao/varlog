package storagenode

import (
	"context"

	pbtypes "github.com/gogo/protobuf/types"

	"github.com/kakao/varlog/proto/snpb"
	"github.com/kakao/varlog/proto/varlogpb"
)

type adminServer struct {
	sn *StorageNode
}

var _ snpb.ManagementServer = (*adminServer)(nil)

func (as *adminServer) GetMetadata(ctx context.Context, _ *snpb.GetMetadataRequest) (*snpb.GetMetadataResponse, error) {
	snmd, err := as.sn.getMetadata(ctx)
	return &snpb.GetMetadataResponse{StorageNodeMetadata: snmd}, err
}

func (as *adminServer) AddLogStreamReplica(ctx context.Context, req *snpb.AddLogStreamReplicaRequest) (*snpb.AddLogStreamReplicaResponse, error) {
	lsrmd, err := as.sn.addLogStreamReplica(ctx, req.TopicID, req.LogStreamID, req.StorageNodePath)
	if err != nil {
		return nil, err
	}
	rsp := &snpb.AddLogStreamReplicaResponse{
		LogStreamReplica: lsrmd,
	}
	return rsp, err
}

func (as *adminServer) RemoveLogStream(ctx context.Context, req *snpb.RemoveLogStreamRequest) (*pbtypes.Empty, error) {
	err := as.sn.removeLogStreamReplica(ctx, req.TopicID, req.LogStreamID)
	return &pbtypes.Empty{}, err
}

func (as *adminServer) Seal(ctx context.Context, req *snpb.SealRequest) (*snpb.SealResponse, error) {
	status, localHWM, err := as.sn.seal(ctx, req.TopicID, req.LogStreamID, req.LastCommittedGLSN)
	return &snpb.SealResponse{
		Status:            status,
		LastCommittedGLSN: localHWM,
	}, err
}

func (as *adminServer) Unseal(ctx context.Context, req *snpb.UnsealRequest) (*pbtypes.Empty, error) {
	err := as.sn.unseal(ctx, req.TopicID, req.LogStreamID, req.Replicas)
	return &pbtypes.Empty{}, err
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
	return &snpb.SyncResponse{Status: syncStatus}, err
}

func (as *adminServer) Trim(ctx context.Context, req *snpb.TrimRequest) (*snpb.TrimResponse, error) {
	results := as.sn.trim(ctx, req.TopicID, req.LastGLSN)
	return &snpb.TrimResponse{Results: results}, nil
}
