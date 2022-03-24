package snc

//go:generate mockgen -build_flags -mod=vendor -self_package github.daumkakao.com/varlog/varlog/pkg/snc -package snc -destination snc_mock.go . StorageNodeManagementClient

import (
	"context"

	"github.com/pkg/errors"
	"go.uber.org/multierr"
	"go.uber.org/zap"

	"github.daumkakao.com/varlog/varlog/pkg/rpc"
	"github.daumkakao.com/varlog/varlog/pkg/types"
	"github.daumkakao.com/varlog/varlog/pkg/util/stringsutil"
	"github.daumkakao.com/varlog/varlog/pkg/verrors"
	"github.daumkakao.com/varlog/varlog/proto/snpb"
	"github.daumkakao.com/varlog/varlog/proto/varlogpb"
)

type StorageNodeManagementClient interface {
	PeerAddress() string
	PeerStorageNodeID() types.StorageNodeID
	GetMetadata(ctx context.Context) (*varlogpb.StorageNodeMetadataDescriptor, error)
	AddLogStreamReplica(ctx context.Context, topicID types.TopicID, logStreamID types.LogStreamID, path string) error
	RemoveLogStream(ctx context.Context, topicID types.TopicID, logStreamID types.LogStreamID) error
	Seal(ctx context.Context, topicID types.TopicID, logStreamID types.LogStreamID, lastCommittedGLSN types.GLSN) (varlogpb.LogStreamStatus, types.GLSN, error)
	Unseal(ctx context.Context, topicID types.TopicID, logStreamID types.LogStreamID, replicas []varlogpb.Replica) error
	Sync(ctx context.Context, topicID types.TopicID, logStreamID types.LogStreamID, backupStorageNodeID types.StorageNodeID, backupAddress string, lastGLSN types.GLSN) (*snpb.SyncStatus, error)
	GetPrevCommitInfo(ctx context.Context, ver types.Version) (*snpb.GetPrevCommitInfoResponse, error)
	Trim(ctx context.Context, topicID types.TopicID, lastGLSN types.GLSN) (map[types.LogStreamID]error, error)
	Close() error
}

type snManagementClient struct {
	rpcConn   *rpc.Conn
	rpcClient snpb.ManagementClient

	clusterID     types.ClusterID
	address       string
	storageNodeID types.StorageNodeID

	logger *zap.Logger
}

func NewManagementClient(ctx context.Context, clusterID types.ClusterID, address string, logger *zap.Logger) (StorageNodeManagementClient, error) {
	if logger == nil {
		logger = zap.NewNop()
	}
	logger = logger.Named("snmcl").With(zap.Any("peer_addr", address))

	rpcConn, err := rpc.NewConn(ctx, address)
	if err != nil {
		return nil, err
	}
	rpcClient := snpb.NewManagementClient(rpcConn.Conn)
	rsp, err := rpcClient.GetMetadata(ctx, &snpb.GetMetadataRequest{
		ClusterID: clusterID,
	})
	if err != nil {
		return nil, multierr.Append(err, rpcConn.Close())
	}
	storageNodeID := rsp.GetStorageNodeMetadata().GetStorageNode().GetStorageNodeID()
	logger = logger.With(zap.Any("peer_snid", storageNodeID))

	return &snManagementClient{
		rpcConn:       rpcConn,
		rpcClient:     rpcClient,
		address:       address,
		clusterID:     clusterID,
		storageNodeID: storageNodeID,
		logger:        logger,
	}, nil
}

func (c snManagementClient) PeerAddress() string {
	return c.address
}

func (c snManagementClient) PeerStorageNodeID() types.StorageNodeID {
	return c.storageNodeID
}

func (c *snManagementClient) GetMetadata(ctx context.Context) (*varlogpb.StorageNodeMetadataDescriptor, error) {
	rsp, err := c.rpcClient.GetMetadata(ctx, &snpb.GetMetadataRequest{
		ClusterID: c.clusterID,
	})
	return rsp.GetStorageNodeMetadata(), errors.Wrap(verrors.FromStatusError(err), "snmcl")
}

func (c *snManagementClient) AddLogStreamReplica(ctx context.Context, tpid types.TopicID, lsid types.LogStreamID, path string) error {
	if stringsutil.Empty(path) {
		return errors.New("snmcl: invalid argument")
	}
	// FIXME(jun): Does the return value of AddLogStream need?
	_, err := c.rpcClient.AddLogStreamReplica(ctx, &snpb.AddLogStreamReplicaRequest{
		ClusterID:     c.clusterID,
		StorageNodeID: c.storageNodeID,
		TopicID:       tpid,
		LogStreamID:   lsid,
		Storage: &varlogpb.StorageDescriptor{
			Path: path,
		},
	})
	return errors.Wrap(verrors.FromStatusError(err), "snmcl")
}

func (c *snManagementClient) RemoveLogStream(ctx context.Context, tpid types.TopicID, lsid types.LogStreamID) error {
	_, err := c.rpcClient.RemoveLogStream(ctx, &snpb.RemoveLogStreamRequest{
		ClusterID:     c.clusterID,
		StorageNodeID: c.storageNodeID,
		TopicID:       tpid,
		LogStreamID:   lsid,
	})
	return errors.Wrap(verrors.FromStatusError(err), "snmcl")
}

func (c *snManagementClient) Seal(ctx context.Context, tpid types.TopicID, lsid types.LogStreamID, lastCommittedGLSN types.GLSN) (varlogpb.LogStreamStatus, types.GLSN, error) {
	rsp, err := c.rpcClient.Seal(ctx, &snpb.SealRequest{
		ClusterID:         c.clusterID,
		StorageNodeID:     c.storageNodeID,
		TopicID:           tpid,
		LogStreamID:       lsid,
		LastCommittedGLSN: lastCommittedGLSN,
	})
	return rsp.GetStatus(), rsp.GetLastCommittedGLSN(), errors.Wrap(verrors.FromStatusError(err), "snmcl")
}

func (c *snManagementClient) Unseal(ctx context.Context, tpid types.TopicID, lsid types.LogStreamID, replicas []varlogpb.Replica) error {
	// TODO(jun): Check ranges CID, SNID and LSID
	_, err := c.rpcClient.Unseal(ctx, &snpb.UnsealRequest{
		ClusterID:     c.clusterID,
		StorageNodeID: c.storageNodeID,
		TopicID:       tpid,
		LogStreamID:   lsid,
		Replicas:      replicas,
	})
	return errors.Wrap(verrors.FromStatusError(err), "snmcl")
}

func (c *snManagementClient) Sync(ctx context.Context, tpid types.TopicID, logStreamID types.LogStreamID, backupStorageNodeID types.StorageNodeID, backupAddress string, lastGLSN types.GLSN) (*snpb.SyncStatus, error) {
	rsp, err := c.rpcClient.Sync(ctx, &snpb.SyncRequest{
		ClusterID:     c.clusterID,
		StorageNodeID: c.storageNodeID,
		TopicID:       tpid,
		LogStreamID:   logStreamID,
		Backup: &snpb.SyncRequest_BackupNode{
			StorageNodeID: backupStorageNodeID,
			Address:       backupAddress,
		},
	})
	return rsp.GetStatus(), errors.Wrap(verrors.FromStatusError(err), "snmcl")
}

func (c *snManagementClient) GetPrevCommitInfo(ctx context.Context, prevVer types.Version) (*snpb.GetPrevCommitInfoResponse, error) {
	rsp, err := c.rpcClient.GetPrevCommitInfo(ctx, &snpb.GetPrevCommitInfoRequest{
		PrevVersion: prevVer,
	})
	return rsp, errors.WithStack(verrors.FromStatusError(err))
}

func (c *snManagementClient) Trim(ctx context.Context, topicID types.TopicID, lastGLSN types.GLSN) (map[types.LogStreamID]error, error) {
	rsp, err := c.rpcClient.Trim(ctx, &snpb.TrimRequest{
		TopicID:  topicID,
		LastGLSN: lastGLSN,
	})
	results := rsp.GetResults()
	ret := make(map[types.LogStreamID]error, len(results))
	for lsid, cause := range results {
		var err error
		if len(cause) > 0 {
			err = errors.New(cause)
		}
		ret[lsid] = err
	}
	return ret, errors.WithStack(verrors.FromStatusError(err))
}

// Close closes connection to the storage node.
func (c *snManagementClient) Close() error {
	return c.rpcConn.Close()
}
