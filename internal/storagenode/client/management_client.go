package client

//go:generate mockgen -build_flags -mod=vendor -self_package github.com/kakao/varlog/internal/storagenode/client -package client -destination management_client_mock.go . StorageNodeManagementClient

import (
	"context"

	"github.com/pkg/errors"
	"go.uber.org/multierr"
	"go.uber.org/zap"

	"github.com/kakao/varlog/pkg/rpc"
	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/pkg/util/stringsutil"
	"github.com/kakao/varlog/pkg/verrors"
	"github.com/kakao/varlog/proto/snpb"
	"github.com/kakao/varlog/proto/varlogpb"
)

type StorageNodeManagementClient interface {
	Target() varlogpb.StorageNode
	GetMetadata(ctx context.Context) (*snpb.StorageNodeMetadataDescriptor, error)
	AddLogStreamReplica(ctx context.Context, topicID types.TopicID, logStreamID types.LogStreamID, path string) error
	RemoveLogStream(ctx context.Context, topicID types.TopicID, logStreamID types.LogStreamID) error
	Seal(ctx context.Context, topicID types.TopicID, logStreamID types.LogStreamID, lastCommittedGLSN types.GLSN) (varlogpb.LogStreamStatus, types.GLSN, error)
	Unseal(ctx context.Context, topicID types.TopicID, logStreamID types.LogStreamID, replicas []varlogpb.LogStreamReplica) error
	Sync(ctx context.Context, topicID types.TopicID, logStreamID types.LogStreamID, backupStorageNodeID types.StorageNodeID, backupAddress string, lastGLSN types.GLSN) (*snpb.SyncStatus, error)
	Trim(ctx context.Context, topicID types.TopicID, lastGLSN types.GLSN) (map[types.LogStreamID]error, error)
	Close() error
}

type ManagementClient struct {
	rpcConn   *rpc.Conn
	rpcClient snpb.ManagementClient
	cid       types.ClusterID
	target    varlogpb.StorageNode
}

func NewManagementClient(ctx context.Context, clusterID types.ClusterID, address string, _ *zap.Logger) (StorageNodeManagementClient, error) {
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
	storageNodeID := rsp.GetStorageNodeMetadata().StorageNode.StorageNodeID

	return &ManagementClient{
		rpcConn:   rpcConn,
		rpcClient: rpcClient,
		cid:       clusterID,
		target: varlogpb.StorageNode{
			StorageNodeID: storageNodeID,
			Address:       address,
		},
	}, nil
}

func (c *ManagementClient) reset(rpcConn *rpc.Conn, cid types.ClusterID, target varlogpb.StorageNode) any {
	return &ManagementClient{
		rpcClient: snpb.NewManagementClient(rpcConn.Conn),
		cid:       cid,
		target:    target,
	}
}

func (c *ManagementClient) Target() varlogpb.StorageNode {
	return c.target
}

func (c *ManagementClient) GetMetadata(ctx context.Context) (*snpb.StorageNodeMetadataDescriptor, error) {
	rsp, err := c.rpcClient.GetMetadata(ctx, &snpb.GetMetadataRequest{
		ClusterID: c.cid,
	})
	return rsp.GetStorageNodeMetadata(), errors.WithMessagef(
		verrors.FromStatusError(err),
		"storage node (%v): get metadata", c.target,
	)
}

func (c *ManagementClient) AddLogStreamReplica(ctx context.Context, tpid types.TopicID, lsid types.LogStreamID, path string) error {
	if stringsutil.Empty(path) {
		return errors.New("snmcl: invalid argument")
	}
	// FIXME(jun): Does the return value of AddLogStream need?
	_, err := c.rpcClient.AddLogStreamReplica(ctx, &snpb.AddLogStreamReplicaRequest{
		ClusterID:     c.cid,
		StorageNodeID: c.target.StorageNodeID,
		TopicID:       tpid,
		LogStreamID:   lsid,
		Storage: &varlogpb.StorageDescriptor{
			Path: path,
		},
	})
	return errors.Wrap(verrors.FromStatusError(err), "snmcl")
}

func (c *ManagementClient) RemoveLogStream(ctx context.Context, tpid types.TopicID, lsid types.LogStreamID) error {
	_, err := c.rpcClient.RemoveLogStream(ctx, &snpb.RemoveLogStreamRequest{
		ClusterID:     c.cid,
		StorageNodeID: c.target.StorageNodeID,
		TopicID:       tpid,
		LogStreamID:   lsid,
	})
	return errors.Wrap(verrors.FromStatusError(err), "snmcl")
}

func (c *ManagementClient) Seal(ctx context.Context, tpid types.TopicID, lsid types.LogStreamID, lastCommittedGLSN types.GLSN) (varlogpb.LogStreamStatus, types.GLSN, error) {
	rsp, err := c.rpcClient.Seal(ctx, &snpb.SealRequest{
		ClusterID:         c.cid,
		StorageNodeID:     c.target.StorageNodeID,
		TopicID:           tpid,
		LogStreamID:       lsid,
		LastCommittedGLSN: lastCommittedGLSN,
	})
	return rsp.GetStatus(), rsp.GetLastCommittedGLSN(), errors.Wrap(verrors.FromStatusError(err), "snmcl")
}

func (c *ManagementClient) Unseal(ctx context.Context, tpid types.TopicID, lsid types.LogStreamID, replicas []varlogpb.LogStreamReplica) error {
	// TODO(jun): Check ranges CID, SNID and LSID
	_, err := c.rpcClient.Unseal(ctx, &snpb.UnsealRequest{
		ClusterID:     c.cid,
		StorageNodeID: c.target.StorageNodeID,
		TopicID:       tpid,
		LogStreamID:   lsid,
		Replicas:      replicas,
	})
	return errors.Wrap(verrors.FromStatusError(err), "snmcl")
}

func (c *ManagementClient) Sync(ctx context.Context, tpid types.TopicID, logStreamID types.LogStreamID, backupStorageNodeID types.StorageNodeID, backupAddress string, lastGLSN types.GLSN) (*snpb.SyncStatus, error) {
	rsp, err := c.rpcClient.Sync(ctx, &snpb.SyncRequest{
		ClusterID:     c.cid,
		StorageNodeID: c.target.StorageNodeID,
		TopicID:       tpid,
		LogStreamID:   logStreamID,
		Backup: &snpb.SyncRequest_BackupNode{
			StorageNodeID: backupStorageNodeID,
			Address:       backupAddress,
		},
	})
	return rsp.GetStatus(), errors.Wrap(verrors.FromStatusError(err), "snmcl")
}

func (c *ManagementClient) Trim(ctx context.Context, topicID types.TopicID, lastGLSN types.GLSN) (map[types.LogStreamID]error, error) {
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
// Deprecated: Use `Manager[*ManagementClient]`.
func (c *ManagementClient) Close() error {
	return c.rpcConn.Close()
}
