package varlog

import (
	"context"

	"github.com/kakao/varlog/pkg/varlog/types"
	"github.com/kakao/varlog/pkg/varlog/util/stringsutil"
	"github.com/kakao/varlog/proto/snpb"
	"github.com/kakao/varlog/proto/varlogpb"
	"go.uber.org/zap"
)

type ManagementClient interface {
	PeerAddress() string
	PeerStorageNodeID() types.StorageNodeID
	GetMetadata(ctx context.Context, metadataType snpb.MetadataType) (*varlogpb.StorageNodeMetadataDescriptor, error)
	AddLogStream(ctx context.Context, logStreamID types.LogStreamID, path string) error
	RemoveLogStream(ctx context.Context, logStreamID types.LogStreamID) error
	Seal(ctx context.Context, logStreamID types.LogStreamID, lastCommittedGLSN types.GLSN) (varlogpb.LogStreamStatus, types.GLSN, error)
	Unseal(ctx context.Context, logStreamID types.LogStreamID) error
	Sync(ctx context.Context, logStreamID types.LogStreamID, backupStorageNodeID types.StorageNodeID, backupAddress string, lastGLSN types.GLSN) (*snpb.SyncStatus, error)
	Close() error
}

type managementClient struct {
	rpcConn   *RpcConn
	rpcClient snpb.ManagementClient

	clusterID     types.ClusterID
	address       string
	storageNodeID types.StorageNodeID

	logger *zap.Logger
}

func NewManagementClient(ctx context.Context, clusterID types.ClusterID, address string, logger *zap.Logger) (ManagementClient, error) {
	if logger == nil {
		logger = zap.NewNop()
	}
	logger = logger.Named("snmcl").With(zap.Any("peer_addr", address))

	rpcConn, err := NewRpcConn(address)
	if err != nil {
		logger.Error("could not connect to storagenode", zap.Error(err))
		return nil, err
	}
	rpcClient := snpb.NewManagementClient(rpcConn.Conn)
	rsp, err := rpcClient.GetMetadata(ctx, &snpb.GetMetadataRequest{
		ClusterID:    clusterID,
		MetadataType: snpb.MetadataTypeHeartbeat,
	})
	if err != nil {
		if closeErr := rpcConn.Close(); closeErr != nil {
			logger.Error("error while closing connection to storagenode", zap.Error(err))
		}
		return nil, err
	}
	storageNodeID := rsp.GetStorageNodeMetadata().GetStorageNode().GetStorageNodeID()
	logger = logger.With(zap.Any("peer_snid", storageNodeID))

	return &managementClient{
		rpcConn:       rpcConn,
		rpcClient:     rpcClient,
		address:       address,
		clusterID:     clusterID,
		storageNodeID: storageNodeID,
		logger:        logger,
	}, nil
}

func (c managementClient) PeerAddress() string {
	return c.address
}

func (c managementClient) PeerStorageNodeID() types.StorageNodeID {
	return c.storageNodeID
}

func (c *managementClient) GetMetadata(ctx context.Context, metadataType snpb.MetadataType) (*varlogpb.StorageNodeMetadataDescriptor, error) {
	rsp, err := c.rpcClient.GetMetadata(ctx, &snpb.GetMetadataRequest{
		ClusterID:    c.clusterID,
		MetadataType: metadataType,
	})
	if err != nil {
		return nil, err
	}
	return rsp.GetStorageNodeMetadata(), nil
}

func (c *managementClient) AddLogStream(ctx context.Context, lsid types.LogStreamID, path string) error {
	// TODO(jun): Check ranges CID, SNID and LSID
	if stringsutil.Empty(path) {
		return ErrInvalid // FIXME: ErrInvalid ErrInvalidArgument
	}
	// FIXME(jun): Does the return value of AddLogStream need?
	_, err := c.rpcClient.AddLogStream(ctx, &snpb.AddLogStreamRequest{
		ClusterID:     c.clusterID,
		StorageNodeID: c.storageNodeID,
		LogStreamID:   lsid,
		Storage: &varlogpb.StorageDescriptor{
			Path: path,
		},
	})
	return err

}

func (c *managementClient) RemoveLogStream(ctx context.Context, lsid types.LogStreamID) error {
	// TODO(jun): Check ranges CID, SNID and LSID
	_, err := c.rpcClient.RemoveLogStream(ctx, &snpb.RemoveLogStreamRequest{
		ClusterID:     c.clusterID,
		StorageNodeID: c.storageNodeID,
		LogStreamID:   lsid,
	})
	return err
}

func (c *managementClient) Seal(ctx context.Context, lsid types.LogStreamID, lastCommittedGLSN types.GLSN) (varlogpb.LogStreamStatus, types.GLSN, error) {
	// TODO(jun): Check ranges CID, SNID and LSID
	rsp, err := c.rpcClient.Seal(ctx, &snpb.SealRequest{
		ClusterID:         c.clusterID,
		StorageNodeID:     c.storageNodeID,
		LogStreamID:       lsid,
		LastCommittedGLSN: lastCommittedGLSN,
	})
	return rsp.GetStatus(), rsp.GetLastCommittedGLSN(), err
}

func (c *managementClient) Unseal(ctx context.Context, lsid types.LogStreamID) error {
	// TODO(jun): Check ranges CID, SNID and LSID
	_, err := c.rpcClient.Unseal(ctx, &snpb.UnsealRequest{
		ClusterID:     c.clusterID,
		StorageNodeID: c.storageNodeID,
		LogStreamID:   lsid,
	})
	return err
}

func (c *managementClient) Sync(ctx context.Context, logStreamID types.LogStreamID, backupStorageNodeID types.StorageNodeID, backupAddress string, lastGLSN types.GLSN) (*snpb.SyncStatus, error) {
	rsp, err := c.rpcClient.Sync(ctx, &snpb.SyncRequest{
		ClusterID:     c.clusterID,
		StorageNodeID: c.storageNodeID,
		LogStreamID:   logStreamID,
		Backup: &snpb.SyncRequest_BackupNode{
			StorageNodeID: backupStorageNodeID,
			Address:       backupAddress,
		},
		LastGLSN: lastGLSN,
	})
	return rsp.GetStatus(), err
}

// Close closes connection to the storage node.
func (c *managementClient) Close() error {
	return c.rpcConn.Close()
}
