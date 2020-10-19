package vms

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.daumkakao.com/varlog/varlog/pkg/varlog"
	"github.daumkakao.com/varlog/varlog/pkg/varlog/types"
	"github.daumkakao.com/varlog/varlog/proto/snpb"
	"github.daumkakao.com/varlog/varlog/proto/varlogpb"
	"go.uber.org/zap"
)

type StorageNodeManager interface {
	Init() error

	FindByStorageNodeID(storageNodeID types.StorageNodeID) varlog.ManagementClient

	FindByAddress(addr string) varlog.ManagementClient

	// GetMetadataByAddr returns metadata about a storage node. It is useful when id of the
	// storage node is not known.
	GetMetadataByAddr(ctx context.Context, addr string) (varlog.ManagementClient, *varlogpb.StorageNodeMetadataDescriptor, error)

	GetMetadata(ctx context.Context, storageNodeID types.StorageNodeID) (*varlogpb.StorageNodeMetadataDescriptor, error)

	AddStorageNode(ctx context.Context, snmcl varlog.ManagementClient) error

	AddLogStream(ctx context.Context, logStreamDesc *varlogpb.LogStreamDescriptor) error

	// Seal seals logstream replicas of storage nodes corresponded with the logStreamID. It
	// passes the last committed GLSN to the logstream replicas.
	Seal(ctx context.Context, logStreamID types.LogStreamID, lastCommittedGLSN types.GLSN) ([]varlogpb.LogStreamMetadataDescriptor, error)

	Sync(ctx context.Context, logStreamID types.LogStreamID) error

	Unseal(ctx context.Context, logStreamID types.LogStreamID) error

	Close() error
}

var _ StorageNodeManager = (*snManager)(nil)

type snManager struct {
	clusterID types.ClusterID

	cs     map[types.StorageNodeID]varlog.ManagementClient
	cmView ClusterMetadataView
	mu     sync.RWMutex

	logger *zap.Logger
}

func NewStorageNodeManager(cmView ClusterMetadataView, logger *zap.Logger) StorageNodeManager {
	if logger == nil {
		logger = zap.NewNop()
	}
	logger = logger.Named("snmanager")
	return &snManager{
		cmView: cmView,
		cs:     make(map[types.StorageNodeID]varlog.ManagementClient),
		logger: logger,
	}
}

func (sm *snManager) Init() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	meta, err := sm.cmView.ClusterMetadata(ctx)
	if err != nil {
		return err
	}

	for _, s := range meta.GetStorageNodes() {
		snmcl, _, err := sm.GetMetadataByAddr(ctx, s.Address)
		if err != nil {
			return err
		}
		storageNodeID := snmcl.PeerStorageNodeID()
		if sm.FindByStorageNodeID(storageNodeID) != nil {
			continue
		}

		sm.AddStorageNode(ctx, snmcl)
	}

	return nil
}

func (sm *snManager) Close() error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	var err error
	for id, cli := range sm.cs {
		if e := cli.Close(); e != nil {
			sm.logger.Error("could not close storagenode management client", zap.Any("snid", id), zap.Error(e))
			err = e
		}
	}
	return err
}

func (sm *snManager) FindByStorageNodeID(storageNodeID types.StorageNodeID) varlog.ManagementClient {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	snmcl, _ := sm.cs[storageNodeID]
	return snmcl
}

func (sm *snManager) FindByAddress(addr string) varlog.ManagementClient {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	for _, snmcl := range sm.cs {
		if snmcl.PeerAddress() == addr {
			return snmcl
		}
	}
	return nil
}

func (sm *snManager) GetMetadataByAddr(ctx context.Context, addr string) (varlog.ManagementClient, *varlogpb.StorageNodeMetadataDescriptor, error) {
	mc, err := varlog.NewManagementClient(ctx, sm.clusterID, addr, sm.logger)
	if err != nil {
		sm.logger.Error("could not create storagenode management client", zap.Error(err))
		return nil, nil, err
	}
	snMeta, err := mc.GetMetadata(ctx, snpb.MetadataTypeHeartbeat)
	if err != nil {
		if err := mc.Close(); err != nil {
			sm.logger.Error("could not close storagenode management client", zap.Error(err))
			return nil, nil, err
		}
		return nil, nil, err
	}
	return mc, snMeta, nil
}

func (sm *snManager) GetMetadata(ctx context.Context, storageNodeID types.StorageNodeID) (*varlogpb.StorageNodeMetadataDescriptor, error) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	snmcl, ok := sm.cs[storageNodeID]
	if !ok {
		sm.logger.Panic("no such storage node", zap.Any("snid", storageNodeID))
	}
	return snmcl.GetMetadata(ctx, snpb.MetadataTypeHeartbeat)
}

func (sm *snManager) AddStorageNode(ctx context.Context, snmcl varlog.ManagementClient) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	storageNodeID := snmcl.PeerStorageNodeID()
	if _, ok := sm.cs[storageNodeID]; ok {
		sm.logger.Panic("already registered storagenode", zap.Any("snid", storageNodeID))
	}
	sm.cs[storageNodeID] = snmcl
	return nil
}

func (sm *snManager) AddLogStream(ctx context.Context, logStreamDesc *varlogpb.LogStreamDescriptor) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	logStreamID := logStreamDesc.GetLogStreamID()
	replicaDescList := logStreamDesc.GetReplicas()

	// Assume that some logstreams are added to storagenode, but they don't registered to MRs.
	// At that time, the VMS is restarted. In that case, logstream id generator can generate
	// duplicated logstreamid.
	for _, replicaDesc := range replicaDescList {
		storageNodeID := replicaDesc.GetStorageNodeID()
		cli, ok := sm.cs[storageNodeID]
		if !ok {
			sm.logger.Panic("no such storage node", zap.Any("snid", storageNodeID))
		}
		snmeta, err := cli.GetMetadata(ctx, snpb.MetadataTypeLogStreams)
		if err != nil {
			return err
		}
		// transient error
		if _, exists := snmeta.FindLogStream(logStreamID); exists {
			return varlog.ErrLogStreamAlreadyExists
		}
	}

	for _, replicaDesc := range replicaDescList {
		storageNodeID := replicaDesc.GetStorageNodeID()
		cli := sm.cs[storageNodeID]
		if err := cli.AddLogStream(ctx, logStreamID, replicaDesc.GetPath()); err != nil {
			if errors.Is(err, varlog.ErrLogStreamAlreadyExists) {
				sm.logger.Panic("logstream should not exist", zap.Any("snid", storageNodeID), zap.Any("lsid", logStreamDesc.GetLogStreamID()), zap.Error(err))
			}
			return err
		}
	}
	return nil
}

func (sm *snManager) Seal(ctx context.Context, logStreamID types.LogStreamID, lastCommittedGLSN types.GLSN) ([]varlogpb.LogStreamMetadataDescriptor, error) {
	replicas, err := sm.replicas(ctx, logStreamID)
	if err != nil {
		return nil, err
	}
	lsmetaDesc := make([]varlogpb.LogStreamMetadataDescriptor, 0, len(replicas))
	for _, replica := range replicas {
		storageNodeID := replica.GetStorageNodeID()
		cli, ok := sm.cs[storageNodeID]
		if !ok {
			sm.logger.Panic("mismatch between clusterMetadataView and StorageNodeManager", zap.Any("snid", storageNodeID), zap.Any("lsid", logStreamID))
		}
		status, highWatermark, err := cli.Seal(ctx, logStreamID, lastCommittedGLSN)
		if err != nil {
			sm.logger.Warn("could not seal logstream replica", zap.Error(err), zap.Any("snid", storageNodeID), zap.Any("lsid", logStreamID))
			continue
		}
		lsmetaDesc = append(lsmetaDesc, varlogpb.LogStreamMetadataDescriptor{
			StorageNodeID: storageNodeID,
			LogStreamID:   logStreamID,
			Status:        status,
			HighWatermark: highWatermark,
			Path:          replica.GetPath(),
		})
		sm.logger.Debug("seal result", zap.Reflect("logstream_meta", lsmetaDesc))
	}
	return lsmetaDesc, nil
}

func (sm *snManager) Sync(ctx context.Context, logStreamID types.LogStreamID) error {
	panic("not implemented")
}

func (sm *snManager) Unseal(ctx context.Context, logStreamID types.LogStreamID) error {
	replicas, err := sm.replicas(ctx, logStreamID)
	if err != nil {
		return err
	}
	for _, replica := range replicas {
		storageNodeID := replica.GetStorageNodeID()
		cli, ok := sm.cs[storageNodeID]
		if !ok {
			sm.logger.Panic("mismatch between clusterMetadataView and StorageNodeManager", zap.Any("snid", storageNodeID), zap.Any("lsid", logStreamID))
		}
		if err := cli.Unseal(ctx, logStreamID); err != nil {
			return err
		}
	}
	return nil
}

func (sm *snManager) replicas(ctx context.Context, logStreamID types.LogStreamID) ([]*varlogpb.ReplicaDescriptor, error) {
	clusmeta, err := sm.cmView.ClusterMetadata(ctx)
	if err != nil {
		return nil, err
	}
	lsdesc := clusmeta.GetLogStream(logStreamID)
	replicas := lsdesc.GetReplicas()
	return replicas, nil
}
