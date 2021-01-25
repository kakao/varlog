package vms

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"

	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.daumkakao.com/varlog/varlog/pkg/snc"
	"github.daumkakao.com/varlog/varlog/pkg/types"
	"github.daumkakao.com/varlog/varlog/proto/snpb"
	"github.daumkakao.com/varlog/varlog/proto/varlogpb"
)

type StorageNodeManager interface {
	Refresh(ctx context.Context) error

	Contains(storageNodeID types.StorageNodeID) bool

	ContainsAddress(addr string) bool

	// GetMetadataByAddr returns metadata about a storage node. It is useful when id of the
	// storage node is not known.
	GetMetadataByAddr(ctx context.Context, addr string) (snc.StorageNodeManagementClient, *varlogpb.StorageNodeMetadataDescriptor, error)

	GetMetadata(ctx context.Context, storageNodeID types.StorageNodeID) (*varlogpb.StorageNodeMetadataDescriptor, error)

	AddStorageNode(snmcl snc.StorageNodeManagementClient)

	RemoveStorageNode(storageNodeID types.StorageNodeID)

	AddLogStream(ctx context.Context, logStreamDesc *varlogpb.LogStreamDescriptor) error

	AddLogStreamReplica(ctx context.Context, storageNodeID types.StorageNodeID, logStreamID types.LogStreamID, path string) error

	RemoveLogStream(ctx context.Context, storageNodeID types.StorageNodeID, logStreamID types.LogStreamID) error

	// Seal seals logstream replicas of storage nodes corresponded with the logStreamID. It
	// passes the last committed GLSN to the logstream replicas.
	Seal(ctx context.Context, logStreamID types.LogStreamID, lastCommittedGLSN types.GLSN) ([]varlogpb.LogStreamMetadataDescriptor, error)

	Sync(ctx context.Context, logStreamID types.LogStreamID, srcID, dstID types.StorageNodeID, lastGLSN types.GLSN) (*snpb.SyncStatus, error)

	Unseal(ctx context.Context, logStreamID types.LogStreamID) error

	Close() error
}

var _ StorageNodeManager = (*snManager)(nil)

type snManager struct {
	clusterID types.ClusterID

	cs     map[types.StorageNodeID]snc.StorageNodeManagementClient
	cmView ClusterMetadataView
	mu     sync.RWMutex

	logger *zap.Logger
}

func NewStorageNodeManager(ctx context.Context, clusterID types.ClusterID, cmView ClusterMetadataView, logger *zap.Logger) (StorageNodeManager, error) {
	if logger == nil {
		logger = zap.NewNop()
	}
	logger = logger.Named("snmanager")
	sm := &snManager{
		clusterID: clusterID,
		cmView:    cmView,
		cs:        make(map[types.StorageNodeID]snc.StorageNodeManagementClient),
		logger:    logger,
	}

	sm.Refresh(ctx)
	return sm, nil
}

func (sm *snManager) Refresh(ctx context.Context) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	return sm.refresh(ctx)
}

func (sm *snManager) refresh(ctx context.Context) error {
	meta, err := sm.cmView.ClusterMetadata(ctx)
	if err != nil {
		return err
	}

	oldCS := make(map[types.StorageNodeID]snc.StorageNodeManagementClient, len(sm.cs))
	for storageNodeID, snmcl := range sm.cs {
		oldCS[storageNodeID] = snmcl
	}

	var mu sync.Mutex
	errSN := make([]string, 0, len(sm.cs))
	newCS := make(map[types.StorageNodeID]snc.StorageNodeManagementClient, len(sm.cs))

	g, ctx := errgroup.WithContext(ctx)
	sndescList := meta.GetStorageNodes()
	for i := range sndescList {
		sndesc := sndescList[i]
		storageNodeID := sndesc.GetStorageNodeID()
		g.Go(func() error {
			if snmcl, ok := oldCS[storageNodeID]; ok {
				if _, err := snmcl.GetMetadata(ctx); err == nil {
					mu.Lock()
					defer mu.Unlock()
					newCS[storageNodeID] = snmcl
					delete(oldCS, storageNodeID)
					return nil
				}
			}

			if snmcl, _, err := sm.GetMetadataByAddr(ctx, sndesc.GetAddress()); err == nil {
				mu.Lock()
				defer mu.Unlock()
				newCS[storageNodeID] = snmcl
				return nil
			}

			sm.logger.Error("storage node refresh failed", zap.Uint32("snid", uint32(storageNodeID)))
			mu.Lock()
			defer mu.Unlock()
			errSN = append(errSN, strconv.FormatUint(uint64(storageNodeID), 10))
			return nil
		})
	}
	g.Wait()

	sm.cs = newCS
	for _, snmcl := range oldCS {
		if err := snmcl.Close(); err != nil {
			sm.logger.Error("could not close storage node manager client", zap.Error(err))
		}
	}

	if len(errSN) > 0 {
		return fmt.Errorf("snamanger: fresh failed storage nodes " + strings.Join(errSN, ","))
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

func (sm *snManager) Contains(storageNodeID types.StorageNodeID) bool {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	_, ok := sm.cs[storageNodeID]
	return ok
}

func (sm *snManager) ContainsAddress(addr string) bool {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	for _, snmcl := range sm.cs {
		if snmcl.PeerAddress() == addr {
			return true
		}
	}
	return false
}

func (sm *snManager) GetMetadataByAddr(ctx context.Context, addr string) (snc.StorageNodeManagementClient, *varlogpb.StorageNodeMetadataDescriptor, error) {
	mc, err := snc.NewManagementClient(ctx, sm.clusterID, addr, sm.logger)
	if err != nil {
		sm.logger.Error("could not create storagenode management client", zap.Error(err))
		return nil, nil, err
	}
	snMeta, err := mc.GetMetadata(ctx)
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
		err := errors.New("no such storage node")
		sm.logger.Warn("no such storage node", zap.Any("snid", storageNodeID), zap.Error(err))
		sm.refresh(ctx)
		return nil, err
	}
	return snmcl.GetMetadata(ctx)
}

func (sm *snManager) AddStorageNode(snmcl snc.StorageNodeManagementClient) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.addStorageNode(snmcl)
}

func (sm *snManager) addStorageNode(snmcl snc.StorageNodeManagementClient) {
	storageNodeID := snmcl.PeerStorageNodeID()
	if _, ok := sm.cs[storageNodeID]; ok {
		sm.logger.Panic("already registered storagenode", zap.Any("snid", storageNodeID))
	}
	sm.cs[storageNodeID] = snmcl
}

func (sm *snManager) RemoveStorageNode(storageNodeID types.StorageNodeID) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	snmcl, ok := sm.cs[storageNodeID]
	if !ok {
		sm.logger.Warn("tried to remove nonexistent storage node", zap.Any("snid", storageNodeID))
		return
	}
	delete(sm.cs, storageNodeID)
	if err := snmcl.Close(); err != nil {
		sm.logger.Warn("error while closing storage node management client", zap.Error(err), zap.Any("snid", storageNodeID))
	}
}

func (sm *snManager) AddLogStreamReplica(ctx context.Context, storageNodeID types.StorageNodeID, logStreamID types.LogStreamID, path string) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	return sm.addLogStreamReplica(ctx, storageNodeID, logStreamID, path)
}

func (sm *snManager) addLogStreamReplica(ctx context.Context, snid types.StorageNodeID, lsid types.LogStreamID, path string) error {
	snmcl, ok := sm.cs[snid]
	if !ok {
		sm.refresh(ctx)
		return errors.New("no such storage node")
	}
	return snmcl.AddLogStream(ctx, lsid, path)
}

func (sm *snManager) AddLogStream(ctx context.Context, logStreamDesc *varlogpb.LogStreamDescriptor) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	logStreamID := logStreamDesc.GetLogStreamID()
	for _, replica := range logStreamDesc.GetReplicas() {
		err := sm.addLogStreamReplica(ctx, replica.GetStorageNodeID(), logStreamID, replica.GetPath())
		if err != nil {
			return err
		}
	}
	return nil
}

func (sm *snManager) RemoveLogStream(ctx context.Context, storageNodeID types.StorageNodeID, logStreamID types.LogStreamID) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	snmcl, ok := sm.cs[storageNodeID]
	if !ok {
		err := errors.New("no such storage node")
		sm.logger.Warn("no such storage node", zap.Any("snid", storageNodeID), zap.Error(err))
		sm.refresh(ctx)
		return err
	}
	return snmcl.RemoveLogStream(ctx, logStreamID)
}

func (sm *snManager) Seal(ctx context.Context, logStreamID types.LogStreamID, lastCommittedGLSN types.GLSN) ([]varlogpb.LogStreamMetadataDescriptor, error) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	replicas, err := sm.replicas(ctx, logStreamID)
	if err != nil {
		return nil, err
	}
	lsmetaDesc := make([]varlogpb.LogStreamMetadataDescriptor, 0, len(replicas))
	for _, replica := range replicas {
		storageNodeID := replica.GetStorageNodeID()
		cli, ok := sm.cs[storageNodeID]
		if !ok {
			err := errors.New("no such storage node")
			sm.logger.Warn("no such storage node", zap.Any("snid", storageNodeID), zap.Error(err))
			sm.refresh(ctx)
			return nil, err
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

func (sm *snManager) Sync(ctx context.Context, logStreamID types.LogStreamID, srcID, dstID types.StorageNodeID, lastGLSN types.GLSN) (*snpb.SyncStatus, error) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	replicas, err := sm.replicas(ctx, logStreamID)
	if err != nil {
		return nil, err
	}
	storageNodeIDs := make(map[types.StorageNodeID]bool, len(replicas))
	for _, replica := range replicas {
		storageNodeID := replica.GetStorageNodeID()
		storageNodeIDs[storageNodeID] = true
	}

	if !storageNodeIDs[srcID] || !storageNodeIDs[dstID] {
		err := errors.New("no such storage node")
		return nil, err
	}

	srcCli := sm.cs[srcID]
	dstCli := sm.cs[dstID]
	if srcCli == nil || dstCli == nil {
		err := errors.New("no such storage node")
		sm.logger.Warn("no such storage node", zap.Any("src_snid", srcID), zap.Any("dst_snid", dstID), zap.Error(err))
		sm.refresh(ctx)
		return nil, err
	}
	// TODO: check cluster meta if snids exist
	return srcCli.Sync(ctx, logStreamID, dstID, dstCli.PeerAddress(), lastGLSN)
}

func (sm *snManager) Unseal(ctx context.Context, logStreamID types.LogStreamID) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	replicas, err := sm.replicas(ctx, logStreamID)
	if err != nil {
		return err
	}
	for _, replica := range replicas {
		storageNodeID := replica.GetStorageNodeID()
		cli, ok := sm.cs[storageNodeID]
		if !ok {
			err := errors.New("no such storage node")
			sm.logger.Warn("no such storage node", zap.Any("snid", storageNodeID), zap.Error(err))
			sm.refresh(ctx)
			return err
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
