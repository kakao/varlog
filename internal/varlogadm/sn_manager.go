package varlogadm

import (
	"context"
	"sync"

	"github.com/pkg/errors"
	"go.uber.org/multierr"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.daumkakao.com/varlog/varlog/pkg/snc"
	"github.daumkakao.com/varlog/varlog/pkg/types"
	"github.daumkakao.com/varlog/varlog/pkg/util/container/set"
	"github.daumkakao.com/varlog/varlog/pkg/verrors"
	"github.daumkakao.com/varlog/varlog/proto/snpb"
	"github.daumkakao.com/varlog/varlog/proto/varlogpb"
	"github.daumkakao.com/varlog/varlog/proto/vmspb"
)

type StorageNodeManager interface {
	Refresh(ctx context.Context) error

	Contains(storageNodeID types.StorageNodeID) bool

	ContainsAddress(addr string) bool

	// GetMetadataByAddr returns metadata about a storage node. It is useful when id of the
	// storage node is not known.
	GetMetadataByAddr(ctx context.Context, addr string) (snc.StorageNodeManagementClient, *snpb.StorageNodeMetadataDescriptor, error)

	GetMetadata(ctx context.Context, storageNodeID types.StorageNodeID) (*snpb.StorageNodeMetadataDescriptor, error)

	AddStorageNode(snmcl snc.StorageNodeManagementClient)

	RemoveStorageNode(storageNodeID types.StorageNodeID)

	AddLogStream(ctx context.Context, logStreamDesc *varlogpb.LogStreamDescriptor) error

	AddLogStreamReplica(ctx context.Context, storageNodeID types.StorageNodeID, topicID types.TopicID, logStreamID types.LogStreamID, path string) error

	RemoveLogStream(ctx context.Context, storageNodeID types.StorageNodeID, topicID types.TopicID, logStreamID types.LogStreamID) error

	// Seal seals logstream replicas of storage nodes corresponded with the logStreamID. It
	// passes the last committed GLSN to the logstream replicas.
	Seal(ctx context.Context, topicID types.TopicID, logStreamID types.LogStreamID, lastCommittedGLSN types.GLSN) ([]snpb.LogStreamReplicaMetadataDescriptor, error)

	Sync(ctx context.Context, topicID types.TopicID, logStreamID types.LogStreamID, srcID, dstID types.StorageNodeID, lastGLSN types.GLSN) (*snpb.SyncStatus, error)

	Unseal(ctx context.Context, topicID types.TopicID, logStreamID types.LogStreamID) error

	Trim(ctx context.Context, topicID types.TopicID, lastGLSN types.GLSN) ([]vmspb.TrimResult, error)

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
	var errs error
	newCS := make(map[types.StorageNodeID]snc.StorageNodeManagementClient, len(sm.cs))

	g, ctx := errgroup.WithContext(ctx)
	sndescList := meta.GetStorageNodes()
	for i := range sndescList {
		sndesc := sndescList[i]
		storageNodeID := sndesc.GetStorageNodeID()
		g.Go(func() error {
			var err error
			snmcl, ok := oldCS[storageNodeID]
			if ok {
				if _, err = snmcl.GetMetadata(ctx); err == nil {
					mu.Lock()
					defer mu.Unlock()
					newCS[storageNodeID] = snmcl
					delete(oldCS, storageNodeID)
					return nil
				}
			}

			if snmcl, _, err = sm.GetMetadataByAddr(ctx, sndesc.GetAddress()); err == nil {
				mu.Lock()
				defer mu.Unlock()
				newCS[storageNodeID] = snmcl
				return nil
			}

			mu.Lock()
			defer mu.Unlock()
			errs = multierr.Append(errs, errors.WithMessagef(err, "snmanager: snid = %d", storageNodeID))
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

	return errs
}

func (sm *snManager) Close() (err error) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	for id, cli := range sm.cs {
		err = multierr.Append(err, errors.WithMessagef(cli.Close(), "snmanager: snid = %d", id))
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

func (sm *snManager) GetMetadataByAddr(ctx context.Context, addr string) (snc.StorageNodeManagementClient, *snpb.StorageNodeMetadataDescriptor, error) {
	mc, err := snc.NewManagementClient(ctx, sm.clusterID, addr, sm.logger)
	if err != nil {
		return nil, nil, err
	}
	snMeta, err := mc.GetMetadata(ctx)
	if err != nil {
		return nil, nil, multierr.Append(err, mc.Close())
	}
	return mc, snMeta, nil
}

func (sm *snManager) GetMetadata(ctx context.Context, storageNodeID types.StorageNodeID) (*snpb.StorageNodeMetadataDescriptor, error) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	snmcl, ok := sm.cs[storageNodeID]
	if !ok {
		sm.refresh(ctx)
		return nil, errors.Wrap(verrors.ErrNotExist, "storage node")
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
		sm.logger.Panic("already registered storagenode", zap.Int32("snid", int32(storageNodeID)))
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

func (sm *snManager) AddLogStreamReplica(ctx context.Context, storageNodeID types.StorageNodeID, topicID types.TopicID, logStreamID types.LogStreamID, path string) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	return sm.addLogStreamReplica(ctx, storageNodeID, topicID, logStreamID, path)
}

func (sm *snManager) addLogStreamReplica(ctx context.Context, snid types.StorageNodeID, topicid types.TopicID, lsid types.LogStreamID, path string) error {
	snmcl, ok := sm.cs[snid]
	if !ok {
		sm.refresh(ctx)
		return errors.Wrap(verrors.ErrNotExist, "storage node")
	}
	return snmcl.AddLogStreamReplica(ctx, topicid, lsid, path)
}

func (sm *snManager) AddLogStream(ctx context.Context, logStreamDesc *varlogpb.LogStreamDescriptor) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	topicID := logStreamDesc.GetTopicID()
	logStreamID := logStreamDesc.GetLogStreamID()
	for _, replica := range logStreamDesc.GetReplicas() {
		err := sm.addLogStreamReplica(ctx, replica.GetStorageNodeID(), topicID, logStreamID, replica.GetPath())
		if err != nil {
			return err
		}
	}
	return nil
}

func (sm *snManager) RemoveLogStream(ctx context.Context, storageNodeID types.StorageNodeID, topicID types.TopicID, logStreamID types.LogStreamID) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	snmcl, ok := sm.cs[storageNodeID]
	if !ok {
		sm.refresh(ctx)
		return errors.Wrap(verrors.ErrNotExist, "storage node")
	}
	return snmcl.RemoveLogStream(ctx, topicID, logStreamID)
}

func (sm *snManager) Seal(ctx context.Context, topicID types.TopicID, logStreamID types.LogStreamID, lastCommittedGLSN types.GLSN) ([]snpb.LogStreamReplicaMetadataDescriptor, error) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	var err error

	replicas, err := sm.replicaDescriptors(ctx, logStreamID)
	if err != nil {
		return nil, err
	}
	lsmetaDesc := make([]snpb.LogStreamReplicaMetadataDescriptor, 0, len(replicas))
	for _, replica := range replicas {
		storageNodeID := replica.GetStorageNodeID()
		cli, ok := sm.cs[storageNodeID]
		if !ok {
			sm.refresh(ctx)
			return nil, errors.Wrap(verrors.ErrNotExist, "storage node")
		}
		status, highWatermark, errSeal := cli.Seal(ctx, topicID, logStreamID, lastCommittedGLSN)
		if errSeal != nil {
			// NOTE: The sealing log stream ignores the failure of sealing its replica.
			sm.logger.Warn("could not seal replica", zap.Int32("snid", int32(storageNodeID)), zap.Int32("lsid", int32(logStreamID)))
			continue
		}
		lsmetaDesc = append(lsmetaDesc, snpb.LogStreamReplicaMetadataDescriptor{
			LogStreamReplica: varlogpb.LogStreamReplica{
				StorageNode: varlogpb.StorageNode{
					StorageNodeID: storageNodeID,
				},
				TopicLogStream: varlogpb.TopicLogStream{
					TopicID:     topicID,
					LogStreamID: logStreamID,
				},
			},
			Status: status,
			LocalHighWatermark: varlogpb.LogSequenceNumber{
				GLSN: highWatermark,
			},
			Path: replica.GetPath(),
		})
	}
	sm.logger.Info("seal result", zap.Reflect("logstream_meta", lsmetaDesc))
	return lsmetaDesc, err
}

func (sm *snManager) Sync(ctx context.Context, topicID types.TopicID, logStreamID types.LogStreamID, srcID, dstID types.StorageNodeID, lastGLSN types.GLSN) (*snpb.SyncStatus, error) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	replicas, err := sm.replicaDescriptors(ctx, logStreamID)
	if err != nil {
		return nil, err
	}
	// FIXME (jun): Tiny set/map may be slower than simple array. Check and fix it.
	storageNodeIDs := set.New(len(replicas))
	for _, replica := range replicas {
		storageNodeID := replica.GetStorageNodeID()
		storageNodeIDs.Add(storageNodeID)
	}

	if !storageNodeIDs.Contains(srcID) || !storageNodeIDs.Contains(dstID) {
		return nil, errors.Wrap(verrors.ErrNotExist, "storage node")
	}

	srcCli := sm.cs[srcID]
	dstCli := sm.cs[dstID]
	if srcCli == nil || dstCli == nil {
		sm.refresh(ctx)
		return nil, errors.Wrap(verrors.ErrNotExist, "storage node")
	}
	// TODO: check cluster meta if snids exist
	return srcCli.Sync(ctx, topicID, logStreamID, dstID, dstCli.PeerAddress(), lastGLSN)
}

func (sm *snManager) Unseal(ctx context.Context, topicID types.TopicID, logStreamID types.LogStreamID) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	rds, err := sm.replicaDescriptors(ctx, logStreamID)
	if err != nil {
		return err
	}

	replicas := make([]varlogpb.LogStreamReplica, 0, len(rds))
	for _, rd := range rds {
		replicas = append(replicas, varlogpb.LogStreamReplica{
			StorageNode: varlogpb.StorageNode{
				StorageNodeID: rd.StorageNodeID,
				Address:       sm.cs[rd.StorageNodeID].PeerAddress(),
			},
			TopicLogStream: varlogpb.TopicLogStream{
				TopicID:     topicID,
				LogStreamID: logStreamID,
			},
		})
	}

	// TODO: use errgroup
	for _, replica := range rds {
		storageNodeID := replica.GetStorageNodeID()
		cli, ok := sm.cs[storageNodeID]
		if !ok {
			sm.refresh(ctx)
			return errors.Wrap(verrors.ErrNotExist, "storage node")
		}
		if err := cli.Unseal(ctx, topicID, logStreamID, replicas); err != nil {
			return err
		}
	}
	return nil
}

func (sm *snManager) Trim(ctx context.Context, topicID types.TopicID, lastGLSN types.GLSN) ([]vmspb.TrimResult, error) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	clusmeta, err := sm.cmView.ClusterMetadata(ctx)
	if err != nil {
		return nil, err
	}
	td := clusmeta.GetTopic(topicID)
	if td == nil {
		return nil, errors.Errorf("trim: no such topic %d", topicID)
	}

	clients := make(map[types.StorageNodeID]snc.StorageNodeManagementClient)
	for _, lsid := range td.LogStreams {
		rds, err := sm.replicaDescriptors(ctx, lsid)
		if err != nil {
			return nil, err
		}
		for _, rd := range rds {
			if _, ok := clients[rd.StorageNodeID]; ok {
				continue
			}
			cli, ok := sm.cs[rd.StorageNodeID]
			if !ok {
				sm.refresh(ctx)
				return nil, err
			}
			clients[rd.StorageNodeID] = cli
		}
	}

	var (
		mu      sync.Mutex
		results []vmspb.TrimResult
	)
	g, ctx := errgroup.WithContext(ctx)
	for snid, client := range clients {
		snid := snid
		client := client
		g.Go(func() error {
			res, err := client.Trim(ctx, topicID, lastGLSN)
			if err != nil {
				return err
			}
			mu.Lock()
			defer mu.Unlock()
			for lsid, err := range res {
				var msg string
				if err != nil {
					msg = err.Error()
				}
				results = append(results, vmspb.TrimResult{
					StorageNodeID: snid,
					LogStreamID:   lsid,
					Error:         msg,
				})
			}
			return nil
		})
	}
	err = g.Wait()
	return results, err
}

func (sm *snManager) replicaDescriptors(ctx context.Context, logStreamID types.LogStreamID) ([]*varlogpb.ReplicaDescriptor, error) {
	clusmeta, err := sm.cmView.ClusterMetadata(ctx)
	if err != nil {
		return nil, err
	}
	lsdesc, err := clusmeta.MustHaveLogStream(logStreamID)
	if err != nil {
		return nil, err
	}
	return lsdesc.GetReplicas(), nil
}
