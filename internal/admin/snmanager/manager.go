package snmanager

//go:generate mockgen -build_flags -mod=vendor -self_package github.com/kakao/varlog/internal/admin/snmanager -package snmanager -destination manager_mock.go . StorageNodeManager

import (
	"context"
	"sync"

	"github.com/pkg/errors"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/kakao/varlog/internal/admin/admerrors"
	"github.com/kakao/varlog/internal/storagenode/client"
	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/pkg/util/container/set"
	"github.com/kakao/varlog/pkg/verrors"
	"github.com/kakao/varlog/proto/snpb"
	"github.com/kakao/varlog/proto/varlogpb"
	"github.com/kakao/varlog/proto/vmspb"
)

type StorageNodeManager interface {
	Contains(storageNodeID types.StorageNodeID) bool

	ContainsAddress(addr string) bool

	GetMetadataByAddress(ctx context.Context, snid types.StorageNodeID, addr string) (*snpb.StorageNodeMetadataDescriptor, error)

	// GetMetadata returns metadata for the storage node identified by the argument snid.
	GetMetadata(ctx context.Context, snid types.StorageNodeID) (*snpb.StorageNodeMetadataDescriptor, error)

	// AddStorageNode adds the storage node to the manager.
	// The new storage node should be registered to the metadata repository first.
	// It is idempotent - already registered one also can be passed by this method.
	// Note that this method cannot guarantee that the manager maintains the storage node immediately. However, the storage node is eventually managed since it is registered to the metadata repository.
	AddStorageNode(ctx context.Context, snid types.StorageNodeID, addr string)

	// RemoveStorageNode unregisters the storage node identified by the argument snid.
	RemoveStorageNode(snid types.StorageNodeID)

	// AddLogStream adds a new log stream to storage nodes.
	AddLogStream(ctx context.Context, lsd *varlogpb.LogStreamDescriptor) error

	// AddLogStreamReplica adds a new log stream replica to the storage node whose ID is the argument snid.
	// The new log stream replica is identified by the argument tpid and lsid.
	AddLogStreamReplica(ctx context.Context, snid types.StorageNodeID, tpid types.TopicID, lsid types.LogStreamID, path string) error

	RemoveLogStreamReplica(ctx context.Context, snid types.StorageNodeID, tpid types.TopicID, lsid types.LogStreamID) error

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
	config

	mu      sync.RWMutex
	clients *client.Manager[*client.ManagementClient]
}

func New(ctx context.Context, opts ...Option) (StorageNodeManager, error) {
	cfg, err := newConfig(opts)
	if err != nil {
		return nil, err
	}

	clients, err := client.NewManager[*client.ManagementClient]()
	if err != nil {
		return nil, err
	}

	sm := &snManager{
		config:  cfg,
		clients: clients,
	}

	sm.refresh(ctx)
	return sm, nil
}

func (sm *snManager) refresh(ctx context.Context) error {
	md, err := sm.cmview.ClusterMetadata(ctx)
	if err != nil {
		return err
	}

	var wg sync.WaitGroup
	snds := md.GetStorageNodes()
	for i := range snds {
		snd := snds[i]
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithCancel(ctx)
			defer cancel()
			sm.clients.GetOrConnect(ctx, snd.StorageNodeID, snd.Address)
		}()
	}
	wg.Wait()
	return nil
}

func (sm *snManager) Close() (err error) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	return sm.clients.Close()
}

func (sm *snManager) Contains(storageNodeID types.StorageNodeID) bool {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	_, err := sm.clients.Get(storageNodeID)
	return err == nil
}

func (sm *snManager) ContainsAddress(addr string) bool {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	_, err := sm.clients.GetByAddress(addr)
	return err == nil
}

func (sm *snManager) GetMetadataByAddress(ctx context.Context, snid types.StorageNodeID, addr string) (*snpb.StorageNodeMetadataDescriptor, error) {
	mgr, err := client.NewManager[*client.ManagementClient]()
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = mgr.Close()
	}()

	mc, err := mgr.GetOrConnect(ctx, snid, addr)
	if err != nil {
		return nil, err
	}

	return mc.GetMetadata(ctx)
}

func (sm *snManager) GetMetadata(ctx context.Context, snid types.StorageNodeID) (*snpb.StorageNodeMetadataDescriptor, error) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	mc, err := sm.clients.Get(snid)
	if err != nil {
		if !errors.Is(err, verrors.ErrClosed) {
			_ = sm.refresh(ctx)
			err = admerrors.ErrNoSuchStorageNode
		}
		return nil, errors.WithMessagef(err, "snmanager")
	}
	snmd, err := mc.GetMetadata(ctx)
	return snmd, errors.WithMessagef(err, "snmanager")
}

func (sm *snManager) AddStorageNode(ctx context.Context, snid types.StorageNodeID, addr string) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if _, err := sm.clients.GetOrConnect(ctx, snid, addr); err != nil {
		sm.logger.Warn("could not register storage node",
			zap.Int32("snid", int32(snid)),
			zap.String("addr", addr),
			zap.Error(err),
		)
	}
}

func (sm *snManager) RemoveStorageNode(snid types.StorageNodeID) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if err := sm.clients.CloseClient(snid); err != nil {
		sm.logger.Warn("close client",
			zap.Int32("snid", int32(snid)),
			zap.Error(err),
		)
	}
}

func (sm *snManager) AddLogStreamReplica(ctx context.Context, snid types.StorageNodeID, tpid types.TopicID, lsid types.LogStreamID, path string) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	return sm.addLogStreamReplica(ctx, snid, tpid, lsid, path)
}

func (sm *snManager) addLogStreamReplica(ctx context.Context, snid types.StorageNodeID, tpid types.TopicID, lsid types.LogStreamID, path string) error {
	mc, err := sm.clients.Get(snid)
	if err != nil {
		sm.refresh(ctx)
		return errors.Wrap(verrors.ErrNotExist, "storage node")
	}
	return mc.AddLogStreamReplica(ctx, tpid, lsid, path)
}

func (sm *snManager) AddLogStream(ctx context.Context, lsd *varlogpb.LogStreamDescriptor) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	tpid := lsd.GetTopicID()
	lsid := lsd.GetLogStreamID()
	g, ctx := errgroup.WithContext(ctx)
	for i := range lsd.GetReplicas() {
		rd := lsd.Replicas[i]
		g.Go(func() error {
			return sm.addLogStreamReplica(ctx, rd.StorageNodeID, tpid, lsid, rd.Path)
		})
	}
	return g.Wait()
}

func (sm *snManager) RemoveLogStreamReplica(ctx context.Context, snid types.StorageNodeID, tpid types.TopicID, lsid types.LogStreamID) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	mc, err := sm.clients.Get(snid)
	if err != nil {
		sm.refresh(ctx)
		return errors.Wrap(verrors.ErrNotExist, "storage node")
	}
	return mc.RemoveLogStream(ctx, tpid, lsid)
}

func (sm *snManager) Seal(ctx context.Context, tpid types.TopicID, lsid types.LogStreamID, lastCommittedGLSN types.GLSN) ([]snpb.LogStreamReplicaMetadataDescriptor, error) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	var err error

	replicas, err := sm.replicaDescriptors(ctx, lsid)
	if err != nil {
		return nil, err
	}
	lsmetaDesc := make([]snpb.LogStreamReplicaMetadataDescriptor, 0, len(replicas))
	for _, replica := range replicas {
		storageNodeID := replica.GetStorageNodeID()
		cli, err := sm.clients.Get(storageNodeID)
		if err != nil {
			sm.refresh(ctx)
			return nil, errors.Wrap(verrors.ErrNotExist, "storage node")
		}
		status, highWatermark, errSeal := cli.Seal(ctx, tpid, lsid, lastCommittedGLSN)
		if errSeal != nil {
			// NOTE: The sealing log stream ignores the failure of sealing its replica.
			sm.logger.Warn("could not seal replica", zap.Int32("snid", int32(storageNodeID)), zap.Int32("lsid", int32(lsid)))
			continue
		}
		sm.logger.Debug("seal",
			zap.Int32("snid", int32(storageNodeID)),
			zap.Int32("tpid", int32(tpid)),
			zap.Int32("lsid", int32(lsid)),
			zap.Uint64("last_glsn", uint64(lastCommittedGLSN)),
			zap.String("status", status.String()),
			zap.Uint64("local_hwm", uint64(highWatermark)),
		)
		lsmetaDesc = append(lsmetaDesc, snpb.LogStreamReplicaMetadataDescriptor{
			LogStreamReplica: varlogpb.LogStreamReplica{
				StorageNode: varlogpb.StorageNode{
					StorageNodeID: storageNodeID,
				},
				TopicLogStream: varlogpb.TopicLogStream{
					TopicID:     tpid,
					LogStreamID: lsid,
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

func (sm *snManager) Sync(ctx context.Context, tpid types.TopicID, lsid types.LogStreamID, srcID, dstID types.StorageNodeID, lastGLSN types.GLSN) (*snpb.SyncStatus, error) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	replicas, err := sm.replicaDescriptors(ctx, lsid)
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
		sm.refresh(ctx)
		return nil, errors.Wrap(verrors.ErrNotExist, "storage node")
	}

	srcCli, err := sm.clients.Get(srcID)
	if err != nil {
		sm.refresh(ctx)
		return nil, errors.Wrap(verrors.ErrNotExist, "storage node")
	}
	dstCli, err := sm.clients.Get(dstID)
	if err != nil {
		sm.refresh(ctx)
		return nil, errors.Wrap(verrors.ErrNotExist, "storage node")
	}

	// TODO: check cluster meta if snids exist
	return srcCli.Sync(ctx, tpid, lsid, dstID, dstCli.Target().Address, lastGLSN)
}

func (sm *snManager) Unseal(ctx context.Context, tpid types.TopicID, lsid types.LogStreamID) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	rds, err := sm.replicaDescriptors(ctx, lsid)
	if err != nil {
		return err
	}

	replicas := make([]varlogpb.LogStreamReplica, 0, len(rds))
	for _, rd := range rds {
		mc, err := sm.clients.Get(rd.StorageNodeID)
		if err != nil {
			return err
		}
		addr := mc.Target().Address
		replicas = append(replicas, varlogpb.LogStreamReplica{
			StorageNode: varlogpb.StorageNode{
				StorageNodeID: rd.StorageNodeID,
				Address:       addr,
			},
			TopicLogStream: varlogpb.TopicLogStream{
				TopicID:     tpid,
				LogStreamID: lsid,
			},
		})
	}

	// TODO: use errgroup
	for _, replica := range rds {
		storageNodeID := replica.GetStorageNodeID()
		cli, err := sm.clients.Get(storageNodeID)
		if err != nil {
			sm.refresh(ctx)
			return errors.Wrap(verrors.ErrNotExist, "storage node")
		}
		if err := cli.Unseal(ctx, tpid, lsid, replicas); err != nil {
			return err
		}
	}
	return nil
}

func (sm *snManager) Trim(ctx context.Context, tpid types.TopicID, lastGLSN types.GLSN) ([]vmspb.TrimResult, error) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	clusmeta, err := sm.cmview.ClusterMetadata(ctx)
	if err != nil {
		return nil, err
	}
	td := clusmeta.GetTopic(tpid)
	if td == nil {
		return nil, errors.Errorf("trim: no such topic %d", tpid)
	}

	clients := make(map[types.StorageNodeID]client.StorageNodeManagementClient)
	for _, lsid := range td.LogStreams {
		rds, err := sm.replicaDescriptors(ctx, lsid)
		if err != nil {
			return nil, err
		}
		for _, rd := range rds {
			if _, ok := clients[rd.StorageNodeID]; ok {
				continue
			}

			cli, err := sm.clients.Get(rd.StorageNodeID)
			if err != nil {
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
			res, err := client.Trim(ctx, tpid, lastGLSN)
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

func (sm *snManager) replicaDescriptors(ctx context.Context, lsid types.LogStreamID) ([]*varlogpb.ReplicaDescriptor, error) {
	clusmeta, err := sm.cmview.ClusterMetadata(ctx)
	if err != nil {
		return nil, err
	}
	lsdesc, err := clusmeta.MustHaveLogStream(lsid)
	if err != nil {
		return nil, err
	}
	return lsdesc.GetReplicas(), nil
}
