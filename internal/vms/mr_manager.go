package vms

import (
	"context"
	"errors"
	"io"
	"math"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.daumkakao.com/varlog/varlog/pkg/mrc"
	"github.daumkakao.com/varlog/varlog/pkg/mrc/mrconnector"
	"github.daumkakao.com/varlog/varlog/pkg/types"
	"github.daumkakao.com/varlog/varlog/pkg/verrors"
	"github.daumkakao.com/varlog/varlog/proto/mrpb"
	"github.daumkakao.com/varlog/varlog/proto/varlogpb"
)

// ClusterMetadataView provides the latest metadata about the cluster.
// TODO: It should have a way to guarantee that ClusterMetadata is the latest.
// TODO: See https://github.daumkakao.com/varlog/varlog/pull/198#discussion_r215542
type ClusterMetadataView interface {
	// ClusterMetadata returns the latest metadata of the cluster.
	ClusterMetadata(ctx context.Context) (*varlogpb.MetadataDescriptor, error)

	// StorageNode returns the storage node corresponded with the storageNodeID.
	StorageNode(ctx context.Context, storageNodeID types.StorageNodeID) (*varlogpb.StorageNodeDescriptor, error)

	// LogStreamReplicas returns all of the latest LogStreamReplicaMetas for the given
	// logStreamID. The first element of the returned LogStreamReplicaMeta list is the primary
	// LogStreamReplica.
	// LogStreamReplicas(ctx context.Context, logStreamID types.LogStreamID) ([]*vpb.LogStreamMetadataDescriptor, error)
}

type ClusterMetadataViewGetter interface {
	ClusterMetadataView() ClusterMetadataView
}

var (
	errCMVNoStorageNode = errors.New("cmview: no such storage node")
)

const (
	RELOAD_INTERVAL = time.Second
)

type MetadataRepositoryManager interface {
	ClusterMetadataViewGetter
	io.Closer

	RegisterStorageNode(ctx context.Context, storageNodeMeta *varlogpb.StorageNodeDescriptor) error

	UnregisterStorageNode(ctx context.Context, storageNodeID types.StorageNodeID) error

	RegisterLogStream(ctx context.Context, logStreamDesc *varlogpb.LogStreamDescriptor) error

	UnregisterLogStream(ctx context.Context, logStreamID types.LogStreamID) error

	UpdateLogStream(ctx context.Context, logStreamDesc *varlogpb.LogStreamDescriptor) error

	// Seal seals logstream corresponded with the logStreamID. It marks the logstream in the
	// cluster metadata stored in MR  as sealed. It returns the last committed GLSN that is
	// confirmed by MR.
	Seal(ctx context.Context, logStreamID types.LogStreamID) (lastCommittedGLSN types.GLSN, err error)

	Unseal(ctx context.Context, logStreamID types.LogStreamID) error

	GetClusterInfo(ctx context.Context) (*mrpb.ClusterInfo, error)

	AddPeer(ctx context.Context, nodeID types.NodeID, peerURL, rpcURL string) error

	RemovePeer(ctx context.Context, nodeID types.NodeID) error

	NumberOfMR() int
}

var (
	_ MetadataRepositoryManager = (*mrManager)(nil)
	_ ClusterMetadataView       = (*mrManager)(nil)
	_ ClusterMetadataViewGetter = (*mrManager)(nil)
)

type mrManager struct {
	clusterID types.ClusterID

	mu        sync.RWMutex
	connector mrconnector.Connector

	dirty   bool
	updated time.Time
	meta    *varlogpb.MetadataDescriptor

	logger *zap.Logger
}

const (
	// TODO (jun): Fix code styles (See https://golang.org/doc/effective_go.html#mixed-caps)
	MRMANAGER_INIT_TIMEOUT     = 5 * time.Second
	RPCAddrsFetchRetryInterval = 100 * time.Millisecond
)

func NewMRManager(ctx context.Context, clusterID types.ClusterID, mrOpts MRManagerOptions, logger *zap.Logger) (MetadataRepositoryManager, error) {
	if logger == nil {
		logger = zap.NewNop()
	}
	logger = logger.Named("mrmanager")

	if len(mrOpts.MetadataRepositoryAddresses) == 0 {
		return nil, verrors.ErrInvalid
	}

	opts := []mrconnector.Option{
		mrconnector.WithClusterID(clusterID),
		mrconnector.WithConnectionTimeout(MRMANAGER_INIT_TIMEOUT),
		mrconnector.WithRPCAddrsFetchRetryInterval(RPCAddrsFetchRetryInterval),
		mrconnector.WithLogger(logger),
	}
	tryCnt := mrOpts.InitialMRConnRetryCount + 1
	if tryCnt <= 0 {
		tryCnt = math.MaxInt32
	}

	var err error
	for i := 0; i < tryCnt; i++ {
		connector, e := mrconnector.New(ctx, mrOpts.MetadataRepositoryAddresses, opts...)
		if e == nil {
			return &mrManager{
				clusterID: clusterID,
				dirty:     true,
				connector: connector,
				logger:    logger,
			}, nil
		}
		err = e
		time.Sleep(mrOpts.InitialMRConnRetryBackoff)
	}
	return nil, err
}

func (mrm *mrManager) c() (mrc.MetadataRepositoryClient, error) {
	return mrm.connector.Client()
}

func (mrm *mrManager) mc() (mrc.MetadataRepositoryManagementClient, error) {
	return mrm.connector.ManagementClient()
}

func (mrm *mrManager) Close() error {
	mrm.mu.Lock()
	defer mrm.mu.Unlock()

	return mrm.connector.Close()
}

func (mrm *mrManager) ClusterMetadataView() ClusterMetadataView {
	return mrm
}

func (mrm *mrManager) closeClient(cl mrc.MetadataRepositoryClient) {
	err := cl.Close()
	if err != nil {
		mrm.logger.Warn("error while closing mr client", zap.Error(err))
	}
}

func (mrm *mrManager) closeManagementClient(mcl mrc.MetadataRepositoryManagementClient) {
	err := mcl.Close()
	if err != nil {
		mrm.logger.Warn("error while closing mr client", zap.Error(err))
	}
}

func (mrm *mrManager) clusterMetadata(ctx context.Context) (*varlogpb.MetadataDescriptor, error) {
	cli, err := mrm.c()
	if err != nil {
		return nil, verrors.ErrNotAccessible
	}

	meta, err := cli.GetMetadata(ctx)
	if err != nil {
		mrm.closeClient(cli)
	}

	return meta, err
}

func (mrm *mrManager) RegisterStorageNode(ctx context.Context, storageNodeMeta *varlogpb.StorageNodeDescriptor) error {
	mrm.mu.Lock()
	defer func() {
		mrm.dirty = true
		mrm.mu.Unlock()
	}()

	cli, err := mrm.c()
	if err != nil {
		return verrors.ErrNotAccessible
	}

	if err := cli.RegisterStorageNode(ctx, storageNodeMeta); err != nil {
		mrm.closeClient(cli)
	}

	return err
}

func (mrm *mrManager) UnregisterStorageNode(ctx context.Context, storageNodeID types.StorageNodeID) error {
	mrm.mu.Lock()
	defer func() {
		mrm.dirty = true
		mrm.mu.Unlock()
	}()

	cli, err := mrm.c()
	if err != nil {
		return verrors.ErrNotAccessible
	}

	if err := cli.UnregisterStorageNode(ctx, storageNodeID); err != nil {
		mrm.closeClient(cli)
	}

	return err
}

func (mrm *mrManager) RegisterLogStream(ctx context.Context, logStreamDesc *varlogpb.LogStreamDescriptor) error {
	mrm.mu.Lock()
	defer func() {
		mrm.dirty = true
		mrm.mu.Unlock()
	}()

	cli, err := mrm.c()
	if err != nil {
		return verrors.ErrNotAccessible
	}

	if err := cli.RegisterLogStream(ctx, logStreamDesc); err != nil {
		mrm.closeClient(cli)
	}

	return err
}

func (mrm *mrManager) UnregisterLogStream(ctx context.Context, logStreamID types.LogStreamID) error {
	mrm.mu.Lock()
	defer func() {
		mrm.dirty = true
		mrm.mu.Unlock()
	}()

	cli, err := mrm.c()
	if err != nil {
		return verrors.ErrNotAccessible
	}

	if err := cli.UnregisterLogStream(ctx, logStreamID); err != nil {
		mrm.closeClient(cli)
	}
	return err
}

func (mrm *mrManager) UpdateLogStream(ctx context.Context, logStreamDesc *varlogpb.LogStreamDescriptor) error {
	mrm.mu.Lock()
	defer func() {
		mrm.dirty = true
		mrm.mu.Unlock()
	}()

	cli, err := mrm.c()
	if err != nil {
		return verrors.ErrNotAccessible
	}

	if err := cli.UpdateLogStream(ctx, logStreamDesc); err != nil {
		mrm.closeClient(cli)
	}
	return err
}

// It implements MetadataRepositoryManager.Seal method.
func (mrm *mrManager) Seal(ctx context.Context, logStreamID types.LogStreamID) (lastCommittedGLSN types.GLSN, err error) {
	mrm.mu.Lock()
	defer func() {
		mrm.dirty = true
		mrm.mu.Unlock()
	}()

	cli, err := mrm.c()
	if err != nil {
		return types.InvalidGLSN, verrors.ErrNotAccessible
	}

	if lastCommittedGLSN, err = cli.Seal(ctx, logStreamID); err != nil {
		mrm.closeClient(cli)
	}
	return lastCommittedGLSN, err
}

func (mrm *mrManager) Unseal(ctx context.Context, logStreamID types.LogStreamID) error {
	mrm.mu.Lock()
	defer func() {
		mrm.dirty = true
		mrm.mu.Unlock()
	}()

	cli, err := mrm.c()
	if err != nil {
		return verrors.ErrNotAccessible
	}

	if err := cli.Unseal(ctx, logStreamID); err != nil {
		mrm.closeClient(cli)
	}
	return err
}

func (mrm *mrManager) GetClusterInfo(ctx context.Context) (*mrpb.ClusterInfo, error) {
	mrm.mu.Lock()
	defer mrm.mu.Unlock()

	cli, err := mrm.mc()
	if cli == nil {
		return nil, verrors.ErrNotAccessible
	}

	rsp, err := cli.GetClusterInfo(ctx, mrm.clusterID)
	if err != nil {
		mrm.closeManagementClient(cli)
		return nil, err
	}
	return rsp.GetClusterInfo(), err
}

func (mrm *mrManager) AddPeer(ctx context.Context, nodeID types.NodeID, peerURL, rpcURL string) error {
	mrm.mu.Lock()
	defer mrm.mu.Unlock()

	cli, err := mrm.mc()
	if err != nil {
		return verrors.ErrNotAccessible
	}

	if err := cli.AddPeer(ctx, mrm.clusterID, nodeID, peerURL); err != nil {
		/*
			if !errors.Is(verrors.FromStatusError(context.TODO(), err),
				verrors.FromStatusError(context.TODO(), verrors.ErrAlreadyExists)) {
		*/
		if !errors.Is(err, verrors.ErrAlreadyExists) {
			mrm.closeManagementClient(cli)
		}
		return err
	}

	mrm.connector.AddRPCAddr(nodeID, rpcURL)
	return nil
}

func (mrm *mrManager) RemovePeer(ctx context.Context, nodeID types.NodeID) error {
	mrm.mu.Lock()
	defer mrm.mu.Unlock()

	cli, err := mrm.mc()
	if err != nil {
		return verrors.ErrNotAccessible
	}

	if err := cli.RemovePeer(ctx, mrm.clusterID, nodeID); err != nil {
		mrm.closeManagementClient(cli)
		return err
	}
	mrm.connector.DelRPCAddr(nodeID)

	return nil
}

func (mrm *mrManager) ClusterMetadata(ctx context.Context) (*varlogpb.MetadataDescriptor, error) {
	mrm.mu.Lock()
	defer mrm.mu.Unlock()

	if mrm.dirty || time.Now().Sub(mrm.updated) > RELOAD_INTERVAL {
		meta, err := mrm.clusterMetadata(ctx)
		if err != nil {
			return nil, err
		}
		mrm.meta = meta
		mrm.dirty = false
		mrm.updated = time.Now()
	}
	return mrm.meta, nil
}

func (mrm *mrManager) StorageNode(ctx context.Context, storageNodeID types.StorageNodeID) (*varlogpb.StorageNodeDescriptor, error) {
	meta, err := mrm.ClusterMetadata(ctx)
	if err != nil {
		return nil, err
	}
	if sndesc := meta.GetStorageNode(storageNodeID); sndesc != nil {
		return sndesc, nil
	}
	return nil, errCMVNoStorageNode
}

func (mrm *mrManager) NumberOfMR() int {
	return mrm.connector.NumberOfMR()
}
