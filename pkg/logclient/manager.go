package logclient

import (
	"context"
	"sync"

	"github.com/pkg/errors"
	"go.uber.org/multierr"
	"go.uber.org/zap"
	"google.golang.org/grpc"

	"github.daumkakao.com/varlog/varlog/pkg/types"
	"github.daumkakao.com/varlog/varlog/pkg/verrors"
	"github.daumkakao.com/varlog/varlog/proto/varlogpb"
)

type Manager struct {
	mu              sync.Mutex
	closed          bool
	clients         map[types.StorageNodeID]*logClientProxy
	grpcDialOptions []grpc.DialOption
	logger          *zap.Logger
}

func NewManager(ctx context.Context, metadata *varlogpb.MetadataDescriptor, grpcDialOptions []grpc.DialOption, logger *zap.Logger) (mgr *Manager, err error) {
	if logger == nil {
		logger = zap.NewNop()
	}
	logger = logger.Named("logclmanager")

	mgr = &Manager{grpcDialOptions: grpcDialOptions, logger: logger}
	mgr.clients = make(map[types.StorageNodeID]*logClientProxy, len(metadata.GetStorageNodes()))

	for _, sndesc := range metadata.GetStorageNodes() {
		storageNodeID := sndesc.GetStorageNodeID()
		addr := sndesc.GetAddress()
		if _, err := mgr.GetOrConnect(ctx, storageNodeID, addr); err != nil {
			return nil, multierr.Append(err, mgr.Close())
		}
	}
	return mgr, err
}

func (mgr *Manager) GetOrConnect(ctx context.Context, storageNodeID types.StorageNodeID, addr string) (LogIOClient, error) {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()

	if mgr.closed {
		return nil, errors.Wrap(verrors.ErrClosed, "logclmanager")
	}

	client, ok := mgr.clients[storageNodeID]
	if ok {
		return client, nil
	}

	logcl, err := NewLogIOClient(ctx, addr, mgr.grpcDialOptions...)
	if err != nil {
		return nil, errors.WithMessagef(err, "logclmanager: snid=%d", storageNodeID)
	}
	closer := func() error {
		mgr.mu.Lock()
		defer mgr.mu.Unlock()
		delete(mgr.clients, storageNodeID)
		return logcl.Close()
	}
	client = newLogIOProxy(logcl, closer)
	mgr.clients[storageNodeID] = client
	return client, nil
}

func (mgr *Manager) Close() (err error) {
	mgr.mu.Lock()

	if mgr.closed {
		mgr.mu.Unlock()
		return nil
	}
	mgr.closed = true

	clients := make(map[types.StorageNodeID]*logClientProxy, len(mgr.clients))
	for snid, client := range mgr.clients {
		clients[snid] = client
	}
	mgr.mu.Unlock()

	for _, client := range clients {
		err = multierr.Append(err, client.Close())
	}
	return err
}
