package logclient

//go:generate mockgen -build_flags -mod=vendor -self_package github.com/kakao/varlog/pkg/logc -package logc -destination log_io_client_manager_mock.go . LogClientManager

import (
	"context"
	"io"
	"strconv"
	"sync"

	"github.com/pkg/errors"
	"go.uber.org/multierr"
	"go.uber.org/zap"
	"golang.org/x/sync/singleflight"
	"google.golang.org/grpc"

	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/pkg/verrors"
	"github.com/kakao/varlog/proto/varlogpb"
)

type LogClientManager interface {
	GetOrConnect(ctx context.Context, storageNodeID types.StorageNodeID, addr string) (LogIOClient, error)
	io.Closer
}

type logClientManager struct {
	mu     sync.RWMutex
	closed bool

	clients struct {
		m map[types.StorageNodeID]*logClientProxy
		x sync.RWMutex

		g singleflight.Group
		k func(types.StorageNodeID) string
	}

	grpcDialOptions []grpc.DialOption

	logger *zap.Logger
}

var _ LogClientManager = (*logClientManager)(nil)

func NewLogClientManager(ctx context.Context, metadata *varlogpb.MetadataDescriptor, grpcDialOptions []grpc.DialOption, logger *zap.Logger) (mgr *logClientManager, err error) {
	if logger == nil {
		logger = zap.NewNop()
	}
	logger = logger.Named("logclmanager")

	mgr = &logClientManager{grpcDialOptions: grpcDialOptions, logger: logger}
	mgr.clients.m = make(map[types.StorageNodeID]*logClientProxy, len(metadata.GetStorageNodes()))
	mgr.clients.k = func(storageNodeID types.StorageNodeID) string {
		return strconv.FormatUint(uint64(storageNodeID), 10)
	}

	for _, sndesc := range metadata.GetStorageNodes() {
		storageNodeID := sndesc.GetStorageNodeID()
		addr := sndesc.GetAddress()
		if _, err := mgr.GetOrConnect(ctx, storageNodeID, addr); err != nil {
			return nil, multierr.Append(err, mgr.Close())
		}
	}
	return mgr, err
}

func (mgr *logClientManager) Close() (err error) {
	mgr.mu.Lock()
	mgr.closed = true
	mgr.mu.Unlock()

	mgr.clients.x.RLock()
	m := make(map[types.StorageNodeID]*logClientProxy, len(mgr.clients.m))
	for storageNodeID, proxy := range mgr.clients.m {
		m[storageNodeID] = proxy
	}
	mgr.clients.x.RUnlock()

	for _, proxy := range m {
		err = multierr.Append(err, proxy.Close())
	}

	mgr.clients.x.RLock()
	if len(mgr.clients.m) > 0 {
		panic("cleanup error")
	}
	mgr.clients.x.RUnlock()

	return err
}

func (mgr *logClientManager) GetOrConnect(ctx context.Context, storageNodeID types.StorageNodeID, addr string) (LogIOClient, error) {
	mgr.mu.RLock()
	defer mgr.mu.RUnlock()
	if mgr.closed {
		return nil, errors.Wrap(verrors.ErrClosed, "logclmanager")
	}

	key := mgr.clients.k(storageNodeID)
	proxyI, err, _ := mgr.clients.g.Do(key, func() (interface{}, error) {
		var (
			proxy *logClientProxy
			ok    bool
		)
		mgr.clients.x.RLock()
		proxy, ok = mgr.clients.m[storageNodeID]
		mgr.clients.x.RUnlock()
		if ok {
			return proxy, nil
		}

		logcl, err := NewLogIOClient(ctx, addr, mgr.grpcDialOptions...)
		if err != nil {
			proxy = nil
			return proxy, errors.WithMessagef(err, "logclmanager: snid=%d", storageNodeID)
		}
		closer := func() error {
			mgr.clients.x.Lock()
			delete(mgr.clients.m, storageNodeID)
			mgr.clients.x.Unlock()
			return logcl.Close()
		}
		proxy = newLogIOProxy(logcl, closer)
		mgr.clients.x.Lock()
		mgr.clients.m[storageNodeID] = proxy
		mgr.clients.x.Unlock()
		return proxy, nil
	})
	if err != nil {
		return nil, err
	}
	return proxyI.(*logClientProxy), nil
}
