package logclient

import (
	"context"
	"fmt"
	"sync"

	"go.uber.org/multierr"

	"github.com/kakao/varlog/pkg/rpc"
	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/pkg/verrors"
	"github.com/kakao/varlog/proto/varlogpb"
)

type Manager struct {
	managerConfig

	conns *rpc.Manager[types.StorageNodeID]

	mu      sync.Mutex
	clients map[types.StorageNodeID]*Client
	closed  bool
}

func NewManager(ctx context.Context, metadata *varlogpb.MetadataDescriptor, opts ...ManagerOption) (*Manager, error) {
	cfg, err := newManagerConfig(opts)
	if err != nil {
		return nil, err
	}

	conns, err := rpc.NewManager[types.StorageNodeID](
		rpc.WithDefaultGRPCDialOptions(cfg.defaultGRPCDialOptions...),
		rpc.WithLogger(cfg.logger),
	)
	if err != nil {
		return nil, err
	}

	mgr := &Manager{
		managerConfig: cfg,
		conns:         conns,
		clients:       make(map[types.StorageNodeID]*Client, len(metadata.GetStorageNodes())),
	}

	for _, snd := range metadata.GetStorageNodes() {
		if _, err := mgr.GetOrConnect(ctx, snd.StorageNodeID, snd.Address); err != nil {
			return nil, multierr.Append(err, mgr.Close())
		}
	}
	return mgr, nil
}

func (mgr *Manager) GetOrConnect(ctx context.Context, snid types.StorageNodeID, addr string) (*Client, error) {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()
	if mgr.closed {
		return nil, fmt.Errorf("logclmanager: %w", verrors.ErrClosed)
	}

	client, ok := mgr.clients[snid]
	if ok {
		if client.target.Address != addr {
			return nil, fmt.Errorf("logclient manager: unexpected target address for snid %d, cached=%s requested=%s", uint64(snid), client.target.Address, addr)
		}
		return client, nil
	}

	conn, err := mgr.conns.GetOrConnect(ctx, snid, addr)
	if err != nil {
		return nil, err
	}

	client = newClient(conn, varlogpb.StorageNode{
		StorageNodeID: snid,
		Address:       addr,
	})
	mgr.clients[snid] = client
	return client, nil
}

func (mgr *Manager) Close() (err error) {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()
	if mgr.closed {
		return nil
	}
	mgr.closed = true
	return mgr.conns.Close()
}
