package client

import (
	"context"
	"fmt"
	"sync"

	"github.com/kakao/varlog/pkg/rpc"
	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/pkg/verrors"
	"github.com/kakao/varlog/proto/varlogpb"
)

// Kind is a generic type for clients managed by the Manager.
type Kind interface {
	*LogClient | *ManagementClient

	// Target returns target storage node.
	Target() varlogpb.StorageNode

	reset(rpcConn *rpc.Conn, cid types.ClusterID, target varlogpb.StorageNode) any
}

// Manager manages clients typed Kind.
type Manager[T Kind] struct {
	managerConfig

	conns *rpc.Manager[types.StorageNodeID]

	mu      sync.Mutex
	clients map[types.StorageNodeID]T
	closed  bool
}

// NewManager creates a Manager.
func NewManager[T Kind](opts ...ManagerOption) (*Manager[T], error) {
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

	return &Manager[T]{
		managerConfig: cfg,
		conns:         conns,
		clients:       make(map[types.StorageNodeID]T),
	}, nil
}

// Get returns the client identified by the argument snid.
// It returns an error if the client does not exist.
func (mgr *Manager[T]) Get(snid types.StorageNodeID) (T, error) {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()

	if mgr.closed {
		return nil, fmt.Errorf("client manager: %w", verrors.ErrClosed)
	}

	client, ok := mgr.clients[snid]
	if !ok {
		return nil, fmt.Errorf("client manager: client %d not exist", int32(snid))
	}
	return client, nil
}

// GetOrConnect returns the client identified by the argument snid.
// It tries to connect the server using the argument addr if the client does
// not exist.
// It returns an error if the existing client has an address other than the
// argument addr or trial of the connection fails.
func (mgr *Manager[T]) GetOrConnect(ctx context.Context, snid types.StorageNodeID, addr string) (T, error) {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()

	if mgr.closed {
		return nil, fmt.Errorf("client manager: %w", verrors.ErrClosed)
	}

	client, ok := mgr.clients[snid]
	if ok {
		if target := client.Target(); target.Address != addr {
			return nil, fmt.Errorf("client manager: unexpected target address for snid %d, cached=%s requested=%s", uint64(snid), target.Address, addr)
		}
		return client, nil
	}

	conn, err := mgr.conns.GetOrConnect(ctx, snid, addr)
	if err != nil {
		return nil, err
	}

	client = client.reset(conn, mgr.cid, varlogpb.StorageNode{
		StorageNodeID: snid,
		Address:       addr,
	}).(T)
	mgr.clients[snid] = client
	return client, nil
}

// CloseClient closes a client identified by the argument snid.
func (mgr *Manager[T]) CloseClient(snid types.StorageNodeID) error {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()

	delete(mgr.clients, snid)
	return mgr.conns.CloseClient(snid)
}

// Close closes all clients managed by the Manager.
func (mgr *Manager[T]) Close() (err error) {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()

	if mgr.closed {
		return nil
	}
	mgr.closed = true
	return mgr.conns.Close()
}
