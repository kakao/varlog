package rpc

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"google.golang.org/grpc"
)

type connection[T comparable] struct {
	rpcConn *Conn
	id      T
	addr    string
}

// Manager keeps RPC connections for each target.
// Type parameter T should be a comparable type that identifies the target
// peer, for instance, types.StorageNodeID and types.NodeID.
// Manager can be shared by multiple clients.
type Manager[T comparable] struct {
	managerConfig

	mu     sync.Mutex
	conns  map[T]*connection[T]
	closed bool
}

// NewManager creates a Manager.
func NewManager[T comparable](opts ...ManagerOption) (*Manager[T], error) {
	cfg, err := newManagerConfig(opts)
	if err != nil {
		return nil, err
	}
	return &Manager[T]{
		managerConfig: cfg,
		conns:         make(map[T]*connection[T]),
	}, nil
}

// GetOrConnect returns an RPC connection that is cached or established if not
// exist.
func (m *Manager[T]) GetOrConnect(ctx context.Context, id T, addr string, grpcDialOptions ...grpc.DialOption) (*Conn, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	conn, ok := m.conns[id]
	if ok {
		if conn.addr != addr {
			return nil, fmt.Errorf("rpc: unexpected target %v address: cached=%s, requested=%s", id, conn.addr, addr)
		}
		return conn.rpcConn, nil
	}

	rpcConn, err := NewConn(ctx, addr, append(m.defaultGRPCDialOptions, grpcDialOptions...)...)
	if err != nil {
		return nil, err
	}

	conn = &connection[T]{
		rpcConn: rpcConn,
		id:      id,
		addr:    addr,
	}
	m.conns[id] = conn
	return rpcConn, nil
}

// CloseClient closes a connection identified by the argument id and removes the connection from the manager.
// It returns nil if the connection does not exist.
func (m *Manager[T]) CloseClient(id T) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	conn, ok := m.conns[id]
	if !ok {
		return nil
	}
	delete(m.conns, id)
	return conn.rpcConn.Close()
}

// Close closes all underlying connections managed by the Manager.
func (m *Manager[T]) Close() (err error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		return nil
	}
	m.closed = true

	for id := range m.conns {
		err = errors.Join(err, m.conns[id].rpcConn.Close())
	}
	m.conns = map[T]*connection[T]{}
	return err
}
