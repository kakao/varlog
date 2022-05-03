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

type connection struct {
	rpcConn *rpc.Conn
	target  varlogpb.StorageNode
}

type Manager struct {
	managerConfig

	mu     sync.Mutex
	closed bool
	conns  map[types.StorageNodeID]*connection
}

func NewManager(ctx context.Context, metadata *varlogpb.MetadataDescriptor, opts ...ManagerOption) (*Manager, error) {
	cfg, err := newManagerConfig(opts)
	if err != nil {
		return nil, err
	}
	mgr := &Manager{managerConfig: cfg}
	mgr.conns = make(map[types.StorageNodeID]*connection, len(metadata.GetStorageNodes()))
	for _, snd := range metadata.GetStorageNodes() {
		_, err := mgr.getOrConnect(ctx, varlogpb.StorageNode{
			StorageNodeID: snd.StorageNodeID,
			Address:       snd.Address,
		})
		if err != nil {
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
	conn, err := mgr.getOrConnect(ctx, varlogpb.StorageNode{
		StorageNodeID: snid,
		Address:       addr,
	})
	if err != nil {
		return nil, err
	}
	return newClient(conn.rpcConn, conn.target), nil
}

func (mgr *Manager) Close() (err error) {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()
	if mgr.closed {
		return nil
	}
	mgr.closed = true
	for snid := range mgr.conns {
		err = multierr.Append(err, mgr.conns[snid].rpcConn.Close())
	}
	return err
}

func (mgr *Manager) getOrConnect(ctx context.Context, target varlogpb.StorageNode) (*connection, error) {
	conn, ok := mgr.conns[target.StorageNodeID]
	if ok {
		if conn.target.Address != target.Address {
			return nil, fmt.Errorf("logclient manager: unexpected target, cached=%s requested=%s", conn.target.String(), target.String())
		}
		return conn, nil
	}

	rpcConn, err := rpc.NewConn(ctx, target.Address, mgr.defaultGRPCDialOptions...)
	if err != nil {
		return nil, err
	}
	conn = &connection{
		rpcConn: rpcConn,
		target:  target,
	}
	mgr.conns[target.StorageNodeID] = conn
	return conn, nil
}
