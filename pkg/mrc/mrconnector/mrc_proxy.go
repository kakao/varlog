package mrconnector

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"

	"go.uber.org/multierr"

	"github.daumkakao.com/varlog/varlog/pkg/mrc"
	"github.daumkakao.com/varlog/varlog/pkg/types"
	"github.daumkakao.com/varlog/varlog/proto/mrpb"
	"github.daumkakao.com/varlog/varlog/proto/varlogpb"
)

type mrProxy struct {
	conn   *connectorImpl
	nodeID types.NodeID
	cl     mrc.MetadataRepositoryClient
	mcl    mrc.MetadataRepositoryManagementClient

	inflight int64
	mu       sync.RWMutex
	cond     *sync.Cond
}

var _ mrc.MetadataRepositoryClient = (*mrProxy)(nil)
var _ mrc.MetadataRepositoryManagementClient = (*mrProxy)(nil)
var _ fmt.Stringer = (*mrProxy)(nil)

func newMRProxy(connector *connectorImpl, nodeID types.NodeID, cl mrc.MetadataRepositoryClient, mcl mrc.MetadataRepositoryManagementClient) *mrProxy {
	ret := &mrProxy{
		conn:   connector,
		nodeID: nodeID,
		cl:     cl,
		mcl:    mcl,
	}
	ret.cond = sync.NewCond(&ret.mu)
	return ret
}

func (m *mrProxy) Close() error {
	_ = m.conn.casProxy(m, nil)

	m.cond.L.Lock()
	defer m.cond.L.Unlock()
	for atomic.LoadInt64(&m.inflight) > 0 {
		m.cond.Wait()
	}
	return multierr.Append(m.cl.Close(), m.mcl.Close())
}

func (m *mrProxy) RegisterStorageNode(ctx context.Context, descriptor *varlogpb.StorageNodeDescriptor) error {
	m.mu.RLock()
	defer func() {
		atomic.AddInt64(&m.inflight, -1)
		m.mu.RUnlock()
		m.cond.Signal()
	}()
	atomic.AddInt64(&m.inflight, 1)

	return m.cl.RegisterStorageNode(ctx, descriptor)
}

func (m *mrProxy) UnregisterStorageNode(ctx context.Context, id types.StorageNodeID) error {
	m.mu.RLock()
	defer func() {
		atomic.AddInt64(&m.inflight, -1)
		m.mu.RUnlock()
		m.cond.Signal()
	}()
	atomic.AddInt64(&m.inflight, 1)

	return m.cl.UnregisterStorageNode(ctx, id)
}

func (m *mrProxy) RegisterTopic(ctx context.Context, id types.TopicID) error {
	m.mu.RLock()
	defer func() {
		atomic.AddInt64(&m.inflight, -1)
		m.mu.RUnlock()
		m.cond.Signal()
	}()
	atomic.AddInt64(&m.inflight, 1)

	return m.cl.RegisterTopic(ctx, id)
}

func (m *mrProxy) UnregisterTopic(ctx context.Context, id types.TopicID) error {
	m.mu.RLock()
	defer func() {
		atomic.AddInt64(&m.inflight, -1)
		m.mu.RUnlock()
		m.cond.Signal()
	}()
	atomic.AddInt64(&m.inflight, 1)

	return m.cl.UnregisterTopic(ctx, id)
}

func (m *mrProxy) RegisterLogStream(ctx context.Context, descriptor *varlogpb.LogStreamDescriptor) error {
	m.mu.RLock()
	defer func() {
		atomic.AddInt64(&m.inflight, -1)
		m.mu.RUnlock()
		m.cond.Signal()
	}()
	atomic.AddInt64(&m.inflight, 1)

	return m.cl.RegisterLogStream(ctx, descriptor)
}

func (m *mrProxy) UnregisterLogStream(ctx context.Context, id types.LogStreamID) error {
	m.mu.RLock()
	defer func() {
		atomic.AddInt64(&m.inflight, -1)
		m.mu.RUnlock()
		m.cond.Signal()
	}()
	atomic.AddInt64(&m.inflight, 1)

	return m.cl.UnregisterLogStream(ctx, id)
}

func (m *mrProxy) UpdateLogStream(ctx context.Context, descriptor *varlogpb.LogStreamDescriptor) error {
	m.mu.RLock()
	defer func() {
		atomic.AddInt64(&m.inflight, -1)
		m.mu.RUnlock()
		m.cond.Signal()
	}()
	atomic.AddInt64(&m.inflight, 1)

	return m.cl.UpdateLogStream(ctx, descriptor)
}

func (m *mrProxy) GetMetadata(ctx context.Context) (*varlogpb.MetadataDescriptor, error) {
	m.mu.RLock()
	defer func() {
		atomic.AddInt64(&m.inflight, -1)
		m.mu.RUnlock()
		m.cond.Signal()
	}()
	atomic.AddInt64(&m.inflight, 1)

	return m.cl.GetMetadata(ctx)
}

func (m *mrProxy) Seal(ctx context.Context, id types.LogStreamID) (types.GLSN, error) {
	m.mu.RLock()
	defer func() {
		atomic.AddInt64(&m.inflight, -1)
		m.mu.RUnlock()
		m.cond.Signal()
	}()
	atomic.AddInt64(&m.inflight, 1)

	return m.cl.Seal(ctx, id)
}

func (m *mrProxy) Unseal(ctx context.Context, id types.LogStreamID) error {
	m.mu.RLock()
	defer func() {
		atomic.AddInt64(&m.inflight, -1)
		m.mu.RUnlock()
		m.cond.Signal()
	}()
	atomic.AddInt64(&m.inflight, 1)

	return m.cl.Unseal(ctx, id)
}

func (m *mrProxy) AddPeer(ctx context.Context, clusterID types.ClusterID, nodeID types.NodeID, url string) error {
	m.mu.RLock()
	defer func() {
		atomic.AddInt64(&m.inflight, -1)
		m.mu.RUnlock()
		m.cond.Signal()
	}()
	atomic.AddInt64(&m.inflight, 1)

	return m.mcl.AddPeer(ctx, clusterID, nodeID, url)
}

func (m *mrProxy) RemovePeer(ctx context.Context, clusterID types.ClusterID, nodeID types.NodeID) error {
	m.mu.RLock()
	defer func() {
		atomic.AddInt64(&m.inflight, -1)
		m.mu.RUnlock()
		m.cond.Signal()
	}()
	atomic.AddInt64(&m.inflight, 1)

	return m.mcl.RemovePeer(ctx, clusterID, nodeID)
}

func (m *mrProxy) GetClusterInfo(ctx context.Context, clusterID types.ClusterID) (*mrpb.GetClusterInfoResponse, error) {
	m.mu.RLock()
	defer func() {
		atomic.AddInt64(&m.inflight, -1)
		m.mu.RUnlock()
		m.cond.Signal()
	}()
	atomic.AddInt64(&m.inflight, 1)

	return m.mcl.GetClusterInfo(ctx, clusterID)
}

func (m *mrProxy) String() string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "proxy{nodeID:%d inflight:%d}", m.nodeID, atomic.LoadInt64(&m.inflight))
	return sb.String()
}
