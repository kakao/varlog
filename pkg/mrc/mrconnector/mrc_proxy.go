package mrconnector

import (
	"context"
	"sync"

	"github.daumkakao.com/varlog/varlog/pkg/mrc"
	"github.daumkakao.com/varlog/varlog/pkg/types"
	"github.daumkakao.com/varlog/varlog/pkg/util/syncutil/atomicutil"
	"github.daumkakao.com/varlog/varlog/proto/mrpb"
	"github.daumkakao.com/varlog/varlog/proto/varlogpb"
)

type mrProxy struct {
	cl           mrc.MetadataRepositoryClient
	mcl          mrc.MetadataRepositoryManagementClient
	nodeID       types.NodeID
	disconnected atomicutil.AtomicBool
	c            *connector
	once         sync.Once
	// TODO: Use singleflight in case of getter rpc
}

var _ mrc.MetadataRepositoryClient = (*mrProxy)(nil)
var _ mrc.MetadataRepositoryManagementClient = (*mrProxy)(nil)

func (m *mrProxy) Close() (err error) {
	m.once.Do(func() {
		m.disconnected.Store(true)
		m.c.releaseMRProxy(m.nodeID)
		if m.cl != nil {
			if e := m.cl.Close(); e != nil {
				err = e
			}
		}
		if m.mcl != nil {
			if e := m.mcl.Close(); e != nil {
				err = e
			}
		}
	})
	return err
}

func (m *mrProxy) RegisterStorageNode(ctx context.Context, descriptor *varlogpb.StorageNodeDescriptor) error {
	if m.cl == nil {
		panic("nil client")
	}
	return m.cl.RegisterStorageNode(ctx, descriptor)
}

func (m *mrProxy) UnregisterStorageNode(ctx context.Context, id types.StorageNodeID) error {
	if m.cl == nil {
		panic("nil client")
	}
	return m.cl.UnregisterStorageNode(ctx, id)
}

func (m *mrProxy) RegisterLogStream(ctx context.Context, descriptor *varlogpb.LogStreamDescriptor) error {
	if m.cl == nil {
		panic("nil client")
	}
	return m.cl.RegisterLogStream(ctx, descriptor)
}

func (m *mrProxy) UnregisterLogStream(ctx context.Context, id types.LogStreamID) error {
	if m.cl == nil {
		panic("nil client")
	}
	return m.cl.UnregisterLogStream(ctx, id)
}

func (m *mrProxy) UpdateLogStream(ctx context.Context, descriptor *varlogpb.LogStreamDescriptor) error {
	if m.cl == nil {
		panic("nil client")
	}
	return m.cl.UpdateLogStream(ctx, descriptor)
}

func (m *mrProxy) GetMetadata(ctx context.Context) (*varlogpb.MetadataDescriptor, error) {
	if m.cl == nil {
		panic("nil client")
	}
	return m.cl.GetMetadata(ctx)
}

func (m *mrProxy) Seal(ctx context.Context, id types.LogStreamID) (types.GLSN, error) {
	if m.cl == nil {
		panic("nil client")
	}
	return m.cl.Seal(ctx, id)
}

func (m *mrProxy) Unseal(ctx context.Context, id types.LogStreamID) error {
	if m.cl == nil {
		panic("nil client")
	}
	return m.cl.Unseal(ctx, id)
}

func (m *mrProxy) AddPeer(ctx context.Context, clusterID types.ClusterID, nodeID types.NodeID, url string) error {
	if m.mcl == nil {
		panic("nil client")
	}
	return m.mcl.AddPeer(ctx, clusterID, nodeID, url)
}

func (m *mrProxy) RemovePeer(ctx context.Context, clusterID types.ClusterID, nodeID types.NodeID) error {
	if m.mcl == nil {
		panic("nil client")
	}
	return m.mcl.RemovePeer(ctx, clusterID, nodeID)
}

func (m *mrProxy) GetClusterInfo(ctx context.Context, clusterID types.ClusterID) (*mrpb.GetClusterInfoResponse, error) {
	if m.mcl == nil {
		panic("nil client")
	}
	return m.mcl.GetClusterInfo(ctx, clusterID)
}
