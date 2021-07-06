package mrconnector

import (
	"context"
	"io"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/pkg/errors"
	"go.uber.org/multierr"
	"go.uber.org/zap"
	"golang.org/x/sync/singleflight"

	"github.daumkakao.com/varlog/varlog/pkg/mrc"
	"github.daumkakao.com/varlog/varlog/pkg/rpc"
	"github.daumkakao.com/varlog/varlog/pkg/types"
	"github.daumkakao.com/varlog/varlog/pkg/util/runner"
	"github.daumkakao.com/varlog/varlog/proto/mrpb"
)

// Connector represents a connection proxy for the metadata repository. It contains clients and
// management clients for the metadata repository.
type Connector interface {
	io.Closer

	ClusterID() types.ClusterID

	NumberOfMR() int

	ActiveMRs() map[types.NodeID]string

	ConnectedNodeID() types.NodeID

	Client(ctx context.Context) (mrc.MetadataRepositoryClient, error)

	ManagementClient(ctx context.Context) (mrc.MetadataRepositoryManagementClient, error)

	AddRPCAddr(nodeID types.NodeID, addr string)

	DelRPCAddr(nodeID types.NodeID)
}

type connectorImpl struct {
	config
	clusterInfo *mrpb.ClusterInfo
	proxy       *mrProxy
	group       singleflight.Group
	runner      *runner.Runner
	cancel      context.CancelFunc
}

func New(ctx context.Context, opts ...Option) (Connector, error) {
	cfg, err := newConfig(opts)
	if err != nil {
		return nil, err
	}
	cfg.logger = cfg.logger.Named("mrconnector")

	c := &connectorImpl{
		config: cfg,
		runner: runner.New("mrconnector", cfg.logger),
	}

	if err := c.initBySeed(ctx); err != nil {
		return nil, err
	}

	c.cancel, err = c.runner.Run(c.updateLoop)
	if err != nil {
		return nil, errors.Wrap(err, "mrconnector")
	}

	return c, nil
}

func (c *connectorImpl) initBySeed(ctx context.Context) (err error) {
	for i := 0; i < c.initCount; i++ {
		err = c.updateClusterInfoFromSeed(ctx)
		if err == nil {
			_, err = c.connect(ctx)
			if err == nil {
				return nil
			}
		}
		time.Sleep(c.initRetryInterval)
	}
	return err
}

func (c *connectorImpl) Close() (err error) {
	c.cancel()
	c.runner.Stop()
	if proxy, err := c.loadProxy(); err == nil {
		return proxy.Close()
	}
	return nil
}

func (c *connectorImpl) ClusterID() types.ClusterID {
	return c.clusterID
}

func (c *connectorImpl) NumberOfMR() int {
	return len(c.ActiveMRs())
}

func (c *connectorImpl) ActiveMRs() map[types.NodeID]string {
	clusterInfo := c.loadClusterInfo()
	if clusterInfo == nil {
		return nil
	}
	members := clusterInfo.GetMembers()
	if len(members) == 0 {
		return nil
	}
	ret := make(map[types.NodeID]string, len(members))
	clusterInfo.ForEachMember(func(nodeID types.NodeID, member *mrpb.ClusterInfo_Member) bool {
		endpoint := member.GetEndpoint()
		if nodeID == types.InvalidNodeID ||
			len(endpoint) == 0 ||
			len(member.GetPeer()) == 0 ||
			member.GetLearner() {
			return true
		}
		ret[nodeID] = endpoint
		return true
	})
	return ret
}

func (c *connectorImpl) Client(ctx context.Context) (mrc.MetadataRepositoryClient, error) {
	return c.connect(ctx)
}

func (c *connectorImpl) ManagementClient(ctx context.Context) (mrc.MetadataRepositoryManagementClient, error) {
	return c.connect(ctx)
}

func (c *connectorImpl) ConnectedNodeID() types.NodeID {
	proxy, err := c.loadProxy()
	if err != nil {
		return types.InvalidNodeID
	}
	return proxy.nodeID
}

func (c *connectorImpl) updateLoop(ctx context.Context) {
	timer := time.NewTimer(c.updateInterval)
	defer timer.Stop()

	for ctx.Err() == nil {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			if err := c.update(ctx); err != nil {
				c.logger.Warn("could not update", zap.Error(err))
			}
			timer.Reset(c.updateInterval)
		}
	}
}

func (c *connectorImpl) AddRPCAddr(nodeID types.NodeID, addr string) {
}

func (c *connectorImpl) DelRPCAddr(nodeID types.NodeID) {
}

func (c *connectorImpl) update(ctx context.Context) error {
	mcl, err := c.ManagementClient(ctx)
	if err != nil {
		return err
	}

	rsp, err := mcl.GetClusterInfo(ctx, c.clusterID)
	if err != nil {
		_ = mcl.Close()
		return err
	}

	c.updateClusterInfo(rsp.GetClusterInfo())
	return nil
}

func (c *connectorImpl) updateClusterInfo(clusterInfo *mrpb.ClusterInfo) bool {
	prev := c.loadClusterInfo()
	for prev == nil || clusterInfo.NewerThan(prev) {
		if c.casClusterInfo(prev, clusterInfo) {
			return true
		}
		prev = c.loadClusterInfo()
	}
	return false
}

func (c *connectorImpl) connect(ctx context.Context) (*mrProxy, error) {
	proxy, err, _ := c.group.Do("connect", func() (interface{}, error) {
		if proxy, err := c.loadProxy(); err == nil {
			return proxy, nil
		}

		var (
			proxy *mrProxy
			err   error
		)
		clusterInfo := c.loadClusterInfo()
		if len(clusterInfo.GetMembers()) == 0 {
			return proxy, errors.New("no accessible MR")
		}
		clusterInfo.ForEachMember(func(nodeID types.NodeID, member *mrpb.ClusterInfo_Member) bool {
			endpoint := member.GetEndpoint()
			if len(endpoint) == 0 {
				return true
			}

			// connect to endpoint
			cl, mcl, cerr := c.connectToMR(ctx, endpoint)
			if cerr != nil {
				err = multierr.Append(err, cerr)
				return true
			}

			// check the MR whether it is okay or not
			if _, cerr = c.getClusterInfo(ctx, mcl); cerr != nil {
				err = multierr.Combine(cerr, cl.Close(), mcl.Close())
				return true
			}

			// set proxy if succeed
			proxy = newMRProxy(c, nodeID, cl, mcl)
			err = nil
			return false
		})
		if err == nil && proxy == nil {
			// NOTE: All endpoints in clusterInfo are empty.
			err = errors.New("no accessible MR")
			return proxy, err
		}
		if err != nil {
			_ = c.updateClusterInfoFromSeed(ctx)
		} else {
			if !c.casProxy(nil, proxy) {
				_ = proxy.Close()
				proxy, err = c.loadProxy()
			}
		}
		return proxy, err
	})
	return proxy.(*mrProxy), err
}

func (c *connectorImpl) connectToMR(ctx context.Context, addr string) (cl mrc.MetadataRepositoryClient, mcl mrc.MetadataRepositoryManagementClient, err error) {
	connCtx, cancel := context.WithTimeout(ctx, c.connTimeout)
	defer cancel()
	conn, err := rpc.NewConn(connCtx, addr)
	if err != nil {
		return nil, nil, err
	}

	// It always returns nil as error value.
	cl, _ = mrc.NewMetadataRepositoryClientFromRpcConn(conn)
	mcl, _ = mrc.NewMetadataRepositoryManagementClientFromRpcConn(conn)
	return cl, mcl, nil
}

func (c *connectorImpl) getClusterInfo(ctx context.Context, mcl mrc.MetadataRepositoryManagementClient) (*mrpb.ClusterInfo, error) {
	rpcCtx, cancel := context.WithTimeout(ctx, c.rpcTimeout)
	defer cancel()
	rsp, err := mcl.GetClusterInfo(rpcCtx, c.clusterID)
	if err != nil {
		return nil, err
	}

	info := rsp.GetClusterInfo()
	// NOTE: This guarantees that the connected node is not split-brained.
	if info.GetLeader() == types.InvalidNodeID {
		return nil, errors.New("maybe split-brained node")
	}

	found := false
	for _, member := range info.GetMembers() {
		if len(member.GetEndpoint()) > 0 {
			found = true
		}
	}
	if !found {
		return nil, errors.New("no accessible MRs")
	}

	return info, err
}

func (c *connectorImpl) updateClusterInfoFromSeed(ctx context.Context) error {
	seedClusterInfo, err := c.getClusterInfoFromSeed(ctx)
	if err != nil {
		return err
	}
	_ = c.updateClusterInfo(seedClusterInfo)
	/*
		if !c.updateClusterInfo(seedClusterInfo) {
			return errors.Errorf("no latest clusterInfo from seed, loaded=%+v, seed=%+v", c.loadClusterInfo(), seedClusterInfo)
		}
	*/
	return nil
}

func (c *connectorImpl) getClusterInfoFromSeed(ctx context.Context) (*mrpb.ClusterInfo, error) {
	var (
		wg         sync.WaitGroup
		mu         sync.Mutex
		latestInfo *mrpb.ClusterInfo
		ok         bool
		err        error
	)
	for idx := range c.seed {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			var (
				cerr error
				info *mrpb.ClusterInfo
			)
			addr := c.seed[idx]

			defer func() {
				mu.Lock()
				err = multierr.Append(err, cerr)
				if cerr == nil {
					ok = true
					if latestInfo == nil || info.NewerThan(latestInfo) {
						latestInfo = info
					}
				}
				mu.Unlock()
			}()

			cl, mcl, cerr := c.connectToMR(ctx, addr)
			if cerr != nil {
				return
			}

			info, cerr = c.getClusterInfo(ctx, mcl)
			cerr = multierr.Combine(cerr, cl.Close(), mcl.Close())
		}(idx)
	}
	wg.Wait()

	if ok {
		return latestInfo, nil
	}
	return nil, err
}

func (c *connectorImpl) loadClusterInfo() *mrpb.ClusterInfo {
	addr := (*unsafe.Pointer)(unsafe.Pointer(&c.clusterInfo))
	return (*mrpb.ClusterInfo)(atomic.LoadPointer(addr))
}

func (c *connectorImpl) casClusterInfo(oldClusterInfo, newClusterInfo *mrpb.ClusterInfo) bool {
	addr := (*unsafe.Pointer)(unsafe.Pointer(&c.clusterInfo))
	return atomic.CompareAndSwapPointer(addr, unsafe.Pointer(oldClusterInfo), unsafe.Pointer(newClusterInfo))
}

func (c *connectorImpl) loadProxy() (*mrProxy, error) {
	ptr := (*unsafe.Pointer)(unsafe.Pointer(&c.proxy))
	proxy := (*mrProxy)(atomic.LoadPointer(ptr))
	if proxy == nil {
		return nil, errors.New("no proxy")
	}
	return proxy, nil
}

func (c *connectorImpl) casProxy(oldProxy, newProxy *mrProxy) bool {
	ptr := (*unsafe.Pointer)(unsafe.Pointer(&c.proxy))
	return atomic.CompareAndSwapPointer(ptr, unsafe.Pointer(oldProxy), unsafe.Pointer(newProxy))
}
