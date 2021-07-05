package mrconnector

import (
	"context"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	"go.uber.org/multierr"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/singleflight"

	"github.com/kakao/varlog/pkg/mrc"
	"github.com/kakao/varlog/pkg/rpc"
	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/pkg/util/runner"
	"github.com/kakao/varlog/proto/mrpb"
)

// Connector represents a connection proxy for the metadata repository. It contains clients and
// management clients for the metadata repository.
type Connector interface {
	io.Closer

	ClusterID() types.ClusterID

	NumberOfMR() int

	ActiveMRs() map[types.NodeID]string

	ConnectedNodeID() types.NodeID

	// TODO (jun): use context to indicate that it can be network communication.
	Client() (mrc.MetadataRepositoryClient, error)

	// TODO (jun): use context to indicate that it can be network communication.
	ManagementClient() (mrc.MetadataRepositoryManagementClient, error)

	AddRPCAddr(nodeID types.NodeID, addr string)

	DelRPCAddr(nodeID types.NodeID)
}

type connectorImpl struct {
	config

	lastNode         atomic.Value // *struct {types.NodeID, string}
	addrs            sync.Map     // map[types.NodeID]string
	connectedMRProxy atomic.Value // *mrProxy

	group singleflight.Group

	runner *runner.Runner
	cancel context.CancelFunc
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

	rpcAddrs, err := c.fetchRPCAddrsFromSeeds(ctx, c.initCount)
	if err != nil {
		c.logger.Error("could not fetch MR addrs",
			zap.Strings("seeds", c.seed),
			zap.String("err", err.Error()),
		)
		return nil, err
	}

	c.logger.Info("fetch MR addrs",
		zap.Strings("seeds", c.seed),
		zap.Int("# addr", len(rpcAddrs)),
	)

	c.updateRPCAddrs(rpcAddrs)
	if _, err = c.connect(); err != nil {
		c.logger.Error("could not connect to MR", zap.Error(err))
		return nil, err
	}

	c.cancel, err = c.runner.Run(c.fetchAndUpdate)
	if err != nil {
		return nil, errors.Wrap(err, "mrconnector")
	}

	return c, nil
}

func (c *connectorImpl) Close() (err error) {
	c.cancel()
	c.runner.Stop()
	if proxy := c.connectedProxy(); proxy != nil {
		err = proxy.Close()
	}
	return err
}

func (c *connectorImpl) ClusterID() types.ClusterID {
	return c.clusterID
}

func (c *connectorImpl) NumberOfMR() int {
	return len(c.ActiveMRs())
}

func (c *connectorImpl) ActiveMRs() map[types.NodeID]string {
	ret := make(map[types.NodeID]string)
	c.addrs.Range(func(key interface{}, value interface{}) bool {
		if key == nil || value == nil {
			return true
		}
		nodeID := key.(types.NodeID)
		addr := value.(string)
		if nodeID == types.InvalidNodeID || addr == "" {
			return true
		}
		ret[nodeID] = addr
		return true
	})
	return ret
}

func (c *connectorImpl) Client() (mrc.MetadataRepositoryClient, error) {
	return c.connect()
}

func (c *connectorImpl) ManagementClient() (mrc.MetadataRepositoryManagementClient, error) {
	return c.connect()
}

func (c *connectorImpl) ConnectedNodeID() types.NodeID {
	mrProxy, err := c.connect()
	if err != nil {
		return types.InvalidNodeID
	}
	return mrProxy.nodeID
}

func (c *connectorImpl) releaseMRProxy(nodeID types.NodeID) {
	c.addrs.Delete(nodeID)
}

func (c *connectorImpl) fetchAndUpdate(ctx context.Context) {
	ticker := time.NewTicker(c.updateInterval)
	defer ticker.Stop()

	for ctx.Err() == nil {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := c.update(ctx); err != nil {
				c.logger.Error("could not update")
			}
		}
	}
}

func (c *connectorImpl) AddRPCAddr(nodeID types.NodeID, addr string) {
	c.addrs.Store(nodeID, addr)
}

func (c *connectorImpl) DelRPCAddr(nodeID types.NodeID) {
	if proxy := c.connectedProxy(); proxy != nil && proxy.nodeID == nodeID {
		proxy.Close()
	}
	c.addrs.Delete(nodeID)
}

func (c *connectorImpl) update(ctx context.Context) error {
	mrmcl, err := c.ManagementClient()
	if err != nil {
		return errors.Wrap(err, "mrconnector")
	}

	rpcAddrs, err := c.getRPCAddrs(ctx, mrmcl)
	if err != nil {
		return multierr.Append(errors.Wrap(err, "mrconnector"), mrmcl.Close())
	}
	if len(rpcAddrs) == 0 {
		return errors.New("mrconnector: number of mr is zero")
	}

	c.updateRPCAddrs(rpcAddrs)

	if proxy := c.connectedProxy(); proxy != nil {
		if _, ok := c.addrs.Load(proxy.nodeID); !ok {
			if err := proxy.Close(); err != nil {
				c.logger.Error("error while closing mr client", zap.Error(err))
			}
		}
	}
	return nil
}

func (c *connectorImpl) fetchRPCAddrsFromSeeds(ctx context.Context, tryCnt int) (rpcAddrs map[types.NodeID]string, err error) {
	for i := 0; i < tryCnt && ctx.Err() == nil; i++ {
		for j := 0; j < len(c.seed) && ctx.Err() == nil; j++ {
			seedAddr := c.seed[j]
			rpcAddrs, errConn := c.fetchRPCAddrsFromSeed(ctx, seedAddr)
			if errConn == nil {
				return rpcAddrs, nil
			}
			err = multierr.Append(err, errConn)
			time.Sleep(c.initRetryInterval)
		}
	}
	return nil, err
}

func (c *connectorImpl) fetchRPCAddrsFromSeed(ctx context.Context, seedAddr string) (rpcAddrs map[types.NodeID]string, err error) {
	mrcl, mrmcl, err := connectToReliableMR(ctx, c.clusterID, seedAddr, c.connTimeout, c.rpcTimeout)
	if err != nil {
		return nil, err
	}
	rpcAddrs, err = c.getRPCAddrs(ctx, mrmcl)
	if err != nil {
		err = errors.WithMessagef(err, "mrconnector: seed = %s", seedAddr)
	} else if len(rpcAddrs) == 0 {
		err = errors.Errorf("mrconnector: seed = %s, no members", seedAddr)
	}
	err = multierr.Combine(err, mrmcl.Close(), mrcl.Close())
	if err != nil {
		rpcAddrs = nil
	}
	return rpcAddrs, err
}

func (c *connectorImpl) getRPCAddrs(ctx context.Context, mrmcl mrc.MetadataRepositoryManagementClient) (map[types.NodeID]string, error) {
	ciCtx, ciCancel := context.WithTimeout(ctx, c.rpcTimeout)
	defer ciCancel()
	rsp, err := mrmcl.GetClusterInfo(ciCtx, c.clusterID)
	if err != nil {
		return nil, err
	}

	members := rsp.GetClusterInfo().GetMembers()
	addrs := make(map[types.NodeID]string, len(members))
	var mu sync.Mutex

	g, gCtx := errgroup.WithContext(ctx)
	for nodeID := range members {
		nodeID := nodeID
		g.Go(func() error {
			ep := members[nodeID].GetEndpoint()
			mrcl, mrmcl, err := connectToReliableMR(gCtx, c.clusterID, ep, c.connTimeout, c.rpcTimeout)
			if err == nil {
				mu.Lock()
				addrs[nodeID] = ep
				mu.Unlock()

				mrcl.Close()
				mrmcl.Close()
			}
			return nil
		})
	}
	g.Wait()
	return addrs, nil
}

func (c *connectorImpl) connectedProxy() *mrProxy {
	if proxy := c.connectedMRProxy.Load(); proxy != nil {
		return proxy.(*mrProxy)
	}
	return nil
}

func (c *connectorImpl) connectMRProxy(ctx context.Context, nodeID types.NodeID, addr string) (*mrProxy, error) {
	mrcl, mrmcl, err := connectToReliableMR(ctx, c.clusterID, addr, c.connTimeout, c.rpcTimeout)
	if err != nil {
		return nil, err
	}
	proxy := &mrProxy{
		cl:     mrcl,
		mcl:    mrmcl,
		nodeID: nodeID,
		c:      c,
	}
	c.connectedMRProxy.Store(proxy)
	c.lastNode.Store(&struct {
		nodeID types.NodeID
		addr   string
	}{
		nodeID: nodeID,
		addr:   addr,
	})
	return proxy, nil
}

func (c *connectorImpl) connect() (*mrProxy, error) {
	proxy, err, _ := c.group.Do("connect", func() (interface{}, error) {
		if proxy := c.connectedProxy(); proxy != nil && !proxy.disconnected.Load() {
			return proxy, nil
		}

		nodes := c.ActiveMRs()
		if len(nodes) == 0 && c.lastNode.Load() != nil {
			lastNode := c.lastNode.Load().(*struct {
				nodeID types.NodeID
				addr   string
			})
			if lastNode != nil {
				nodes[lastNode.nodeID] = lastNode.addr
			}
		}

		var (
			err     error
			errCurr error
			proxy   *mrProxy
		)
		for nodeID, addr := range nodes {
			proxy, errCurr = c.connectMRProxy(context.Background(), nodeID, addr)
			if errCurr != nil {
				err = multierr.Append(err, errCurr)
				continue
			}
			return proxy, nil
		}
		addrs, errCurr := c.fetchRPCAddrsFromSeeds(context.Background(), 1)
		if errCurr != nil {
			err = multierr.Append(err, errCurr)
			return proxy, err
		}
		for nodeID, addr := range addrs {
			proxy, errCurr = c.connectMRProxy(context.Background(), nodeID, addr)
			if errCurr != nil {
				err = multierr.Append(err, errCurr)
				continue
			}
			return proxy, nil
		}
		errCurr = errors.New("mrconnector: no accessible metadata repository")
		return proxy, multierr.Append(err, errCurr)
	})
	return proxy.(*mrProxy), err
}

func (c *connectorImpl) updateRPCAddrs(newAddrs map[types.NodeID]string) {
	c.addrs.Range(func(nodeID interface{}, addr interface{}) bool {
		if _, ok := newAddrs[nodeID.(types.NodeID)]; !ok {
			c.addrs.Delete(nodeID)
		}
		return true
	})
	for nodeID, addr := range newAddrs {
		c.addrs.Store(nodeID, addr)
	}
}

func makeRPCAddrs(clusterInfo *mrpb.ClusterInfo) map[types.NodeID]string {
	members := clusterInfo.GetMembers()
	addrs := make(map[types.NodeID]string, len(members))
	for nodeID, member := range members {
		endpoint := member.GetEndpoint()
		if endpoint != "" {
			addrs[nodeID] = endpoint
		}
	}
	return addrs
}

func connectToReliableMR(ctx context.Context, clusterID types.ClusterID, addr string, connTimeout, rpcTimeout time.Duration) (mrcl mrc.MetadataRepositoryClient, mrmcl mrc.MetadataRepositoryManagementClient, err error) {
	var (
		connCtx    context.Context
		connCancel context.CancelFunc
		rpcCtx     context.Context
		rpcCancel  context.CancelFunc
		rsp        *mrpb.GetClusterInfoResponse
	)

	connCtx, connCancel = context.WithTimeout(ctx, connTimeout)
	defer connCancel()
	conn, err := rpc.NewConn(connCtx, addr)
	if err != nil {
		return nil, nil, err
	}
	mrcl, err = mrc.NewMetadataRepositoryClientFromRpcConn(conn)
	if err != nil {
		err = errors.Wrapf(err, "mrconnector: addr = %s", addr)
		goto errOut
	}
	mrmcl, err = mrc.NewMetadataRepositoryManagementClientFromRpcConn(conn)
	if err != nil {
		err = errors.Wrapf(err, "mrconnector: addr = %s", addr)
		goto errOut
	}

	// NOTE: This guarantees that the connected node is not split-brained.
	rpcCtx, rpcCancel = context.WithTimeout(ctx, rpcTimeout)
	defer rpcCancel()
	rsp, err = mrmcl.GetClusterInfo(rpcCtx, clusterID)
	if err != nil {
		err = errors.WithMessagef(err, "mrconnector: addr = %s", addr)
		goto errOut
	}
	if rsp.GetClusterInfo().GetLeader() == types.InvalidNodeID {
		err = errors.Errorf("mrconnector: maybe split-brained node, addr = %s", addr)
		goto errOut
	}

	return mrcl, mrmcl, nil
errOut:
	if mrcl != nil {
		err = multierr.Append(err, mrcl.Close())
	}
	if mrmcl != nil {
		err = multierr.Append(err, mrmcl.Close())
	}
	err = multierr.Append(err, conn.Close())
	return nil, nil, err
}
