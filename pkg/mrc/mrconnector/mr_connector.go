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

	// TODO (jun): use context to indicate that it can be network communication.
	Client() (mrc.MetadataRepositoryClient, error)

	// TODO (jun): use context to indicate that it can be network communication.
	ManagementClient() (mrc.MetadataRepositoryManagementClient, error)

	AddRPCAddr(nodeID types.NodeID, addr string)

	DelRPCAddr(nodeID types.NodeID)
}

type connector struct {
	clusterID types.ClusterID

	seedAddrs        []string     // seedAddrs is immutable list of mr addresses
	lastNode         atomic.Value // *struct {types.NodeID, string}
	addrs            sync.Map     // map[types.NodeID]string
	connectedMRProxy atomic.Value // *mrProxy

	group singleflight.Group

	runner *runner.Runner
	cancel context.CancelFunc

	logger  *zap.Logger
	options options
}

func New(ctx context.Context, seedRPCAddrs []string, opts ...Option) (Connector, error) {
	if len(seedRPCAddrs) == 0 {
		return nil, errors.New("no seed address")
	}

	mrcOpts := defaultOptions()
	for _, opt := range opts {
		opt(&mrcOpts)
	}
	mrcOpts.logger = mrcOpts.logger.Named("mrconnector")

	mrc := &connector{
		clusterID: mrcOpts.clusterID,
		seedAddrs: seedRPCAddrs,
		runner:    runner.New("mrconnector", mrcOpts.logger),
		logger:    mrcOpts.logger,
		options:   mrcOpts,
	}

	rpcAddrs, err := mrc.fetchRPCAddrsFromSeeds(ctx, mrc.options.initialRPCAddrsFetchRetryCount+1)
	if err != nil {
		mrc.logger.Error("could not fetch MR addrs",
			zap.Strings("seeds", mrc.seedAddrs),
			zap.String("err", err.Error()),
		)
		return nil, err
	}

	mrc.logger.Info("fetch MR addrs",
		zap.Strings("seeds", mrc.seedAddrs),
		zap.Int("# addr", len(rpcAddrs)),
	)

	mrc.updateRPCAddrs(rpcAddrs)
	if _, err = mrc.connect(); err != nil {
		mrc.logger.Error("could not connect to MR", zap.Error(err))
		return nil, err
	}

	mrc.cancel, err = mrc.runner.Run(mrc.fetchAndUpdate)
	if err != nil {
		return nil, errors.Wrap(err, "mrconnector")
	}

	return mrc, nil
}

func (c *connector) Close() (err error) {
	c.cancel()
	c.runner.Stop()
	if proxy := c.connectedProxy(); proxy != nil {
		err = proxy.Close()
	}
	return err
}

func (c *connector) ClusterID() types.ClusterID {
	return c.clusterID
}

func (c *connector) NumberOfMR() int {
	return len(c.ActiveMRs())
}

func (c *connector) ActiveMRs() map[types.NodeID]string {
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

func (c *connector) Client() (mrc.MetadataRepositoryClient, error) {
	return c.connect()
}

func (c *connector) ManagementClient() (mrc.MetadataRepositoryManagementClient, error) {
	return c.connect()
}

func (c *connector) ConnectedNodeID() types.NodeID {
	mrProxy, err := c.connect()
	if err != nil {
		return types.InvalidNodeID
	}
	return mrProxy.nodeID
}

func (c *connector) releaseMRProxy(nodeID types.NodeID) {
	c.addrs.Delete(nodeID)
}

func (c *connector) fetchAndUpdate(ctx context.Context) {
	ticker := time.NewTicker(c.options.rpcAddrsFetchInterval)
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

func (c *connector) AddRPCAddr(nodeID types.NodeID, addr string) {
	c.addrs.Store(nodeID, addr)
}

func (c *connector) DelRPCAddr(nodeID types.NodeID) {
	if proxy := c.connectedProxy(); proxy != nil && proxy.nodeID == nodeID {
		proxy.Close()
	}
	c.addrs.Delete(nodeID)
}

func (c *connector) update(ctx context.Context) error {
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

func (c *connector) fetchRPCAddrsFromSeeds(ctx context.Context, tryCnt int) (rpcAddrs map[types.NodeID]string, err error) {
	for i := 0; i < tryCnt && ctx.Err() == nil; i++ {
		for j := 0; j < len(c.seedAddrs) && ctx.Err() == nil; j++ {
			seedAddr := c.seedAddrs[j]
			rpcAddrs, errConn := c.fetchRPCAddrsFromSeed(ctx, seedAddr)
			if errConn == nil {
				return rpcAddrs, nil
			}
			err = multierr.Append(err, errConn)
			time.Sleep(c.options.initialRPCAddrsFetchRetryInterval)
		}
	}
	return nil, err
}

func (c *connector) fetchRPCAddrsFromSeed(ctx context.Context, seedAddr string) (rpcAddrs map[types.NodeID]string, err error) {
	mrcl, mrmcl, err := connectToReliableMR(ctx, c.clusterID, seedAddr, c.options.connTimeout, c.options.rpcTimeout)
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

func (c *connector) getRPCAddrs(ctx context.Context, mrmcl mrc.MetadataRepositoryManagementClient) (map[types.NodeID]string, error) {
	ciCtx, ciCancel := context.WithTimeout(ctx, c.options.rpcTimeout)
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
			mrcl, mrmcl, err := connectToReliableMR(gCtx, c.clusterID, ep, c.options.connTimeout, c.options.rpcTimeout)
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

func (c *connector) connectedProxy() *mrProxy {
	if proxy := c.connectedMRProxy.Load(); proxy != nil {
		return proxy.(*mrProxy)
	}
	return nil
}

func (c *connector) connectMRProxy(ctx context.Context, nodeID types.NodeID, addr string) (*mrProxy, error) {
	mrcl, mrmcl, err := connectToReliableMR(ctx, c.clusterID, addr, c.options.connTimeout, c.options.rpcTimeout)
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

func (c *connector) connect() (*mrProxy, error) {
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

func (c *connector) updateRPCAddrs(newAddrs map[types.NodeID]string) {
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
