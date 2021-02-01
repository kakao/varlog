package mrconnector

import (
	"context"
	stderrors "errors"
	"io"
	"strings"
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

var (
	errNoMR = stderrors.New("mrconnector: no accessible metadata repository")
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

	rpcAddrs         sync.Map     // map[types.NodeID]string
	connectedMRProxy atomic.Value // *mrProxy
	group            singleflight.Group

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
		runner:    runner.New("mrconnector", mrcOpts.logger),
		logger:    mrcOpts.logger,
		options:   mrcOpts,
	}

	tctx, cancel := context.WithTimeout(ctx, mrc.options.connectionTimeout)
	defer cancel()

	rpcAddrs, err := mrc.fetchRPCAddrsFromSeeds(tctx, seedRPCAddrs)
	if err != nil {
		mrc.logger.Error("could not fetch MR addrs",
			zap.String("seed", strings.Join(seedRPCAddrs, ",")),
			zap.String("err", err.Error()),
		)
		return nil, err
	}

	mrc.logger.Info("fetch MR addrs",
		zap.String("seed", strings.Join(seedRPCAddrs, ",")),
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
	ret := 0
	c.rpcAddrs.Range(func(_ interface{}, _ interface{}) bool {
		ret++
		return true
	})
	return ret
}

func (c *connector) ActiveMRs() map[types.NodeID]string {
	ret := make(map[types.NodeID]string)
	c.rpcAddrs.Range(func(nodeIDI interface{}, addrI interface{}) bool {
		ret[nodeIDI.(types.NodeID)] = addrI.(string)
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
	c.rpcAddrs.Delete(nodeID)
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
	c.rpcAddrs.Store(nodeID, addr)
}

func (c *connector) DelRPCAddr(nodeID types.NodeID) {
	if proxy := c.connectedProxy(); proxy != nil && proxy.nodeID == nodeID {
		proxy.Close()
	}
	c.rpcAddrs.Delete(nodeID)
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
		if _, ok := c.rpcAddrs.Load(proxy.nodeID); !ok {
			if err := proxy.Close(); err != nil {
				c.logger.Error("error while closing mr client", zap.Error(err))
			}
		}
	}
	return nil
}

func (c *connector) fetchRPCAddrsFromSeeds(ctx context.Context, seedRPCAddrs []string) (rpcAddrs map[types.NodeID]string, err error) {
	for ctx.Err() == nil {
		for _, seedRPCAddr := range seedRPCAddrs {
			rpcAddrs, errConn := c.fetchRPCAddrsFromSeed(ctx, seedRPCAddr)
			if errConn == nil {
				return rpcAddrs, nil
			}
			err = multierr.Append(err, errConn)
			time.Sleep(c.options.rpcAddrsInitialFetchRetryInterval)
		}
	}
	return nil, err
}

func (c *connector) fetchRPCAddrsFromSeed(ctx context.Context, seedRPCAddr string) (rpcAddrs map[types.NodeID]string, err error) {
	mrmcl, err := mrc.NewMetadataRepositoryManagementClient(seedRPCAddr)
	if err != nil {
		return nil, err
	}

	rpcAddrs, err = c.getRPCAddrs(ctx, mrmcl)
	if err != nil {
		err = errors.WithMessagef(err, "mrconnector: seed = %s", seedRPCAddr)
	} else if len(rpcAddrs) == 0 {
		err = errors.Errorf("mrconnector: seed = %s, no members", seedRPCAddr)
	}

	err = multierr.Append(err, mrmcl.Close())
	if err != nil {
		rpcAddrs = nil
	}
	return rpcAddrs, err
}

func (c *connector) getRPCAddrs(ctx context.Context, mrmcl mrc.MetadataRepositoryManagementClient) (map[types.NodeID]string, error) {
	ctx, cancel := context.WithTimeout(ctx, c.options.rpcAddrsFetchTimeout)
	defer cancel()

	rsp, err := mrmcl.GetClusterInfo(ctx, c.clusterID)
	if err != nil {
		return nil, err
	}
	members := rsp.GetClusterInfo().GetMembers()
	addrs := make(map[types.NodeID]string, len(members))
	var mu sync.Mutex
	g, ctx := errgroup.WithContext(ctx)
	for nodeID := range members {
		nodeID := nodeID
		g.Go(func() error {
			ep := members[nodeID].GetEndpoint()
			if mrc, err := mrc.NewMetadataRepositoryClient(ep); err == nil {
				if _, err := mrc.GetMetadata(ctx); err == nil {
					mu.Lock()
					addrs[nodeID] = ep
					mu.Unlock()
				}
				mrc.Close()
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

func (c *connector) connect() (*mrProxy, error) {
	proxy, err, _ := c.group.Do("connect", func() (interface{}, error) {
		if proxy := c.connectedProxy(); proxy != nil && !proxy.disconnected.Load() {
			return proxy, nil
		}

		var (
			noMR  = true
			err   error
			proxy *mrProxy
		)

		c.rpcAddrs.Range(func(nodeID interface{}, addr interface{}) bool {
			if addr == "" || nodeID.(types.NodeID) == types.InvalidNodeID {
				return true
			}
			noMR = false
			mrcl, mrmcl, e := connectToMR(c.clusterID, addr.(string))
			if e != nil {
				c.logger.Debug("could not connect to MR", zap.Error(err), zap.Any("node_id", nodeID), zap.Any("addr", addr))
				err = multierr.Append(err, e)
				return true
			}
			err = nil
			proxy = &mrProxy{
				cl:     mrcl,
				mcl:    mrmcl,
				nodeID: nodeID.(types.NodeID),
				c:      c,
			}
			proxy.disconnected.Store(false)
			c.connectedMRProxy.Store(proxy)
			return false
		})
		if noMR {
			return proxy, errors.New("mrconnector: no accessible metadata repository")
		}
		return proxy, err
	})
	return proxy.(*mrProxy), err
}

func (c *connector) updateRPCAddrs(newAddrs map[types.NodeID]string) {
	c.rpcAddrs.Range(func(nodeID interface{}, addr interface{}) bool {
		if _, ok := newAddrs[nodeID.(types.NodeID)]; !ok {
			c.rpcAddrs.Delete(nodeID)
		}
		return true
	})
	for nodeID, addr := range newAddrs {
		c.rpcAddrs.Store(nodeID, addr)
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

func connectToMR(clusterID types.ClusterID, addr string) (mrcl mrc.MetadataRepositoryClient, mrmcl mrc.MetadataRepositoryManagementClient, err error) {
	// TODO (jun): rpc.NewConn is a blocking function, thus it should be specified by an
	// explicit timeout parameter.
	var conn *rpc.Conn
	conn, err = rpc.NewBlockingConn(addr)
	if err != nil {
		return nil, nil, errors.Wrapf(err, "mrconnector")
	}
	mrcl, err = mrc.NewMetadataRepositoryClientFromRpcConn(conn)
	if err != nil {
		err = errors.Wrapf(err, "mrconnector")
		goto ErrOut
	}
	mrmcl, err = mrc.NewMetadataRepositoryManagementClientFromRpcConn(conn)
	if err != nil {
		err = errors.Wrapf(err, "mrconnector")
		goto ErrOut
	}
	return mrcl, mrmcl, nil

ErrOut:
	if mrcl != nil {
		err = multierr.Append(err, mrcl.Close())
	}
	if mrmcl != nil {
		err = multierr.Append(err, mrmcl.Close())
	}
	err = multierr.Append(err, conn.Close())
	return nil, nil, err
}
