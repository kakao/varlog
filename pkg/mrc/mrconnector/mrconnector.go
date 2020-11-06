package mrconnector

import (
	"context"
	"io"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/kakao/varlog/pkg/mrc"
	"github.com/kakao/varlog/pkg/rpc"
	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/pkg/verrors"
	"github.com/kakao/varlog/proto/mrpb"
)

// Connector represents a connection proxy for the metadata repository. It contains clients and
// management clients for the metadata repository.
type Connector interface {
	io.Closer

	ClusterID() types.ClusterID

	ConnectedNodeID() types.NodeID

	// TODO (jun): use context to indicate that it can be network communication.
	Client() mrc.MetadataRepositoryClient

	// TODO (jun): use context to indicate that it can be network communication.
	ManagementClient() mrc.MetadataRepositoryManagementClient

	Disconnect() (err error)

	UpdateRPCAddrs(clusterInfo *mrpb.ClusterInfo)

	AddRPCAddr(nodeID types.NodeID, addr string)

	DelRPCAddr(nodeID types.NodeID)
}

type connector struct {
	clusterID types.ClusterID

	// NOTE(jun): some possible data types
	// - built-in map and RWMutex
	// - sync.Map and some loops during update
	// - sync.Map and atomic.Value
	rpcAddrs sync.Map

	muConnected     sync.RWMutex
	connectedNodeID types.NodeID
	connectedCL     mrc.MetadataRepositoryClient
	connectedMCL    mrc.MetadataRepositoryManagementClient

	logger  *zap.Logger
	options options
}

func New(ctx context.Context, seedRpcAddrs []string, opts ...Option) (Connector, error) {
	if len(seedRpcAddrs) == 0 {
		return nil, verrors.ErrInvalid
	}

	options := defaultOptions
	for _, opt := range opts {
		opt(&options)
	}
	options.logger = options.logger.Named("mrconnect")

	mrc := &connector{
		clusterID: options.clusterID,
		logger:    options.logger,
		options:   options,
	}

	tctx, cancel := context.WithTimeout(ctx, mrc.options.connectionTimeout)
	defer cancel()

	rpcAddrs, err := mrc.doFetchRPCAddrsLoop(tctx, seedRpcAddrs)
	if err != nil {
		return nil, err
	}
	mrc.updateRPCAddrs(rpcAddrs)
	return mrc, nil
}

func (c *connector) ClusterID() types.ClusterID {
	return c.clusterID
}

func (c *connector) Close() error {
	return c.Disconnect()
}

func (c *connector) Disconnect() (err error) {
	c.muConnected.Lock()
	defer c.muConnected.Unlock()
	if c.connectedCL != nil || c.connectedMCL != nil {
		if e := c.connectedCL.Close(); e != nil {
			err = e
		}
		if e := c.connectedMCL.Close(); e != nil {
			err = e
		}
	}
	c.connectedCL = nil
	c.connectedMCL = nil
	c.connectedNodeID = types.InvalidNodeID
	return err
}

func (c *connector) UpdateRPCAddrs(clusterInfo *mrpb.ClusterInfo) {
	newAddrs := makeRPCAddrs(clusterInfo)
	needToDisconnect := c.needToDisconnect(newAddrs)
	c.updateRPCAddrs(newAddrs)
	if needToDisconnect {
		c.Disconnect()
	}
}

func (c *connector) AddRPCAddr(nodeID types.NodeID, addr string) {
	c.rpcAddrs.Store(nodeID, addr)
}

func (c *connector) DelRPCAddr(nodeID types.NodeID) {
	c.rpcAddrs.Delete(nodeID)
}

func (c *connector) Client() mrc.MetadataRepositoryClient {
	c.connect()
	c.muConnected.RLock()
	defer c.muConnected.RUnlock()
	return c.connectedCL
}

func (c *connector) ManagementClient() mrc.MetadataRepositoryManagementClient {
	c.connect()
	c.muConnected.RLock()
	defer c.muConnected.RUnlock()
	return c.connectedMCL
}

func (c *connector) ConnectedNodeID() types.NodeID {
	c.muConnected.RLock()
	defer c.muConnected.RUnlock()
	return c.connectedNodeID
}

func (c *connector) doFetchRPCAddrsLoop(ctx context.Context, seedRpcAddrs []string) (rpcAddrs map[types.NodeID]string, err error) {
	for ctx.Err() == nil {
		for _, rpcAddr := range seedRpcAddrs {
			rpcAddrs, err = c.fetchRPCAddrs(ctx, rpcAddr)
			// TODO (jun, pharrell): Should the number of announced peers be equal to
			// the number of requested peers?
			// MRManager checks only if the number of announced peers is positive or
			// not. This is more strict condition than the condition of MRManager.
			if err == nil && len(rpcAddrs) >= len(seedRpcAddrs) {
				return rpcAddrs, nil
			}
			time.Sleep(c.options.rpcAddrsFetchRetryInterval)
		}
	}
	return nil, err
}

func (c *connector) fetchRPCAddrs(ctx context.Context, rpcAddr string) (map[types.NodeID]string, error) {
	mrmcl, err := mrc.NewMetadataRepositoryManagementClient(rpcAddr)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err := mrmcl.Close(); err != nil {
			c.logger.Warn("error while closing mrc management client", zap.Error(err))
		}
	}()

	for ctx.Err() == nil {
		rsp, err := mrmcl.GetClusterInfo(ctx, c.clusterID)
		if err != nil {
			return nil, err
		}
		clusterInfo := rsp.GetClusterInfo()
		return makeRPCAddrs(clusterInfo), nil
	}
	return nil, ctx.Err()
}

func (c *connector) connect() (err error) {
	c.muConnected.RLock()
	if c.connectedCL != nil && c.connectedMCL != nil {
		c.muConnected.RUnlock()
		return nil
	}
	c.muConnected.RUnlock()

	var mrcl mrc.MetadataRepositoryClient
	var mrmcl mrc.MetadataRepositoryManagementClient
	c.rpcAddrs.Range(func(nodeID interface{}, addr interface{}) bool {
		if addr == "" {
			return true
		}
		if mrcl, mrmcl, err = connectToMR(c.clusterID, addr.(string)); err == nil {
			c.muConnected.Lock()
			c.connectedCL = mrcl
			c.connectedMCL = mrmcl
			c.connectedNodeID = nodeID.(types.NodeID)
			c.muConnected.Unlock()
			return false
		}
		return true
	})
	return err
}

func (c *connector) needToDisconnect(newAddrs map[types.NodeID]string) bool {
	need := false
	c.rpcAddrs.Range(func(nodeID interface{}, addr interface{}) bool {
		if _, ok := newAddrs[nodeID.(types.NodeID)]; !ok {
			need = true
			return false
		}
		return true
	})
	return need
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
	conn, err := rpc.NewConn(addr)
	if err != nil {
		return nil, nil, err
	}
	mrcl, err = mrc.NewMetadataRepositoryClientFromRpcConn(conn)
	if err != nil {
		goto ErrOut
	}
	mrmcl, err = mrc.NewMetadataRepositoryManagementClientFromRpcConn(conn)
	if err != nil {
		goto ErrOut
	}
	return mrcl, mrmcl, nil

ErrOut:
	if mrcl != nil {
		mrcl.Close()
	}
	if mrmcl != nil {
		mrmcl.Close()
	}
	conn.Close()
	return nil, nil, err
}
