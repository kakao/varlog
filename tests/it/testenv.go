package it

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"google.golang.org/grpc/health/grpc_health_v1"

	"github.com/kakao/varlog/internal/admin"
	"github.com/kakao/varlog/internal/admin/mrmanager"
	"github.com/kakao/varlog/internal/admin/snmanager"
	"github.com/kakao/varlog/internal/metarepos"
	"github.com/kakao/varlog/internal/reportcommitter"
	"github.com/kakao/varlog/internal/storagenode"
	"github.com/kakao/varlog/internal/storagenode/client"
	"github.com/kakao/varlog/internal/storagenode/volume"
	"github.com/kakao/varlog/internal/vtesting"
	"github.com/kakao/varlog/pkg/mrc"
	"github.com/kakao/varlog/pkg/rpc"
	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/pkg/util/testutil/ports"
	"github.com/kakao/varlog/pkg/varlog"
	"github.com/kakao/varlog/proto/admpb"
	"github.com/kakao/varlog/proto/snpb"
	"github.com/kakao/varlog/proto/varlogpb"
)

type VarlogCluster struct {
	config

	// metadata repository
	muMR                 sync.Mutex
	metadataRepositories []*metarepos.RaftMetadataRepository
	mrPeers              []string
	mrRPCEndpoints       []string
	mrIDs                []types.NodeID
	mrCLs                map[types.NodeID]mrc.MetadataRepositoryClient
	mrMCLs               map[types.NodeID]mrc.MetadataRepositoryManagementClient
	cachedMetadata       *varlogpb.MetadataDescriptor

	// storage node
	muSN             sync.Mutex
	storageNodes     map[types.StorageNodeID]*storagenode.StorageNode
	snMCLs           map[types.StorageNodeID]client.StorageNodeManagementClient
	reportCommitters map[types.StorageNodeID]reportcommitter.Client
	volumes          map[types.StorageNodeID]string
	snAddrs          map[types.StorageNodeID]string
	storageNodeIDs   []types.StorageNodeID
	nextSNID         types.StorageNodeID
	manualNextLSID   types.LogStreamID
	snWGs            map[types.StorageNodeID]*sync.WaitGroup

	// log streams
	muLS              sync.Mutex
	topicLogStreamIDs map[types.TopicID][]types.LogStreamID
	// FIXME: type of value
	replicas map[types.LogStreamID][]*varlogpb.ReplicaDescriptor

	// clients
	clients []varlog.Log
	muCL    sync.Mutex

	// logclient
	logClientManager *client.Manager[*client.LogClient]

	muVMS     sync.Mutex
	vmsServer *admin.Admin
	wgVms     sync.WaitGroup
	vmsCL     varlog.Admin

	portLease *ports.Lease

	rng *rand.Rand
}

// NewVarlogCluster creates a new integration testing environment.
//
// The following metadata repository options are set implicitly:
//
// - WithClusterID
// - WithReplicationFactor
// - WithRPCAddress
// - WithRaftAddress
// - WithReporterClientFactory
// - WithSnapshotCount
// - WithRaftTick
// - WithRaftDirectory
// - WithPeers
// - WithRPCTimeout
// - JoinCluster
// - WithTelemetryCollectorName
// - WithTelemetryCollectorEndpoint
// - WithLogger
//
// The following storage node options are set implicitly:
//
// - WithClusterID
// - WithStorageNodeID
// - WithVolumes
// - WithLogger
func NewVarlogCluster(t *testing.T, opts ...Option) *VarlogCluster {
	cfg := newConfig(t, opts)
	clus := &VarlogCluster{
		config:               cfg,
		mrPeers:              make([]string, cfg.nrMR),
		mrRPCEndpoints:       make([]string, cfg.nrMR),
		metadataRepositories: make([]*metarepos.RaftMetadataRepository, cfg.nrMR),
		mrIDs:                make([]types.NodeID, cfg.nrMR),
		mrCLs:                make(map[types.NodeID]mrc.MetadataRepositoryClient),
		mrMCLs:               make(map[types.NodeID]mrc.MetadataRepositoryManagementClient),
		storageNodes:         make(map[types.StorageNodeID]*storagenode.StorageNode),
		snMCLs:               make(map[types.StorageNodeID]client.StorageNodeManagementClient),
		volumes:              make(map[types.StorageNodeID]string),
		snAddrs:              make(map[types.StorageNodeID]string),
		reportCommitters:     make(map[types.StorageNodeID]reportcommitter.Client),
		replicas:             make(map[types.LogStreamID][]*varlogpb.ReplicaDescriptor),
		snWGs:                make(map[types.StorageNodeID]*sync.WaitGroup),
		topicLogStreamIDs:    make(map[types.TopicID][]types.LogStreamID),
		nextSNID:             types.StorageNodeID(1),
		manualNextLSID:       types.MaxLogStreamID,
		rng:                  rand.New(rand.NewSource(time.Now().UnixNano())),
	}

	// ports
	portLease, err := ports.ReserveWeaklyWithRetry(clus.portBase)
	require.NoError(t, err)
	clus.portLease = portLease

	clus.logClientManager, err = client.NewManager[*client.LogClient]()
	assert.NoError(t, err)

	// mr
	clus.initMR(t)
	clus.initMRClients(t)

	// vms
	if clus.startVMS {
		clus.initVMS(t)
		clus.initVMSClient(t)
	}

	// sn
	clus.initSN(t)

	// topic
	clus.initTopic(t)

	// ls
	clus.initLS(t)

	// clients
	clus.initClients(t)

	return clus
}

func (clus *VarlogCluster) initMR(t *testing.T) {
	for i := range clus.mrPeers {
		raftPort := i*2 + clus.portLease.Base()
		rpcPort := i*2 + 1 + clus.portLease.Base()
		clus.mrPeers[i] = fmt.Sprintf("http://127.0.0.1:%d", raftPort)
		clus.mrRPCEndpoints[i] = fmt.Sprintf("127.0.0.1:%d", rpcPort)
	}

	for i := range clus.mrPeers {
		clus.clearMR(t, i)
		clus.createMR(t, i, false, clus.unsafeNoWAL)
	}

	for i := range clus.metadataRepositories {
		clus.startMR(t, i)
	}

	for i := range clus.mrRPCEndpoints {
		clus.healthCheckForMR(t, i)
	}
}

func (clus *VarlogCluster) initMRClients(t *testing.T) {
	for idx := range clus.metadataRepositories {
		clus.newMRClient(t, idx)
	}
}

func (clus *VarlogCluster) initSN(t *testing.T) {
	for i := 0; i < clus.numSN; i++ {
		clus.AddSN(t)
	}
}

func (clus *VarlogCluster) initTopic(t *testing.T) {
	for i := 0; i < clus.numTopic; i++ {
		clus.AddTopic(t)
	}
}

func (clus *VarlogCluster) initLS(t *testing.T) {
	for _, topicID := range clus.TopicIDs() {
		for i := 0; i < clus.numLS; i++ {
			clus.AddLS(t, topicID)
		}
	}
}

func (clus *VarlogCluster) initClients(t *testing.T) {
	for i := 0; i < clus.numCL; i++ {
		clus.clients = append(clus.clients, clus.newClient(t))
	}
}

func (clus *VarlogCluster) closeClients(t *testing.T) {
	for _, client := range clus.clients {
		require.NoError(t, client.Close())
	}
}

func (clus *VarlogCluster) clearMR(t *testing.T, idx int) {
	require.GreaterOrEqual(t, idx, 0)
	require.Less(t, idx, len(clus.metadataRepositories))

	nodeID := types.NewNodeIDFromURL(clus.mrPeers[idx])
	require.NotEqual(t, types.InvalidNodeID, nodeID)

	walPath := fmt.Sprintf("%s/wal/%d", vtesting.TestRaftDir(), nodeID)
	snapPath := fmt.Sprintf("%s/snap/%d", vtesting.TestRaftDir(), nodeID)

	require.NoError(t, os.RemoveAll(walPath))
	require.NoError(t, os.RemoveAll(snapPath))

	clus.logger.Info("metadata repository was cleared",
		zap.Any("idx", idx),
		zap.Any("nid", nodeID),
		zap.Any("wal", walPath),
		zap.Any("snap", snapPath),
	)
}

func (clus *VarlogCluster) createMR(t *testing.T, idx int, join, unsafeNoWal bool) {
	require.GreaterOrEqual(t, idx, 0)
	require.Less(t, idx, len(clus.metadataRepositories))

	nodeID := types.NewNodeIDFromURL(clus.mrPeers[idx])
	require.NotEqual(t, types.InvalidNodeID, nodeID)

	peers := clus.mrPeers

	opts := []metarepos.Option{
		metarepos.WithClusterID(clus.clusterID),
		metarepos.WithReplicationFactor(clus.nrRep),
		metarepos.WithRPCAddress(clus.mrRPCEndpoints[idx]),
		metarepos.WithRaftAddress(clus.mrPeers[idx]),
		metarepos.WithReporterClientFactory(clus.reporterClientFac),
		metarepos.WithLogger(clus.logger.Logger),
		metarepos.WithSnapshotCount(uint64(clus.snapCount)),
		metarepos.WithRaftTick(vtesting.TestRaftTick()),
		metarepos.WithRaftDirectory(vtesting.TestRaftDir()),
		metarepos.WithPeers(peers...),
		metarepos.WithRPCTimeout(vtesting.TimeoutAccordingToProcCnt(metarepos.DefaultRPCTimeout)),
	}
	if join {
		opts = append(opts, metarepos.JoinCluster())
	}
	if clus.collectorName != "" {
		opts = append(opts,
			metarepos.WithTelemetryCollectorName(clus.collectorName),
			metarepos.WithTelemetryCollectorEndpoint("localhost:55680"),
		)
	}
	opts = append(opts, clus.mrOpts...)
	clus.mrIDs[idx] = nodeID
	clus.metadataRepositories[idx] = metarepos.NewRaftMetadataRepository(opts...)

	clus.logger.Info("metadata repository was created", zap.Any("idx", idx), zap.Any("nid", nodeID))
}

func (clus *VarlogCluster) NewMRClient(t *testing.T, idx int) {
	clus.muMR.Lock()
	defer clus.muMR.Unlock()

	require.NotContains(t, clus.mrCLs, idx)
	require.NotContains(t, clus.mrMCLs, idx)

	clus.newMRClient(t, idx)
}

func (clus *VarlogCluster) AppendMR(t *testing.T) {
	clus.muMR.Lock()
	defer clus.muMR.Unlock()

	idx := len(clus.metadataRepositories)
	raftPort := 2*idx + clus.portLease.Base()
	rpcPort := 2*idx + 1 + clus.portLease.Base()
	clus.mrPeers = append(clus.mrPeers, fmt.Sprintf("http://127.0.0.1:%d", raftPort))
	clus.mrRPCEndpoints = append(clus.mrRPCEndpoints, fmt.Sprintf("127.0.0.1:%d", rpcPort))
	clus.mrIDs = append(clus.mrIDs, types.InvalidNodeID)
	clus.metadataRepositories = append(clus.metadataRepositories, nil)

	clus.clearMR(t, idx)
	clus.createMR(t, idx, true, clus.unsafeNoWAL)
}

func (clus *VarlogCluster) RecoverMR(t *testing.T) {
	clus.muMR.Lock()
	defer clus.muMR.Unlock()

	idx := 0
	raftPort := 2*idx + clus.portLease.Base()
	rpcPort := 2*idx + 1 + clus.portLease.Base()
	clus.mrPeers = []string{fmt.Sprintf("http://127.0.0.1:%d", raftPort)}
	clus.mrRPCEndpoints = []string{fmt.Sprintf("127.0.0.1:%d", rpcPort)}
	clus.mrIDs = []types.NodeID{types.InvalidNodeID}
	clus.metadataRepositories = []*metarepos.RaftMetadataRepository{nil}

	clus.createMR(t, idx, false, clus.unsafeNoWAL)
	clus.startMR(t, idx)
}

func (clus *VarlogCluster) StartMR(t *testing.T, idx int) {
	clus.muMR.Lock()
	defer clus.muMR.Unlock()

	clus.startMR(t, idx)
}

func (clus *VarlogCluster) startMR(t *testing.T, idx int) {
	require.GreaterOrEqual(t, idx, 0)
	require.Less(t, idx, len(clus.metadataRepositories))
	clus.metadataRepositories[idx].Run()
}

func (clus *VarlogCluster) stopMR(t *testing.T, idx int) {
	require.GreaterOrEqual(t, idx, 0)
	require.Less(t, idx, len(clus.metadataRepositories))
	require.NoError(t, clus.metadataRepositories[idx].Close())

	clus.logger.Info("MetadataRepository was closed",
		zap.Int("idx", idx),
		zap.Any("nid", clus.mrIDs[idx]),
	)
}

func (clus *VarlogCluster) RestartMR(t *testing.T, idx int) {
	clus.muMR.Lock()
	defer clus.muMR.Unlock()

	require.GreaterOrEqual(t, idx, 0)
	require.Less(t, idx, len(clus.metadataRepositories))

	clus.stopMR(t, idx)
	clus.createMR(t, idx, false, clus.unsafeNoWAL)
	clus.startMR(t, idx)
}

func (clus *VarlogCluster) CloseMR(t *testing.T, idx int) {
	clus.muMR.Lock()
	defer clus.muMR.Unlock()

	clus.closeMR(t, idx)
}

func (clus *VarlogCluster) closeMR(t *testing.T, idx int) {
	require.GreaterOrEqual(t, idx, 0)
	require.Less(t, idx, len(clus.metadataRepositories))

	clus.stopMR(t, idx)
	clus.clearMR(t, idx)
}

func (clus *VarlogCluster) CloseMRAllForRestart(t *testing.T) {
	clus.muMR.Lock()
	defer clus.muMR.Unlock()

	for i := range clus.mrPeers {
		clus.stopMR(t, i)
	}
}

// Close closes all cluster MRs.
func (clus *VarlogCluster) Close(t *testing.T) {
	defer func() {
		if !clus.logger.injected {
			_ = clus.logger.Sync()
		}
	}()

	clus.muMR.Lock()
	defer clus.muMR.Unlock()

	clus.muSN.Lock()
	defer clus.muSN.Unlock()

	clus.muLS.Lock()
	defer clus.muLS.Unlock()

	// clients
	clus.closeClients(t)
	// FIXME: add method for closing vmsCL
	if clus.vmsCL != nil {
		require.NoError(t, clus.vmsCL.Close())
	}
	clus.closeMRClients(t)
	clus.closeSNClients(t)
	clus.closeReportCommitterClients(t)

	// vms
	// FIXME: add method for closing vmsServer
	if clus.vmsServer != nil {
		require.NoError(t, clus.vmsServer.Close())
		clus.wgVms.Wait()
	}

	// mr
	for i := range clus.mrPeers {
		clus.closeMR(t, i)
	}
	require.NoError(t, os.RemoveAll(vtesting.TestRaftDir()))

	// sn
	for _, sn := range clus.storageNodes {
		_ = sn.Close()
	}
	for _, wg := range clus.snWGs {
		wg.Wait()
	}
	clus.topicLogStreamIDs = nil

	assert.NoError(t, clus.logClientManager.Close())

	require.NoError(t, clus.portLease.Release())
}

func (clus *VarlogCluster) healthCheckForMR(t *testing.T, idx int) {
	endpoint := clus.mrRPCEndpoints[idx]
	require.Eventually(t, func() bool {
		conn, err := rpc.NewConn(context.TODO(), endpoint)
		if !assert.NoError(t, err) {
			return false
		}
		defer func() {
			_ = conn.Close()
		}()

		rsp, err := grpc_health_v1.NewHealthClient(conn.Conn).Check(
			context.Background(), &grpc_health_v1.HealthCheckRequest{},
		)
		return err == nil && rsp.GetStatus() == grpc_health_v1.HealthCheckResponse_SERVING
	}, 5*time.Second, 10*time.Millisecond)

	mr := clus.metadataRepositories[idx]
	require.Eventually(t, func() bool {
		return mr.GetServerAddr() != ""
	}, time.Second, 10*time.Millisecond)
}

func (clus *VarlogCluster) HealthCheckForMR(t *testing.T) {
	clus.muMR.Lock()
	defer clus.muMR.Unlock()

	for i := range clus.mrRPCEndpoints {
		clus.healthCheckForMR(t, i)
	}
}

func (clus *VarlogCluster) indexOfLeaderMR() int {
	leader := -1
	for i, n := range clus.metadataRepositories {
		cinfo, _ := n.GetClusterInfo(context.TODO(), clus.clusterID)
		if cinfo.GetLeader() != types.InvalidNodeID && clus.mrIDs[i] == cinfo.GetLeader() {
			leader = i
			break
		}
	}
	return leader
}

func (clus *VarlogCluster) IndexOfLeaderMR() int {
	clus.muMR.Lock()
	defer clus.muMR.Unlock()

	return clus.indexOfLeaderMR()
}

func (clus *VarlogCluster) LeaderFail(t *testing.T) {
	clus.muMR.Lock()
	defer clus.muMR.Unlock()

	leader := clus.indexOfLeaderMR()
	require.GreaterOrEqual(t, leader, 0)

	clus.stopMR(t, leader)
}

func (clus *VarlogCluster) AddSN(t *testing.T) types.StorageNodeID {
	clus.muSN.Lock()
	defer clus.muSN.Unlock()

	snID := clus.nextSNID
	clus.nextSNID++

	volume := t.TempDir()

	opts := []storagenode.Option{
		storagenode.WithClusterID(clus.clusterID),
		storagenode.WithStorageNodeID(snID),
		storagenode.WithVolumes(volume),
		storagenode.WithLogger(clus.logger.Named("sn").With(zap.Int32("snid", int32(snID)))),
	}
	opts = append(opts, clus.snOpts...)

	sn := storagenode.TestNewSimpleStorageNode(t, opts...)
	if _, ok := clus.snWGs[snID]; !ok {
		clus.snWGs[snID] = new(sync.WaitGroup)
	}

	clus.snWGs[snID].Add(1)
	go func() {
		defer clus.snWGs[snID].Done()
		_ = sn.Serve()
	}()

	var addr string
	require.Eventually(t, func() bool {
		meta := storagenode.TestGetStorageNodeMetadataDescriptorWithoutAddr(t, sn)
		addr = meta.Address
		return len(addr) > 0
	}, time.Second, 10*time.Millisecond)

	_, err := clus.vmsCL.AddStorageNode(context.Background(), snID, addr)
	require.NoError(t, err)

	mcl, err := client.NewManagementClient(context.Background(), clus.clusterID, addr, clus.logger.Logger)
	require.NoError(t, err)

	clus.storageNodes[snID] = sn
	clus.volumes[snID] = volume
	clus.snAddrs[snID] = addr
	clus.snMCLs[snID] = mcl
	clus.storageNodeIDs = append(clus.storageNodeIDs, snID)
	clus.newReportCommitterClient(t, snID, addr)
	return snID
}

func (clus *VarlogCluster) NewReportCommitterClient(t *testing.T, snID types.StorageNodeID) {
	clus.muSN.Lock()
	defer clus.muSN.Unlock()

	require.Contains(t, clus.storageNodes, snID)
	require.NotContains(t, clus.reportCommitters, snID)

	require.Contains(t, clus.snAddrs, snID)
	clus.newReportCommitterClient(t, snID, clus.snAddrs[snID])
}

func (clus *VarlogCluster) newReportCommitterClient(t *testing.T, snID types.StorageNodeID, addr string) {
	require.NotContains(t, clus.reportCommitters, snID)
	client, err := reportcommitter.NewClient(context.Background(), addr)
	require.NoError(t, err)
	clus.reportCommitters[snID] = client
}

func (clus *VarlogCluster) NewSNClient(t *testing.T, snID types.StorageNodeID) {
	clus.muSN.Lock()
	defer clus.muSN.Unlock()

	require.Contains(t, clus.storageNodes, snID)
	require.NotContains(t, clus.snMCLs, snID)

	addr, ok := clus.snAddrs[snID]
	require.True(t, ok)

	mcl, err := client.NewManagementClient(context.Background(), clus.clusterID, addr, clus.logger.Logger)
	require.NoError(t, err)

	clus.snMCLs[snID] = mcl
}

// FIXME: Extract common codes between AddSN.
func (clus *VarlogCluster) RecoverSN(t *testing.T, snID types.StorageNodeID) *storagenode.StorageNode {
	clus.muSN.Lock()
	defer clus.muSN.Unlock()

	// TODO: make sure that sn is stopped by this framework.

	require.Contains(t, clus.volumes, snID)
	require.Contains(t, clus.snAddrs, snID)
	volume := clus.volumes[snID]
	addr := clus.snAddrs[snID]

	sn := storagenode.TestNewSimpleStorageNode(t,
		storagenode.WithClusterID(clus.clusterID),
		storagenode.WithStorageNodeID(snID),
		storagenode.WithListenAddress(addr),
		storagenode.WithVolumes(volume),
	)

	if _, ok := clus.snWGs[snID]; !ok {
		clus.snWGs[snID] = new(sync.WaitGroup)
	}

	clus.snWGs[snID].Add(1)
	go func() {
		defer clus.snWGs[snID].Done()
		_ = sn.Serve()
	}()

	storagenode.TestWaitForStartingOfServe(t, sn)

	clus.storageNodes[snID] = sn

	return sn
}

func (clus *VarlogCluster) AddTopic(t *testing.T) types.TopicID {
	clus.muSN.Lock()
	defer clus.muSN.Unlock()

	clus.muLS.Lock()
	defer clus.muLS.Unlock()

	topicDesc, err := clus.vmsCL.AddTopic(context.Background())
	require.NoError(t, err)

	topicID := topicDesc.GetTopicID()

	clus.topicLogStreamIDs[topicID] = nil
	return topicID
}

func (clus *VarlogCluster) AddLS(t *testing.T, topicID types.TopicID) types.LogStreamID {
	clus.muSN.Lock()
	defer clus.muSN.Unlock()

	clus.muLS.Lock()
	defer clus.muLS.Unlock()

	require.GreaterOrEqual(t, len(clus.storageNodes), clus.nrRep)

	logStreamDesc, err := clus.vmsCL.AddLogStream(context.Background(), topicID, nil)
	require.NoError(t, err)
	require.Len(t, logStreamDesc.Replicas, clus.nrRep)
	for _, replica := range logStreamDesc.Replicas {
		// Verify the data path and storage node path for each ReplicaDescriptor.
		dataDir, err := volume.ParseDataDir(replica.DataPath)
		require.NoError(t, err)
		snpath := filepath.Join(dataDir.Volume, volume.StorageNodeDirName(dataDir.ClusterID, dataDir.StorageNodeID))
		require.Equal(t, replica.StorageNodePath, snpath)
	}

	logStreamID := logStreamDesc.GetLogStreamID()

	// FIXME: use map to store logstream and its replicas
	logStreamIDs := clus.topicLogStreamIDs[topicID]
	clus.topicLogStreamIDs[topicID] = append(logStreamIDs, logStreamID)
	clus.replicas[logStreamID] = logStreamDesc.GetReplicas()

	return logStreamID
}

func (clus *VarlogCluster) UpdateLS(t *testing.T, tpID types.TopicID, lsID types.LogStreamID, oldsn, newsn types.StorageNodeID) {
	clus.muSN.Lock()
	defer clus.muSN.Unlock()

	clus.muLS.Lock()
	defer clus.muLS.Unlock()

	var oldSNPath string
	// check replicas
	require.Contains(t, clus.replicas, lsID)
	rds := clus.replicas[lsID]
	require.Condition(t, func() bool {
		for _, rd := range rds {
			if rd.GetStorageNodeID() == oldsn {
				oldSNPath = rd.StorageNodePath
				return true
			}
		}
		return false
	})

	require.Contains(t, clus.snMCLs, newsn)
	snmd, err := clus.snMCLs[newsn].GetMetadata(context.TODO())
	require.NoError(t, err)
	path := snmd.Storages[0].Path

	newReplica := varlogpb.ReplicaDescriptor{
		StorageNodeID:   newsn,
		StorageNodePath: path,
	}
	oldReplica := varlogpb.ReplicaDescriptor{
		StorageNodeID:   oldsn,
		StorageNodePath: oldSNPath,
	}

	lsd, err := clus.vmsCL.UpdateLogStream(context.Background(), tpID, lsID, oldReplica, newReplica)
	require.NoError(t, err)
	require.Len(t, lsd.Replicas, clus.nrRep)
	for _, replica := range lsd.Replicas {
		// Verify the data path and storage node path for each ReplicaDescriptor.
		dataDir, err := volume.ParseDataDir(replica.DataPath)
		require.NoError(t, err)
		snpath := filepath.Join(dataDir.Volume, volume.StorageNodeDirName(dataDir.ClusterID, dataDir.StorageNodeID))
		require.Equal(t, replica.StorageNodePath, snpath)
	}

	// update replicas
	for i := range rds {
		if rds[i].GetStorageNodeID() == oldsn {
			rds[i] = &newReplica
		}
	}
}

func (clus *VarlogCluster) Seal(tpID types.TopicID, lsID types.LogStreamID) (*admpb.SealResponse, error) {
	return clus.vmsCL.Seal(context.Background(), tpID, lsID)
}

func (clus *VarlogCluster) Unseal(tpID types.TopicID, lsID types.LogStreamID) (*varlogpb.LogStreamDescriptor, error) {
	return clus.vmsCL.Unseal(context.Background(), tpID, lsID)
}

func (clus *VarlogCluster) AddLSWithoutMR(t *testing.T, topicID types.TopicID) types.LogStreamID {
	clus.muSN.Lock()
	defer clus.muSN.Unlock()

	clus.muLS.Lock()
	defer clus.muLS.Unlock()

	require.GreaterOrEqual(t, len(clus.storageNodes), clus.nrRep)

	lsID := clus.manualNextLSID
	clus.manualNextLSID--

	rds := make([]*varlogpb.ReplicaDescriptor, 0, clus.nrRep)
	replicas := make([]varlogpb.LogStreamReplica, 0, clus.nrRep)
	for idx := range clus.rng.Perm(len(clus.storageNodeIDs))[:clus.nrRep] {
		snID := clus.storageNodeIDs[idx]
		replicas = append(replicas, varlogpb.LogStreamReplica{
			StorageNode: varlogpb.StorageNode{
				StorageNodeID: snID,
				Address:       clus.snAddrs[snID],
			},
			TopicLogStream: varlogpb.TopicLogStream{
				LogStreamID: lsID,
			},
		})

		snmd, err := clus.storageNodeManagementClientOf(t, snID).GetMetadata(context.Background())
		require.NoError(t, err)
		path := snmd.Storages[0].Path
		rds = append(rds, &varlogpb.ReplicaDescriptor{
			StorageNodeID:   snID,
			StorageNodePath: path,
		})
	}

	for _, rd := range rds {
		snID := rd.StorageNodeID
		path := rd.StorageNodePath
		lsrmd, err := clus.storageNodeManagementClientOf(t, snID).AddLogStreamReplica(
			context.Background(),
			topicID,
			lsID,
			path,
		)
		require.NoError(t, err)
		_, err = volume.ParseDataDir(lsrmd.Path)
		require.NoError(t, err)

		status, _, err := clus.storageNodeManagementClientOf(t, snID).Seal(context.Background(), topicID, lsID, types.InvalidGLSN)
		require.NoError(t, err)
		require.Equal(t, varlogpb.LogStreamStatusSealed, status)

		require.NoError(t, clus.storageNodeManagementClientOf(t, snID).Unseal(context.Background(), topicID, lsID, replicas))
	}

	// FIXME: use map to store logstream and its replicas
	logStreamIDs := clus.topicLogStreamIDs[topicID]
	clus.topicLogStreamIDs[topicID] = append(logStreamIDs, lsID)
	clus.replicas[lsID] = rds
	clus.logger.Info("AddLS without MR", zap.Any("lsid", lsID), zap.Any("replicas", replicas))
	return lsID
}

func (clus *VarlogCluster) AddLSIncomplete(t *testing.T, topicID types.TopicID) types.LogStreamID {
	clus.muSN.Lock()
	defer clus.muSN.Unlock()

	clus.muLS.Lock()
	defer clus.muLS.Unlock()

	require.GreaterOrEqual(t, len(clus.storageNodes), clus.nrRep)

	lsID := clus.manualNextLSID
	clus.manualNextLSID--

	replicas := make([]varlogpb.LogStreamReplica, 0, clus.nrRep-1)
	for idx := range clus.rng.Perm(len(clus.storageNodeIDs))[:clus.nrRep-1] {
		snID := clus.storageNodeIDs[idx]
		replicas = append(replicas, varlogpb.LogStreamReplica{
			StorageNode: varlogpb.StorageNode{
				StorageNodeID: snID,
				Address:       clus.snAddrs[snID],
			},
			TopicLogStream: varlogpb.TopicLogStream{
				LogStreamID: lsID,
			},
		})
	}

	for _, replica := range replicas {
		snID := replica.StorageNodeID
		snmd, err := clus.storageNodeManagementClientOf(t, snID).GetMetadata(context.Background())
		require.NoError(t, err)
		path := snmd.Storages[0].Path

		_, err = clus.storageNodeManagementClientOf(t, snID).AddLogStreamReplica(
			context.Background(),
			topicID,
			lsID,
			path,
		)
		require.NoError(t, err)
	}
	clus.logger.Info("AddLS incompletely", zap.Any("lsid", lsID), zap.Any("replicas", replicas))
	return lsID
}

func (clus *VarlogCluster) UpdateLSWithoutMR(t *testing.T, topicID types.TopicID, logStreamID types.LogStreamID, storageNodeID types.StorageNodeID, clear bool) {
	clus.muSN.Lock()
	defer clus.muSN.Unlock()

	clus.muLS.Lock()
	defer clus.muLS.Unlock()

	log.Println("UpdateLS without MR")

	require.GreaterOrEqual(t, len(clus.storageNodes), clus.nrRep)

	replicas, ok := clus.replicas[logStreamID]
	require.Equal(t, ok, true)

	require.Eventually(t, func() bool {
		for _, rd := range replicas {
			snid := rd.GetStorageNodeID()
			if !assert.Contains(t, clus.snMCLs, snid) {
				return false
			}

			snmd, err := clus.snMCLs[snid].GetMetadata(context.Background())
			require.NoError(t, err)

			lsmd, ok := snmd.GetLogStream(logStreamID)
			require.True(t, ok)

			if lsmd.GetStatus() != varlogpb.LogStreamStatusRunning {
				return false
			}

			_, _, err = clus.snMCLs[snid].Seal(context.Background(), topicID, logStreamID, lsmd.LocalHighWatermark.GLSN)
			require.NoError(t, err)
		}
		return true
	}, 3*time.Second, 100*time.Millisecond)

	victim := replicas[0]

	meta, err := clus.snMCLs[storageNodeID].GetMetadata(context.Background())
	require.NoError(t, err)

	path := meta.Storages[0].Path

	_, err = clus.snMCLs[storageNodeID].AddLogStreamReplica(context.Background(), topicID, logStreamID, path)
	require.NoError(t, err)

	replicas[0] = &varlogpb.ReplicaDescriptor{
		StorageNodeID:   storageNodeID,
		StorageNodePath: path,
	}

	clus.replicas[logStreamID] = replicas

	if clear {
		require.NoError(t, clus.snMCLs[victim.StorageNodeID].RemoveLogStream(context.Background(), topicID, logStreamID))
	}
}

func (clus *VarlogCluster) UnsealWithoutMR(t *testing.T, topicID types.TopicID, logStreamID types.LogStreamID, expectedHighWatermark types.GLSN) {
	clus.muSN.Lock()
	defer clus.muSN.Unlock()

	clus.muLS.Lock()
	defer clus.muLS.Unlock()

	log.Println("Unseal without MR")

	rds, ok := clus.replicas[logStreamID]
	require.Equal(t, ok, true)

	replicas := make([]varlogpb.LogStreamReplica, 0, len(rds))
	for _, rd := range rds {
		snid := rd.GetStorageNodeID()
		require.Contains(t, clus.snMCLs, snid)

		snmd, err := clus.snMCLs[snid].GetMetadata(context.Background())
		require.NoError(t, err)

		lsmd, ok := snmd.GetLogStream(logStreamID)
		require.True(t, ok)
		require.NotEqual(t, lsmd.GetStatus(), varlogpb.LogStreamStatusRunning)

		require.Equal(t, expectedHighWatermark, lsmd.GetLocalHighWatermark())

		replicas = append(replicas, varlogpb.LogStreamReplica{
			StorageNode: varlogpb.StorageNode{
				StorageNodeID: snid,
			},
			TopicLogStream: varlogpb.TopicLogStream{
				LogStreamID: logStreamID,
			},
		})
	}

	for _, rd := range rds {
		snid := rd.GetStorageNodeID()

		snmd, err := clus.snMCLs[snid].GetMetadata(context.Background())
		require.NoError(t, err)

		lsmd, ok := snmd.GetLogStream(logStreamID)
		require.True(t, ok)

		if lsmd.GetStatus() == varlogpb.LogStreamStatusSealing {
			_, _, err = clus.snMCLs[snid].Seal(context.Background(), topicID, logStreamID, types.InvalidGLSN)
			require.NoError(t, err)
		}

		err = clus.snMCLs[snid].Unseal(context.Background(), topicID, logStreamID, replicas)
		require.NoError(t, err)
	}
}

func (clus *VarlogCluster) StorageNodes() map[types.StorageNodeID]*storagenode.StorageNode {
	clus.muSN.Lock()
	defer clus.muSN.Unlock()

	ret := make(map[types.StorageNodeID]*storagenode.StorageNode, len(clus.storageNodes))
	for id, sn := range clus.storageNodes {
		ret[id] = sn
	}
	return ret
}

func (clus *VarlogCluster) StorageNodesManagementClients() map[types.StorageNodeID]client.StorageNodeManagementClient {
	clus.muSN.Lock()
	defer clus.muSN.Unlock()

	ret := make(map[types.StorageNodeID]client.StorageNodeManagementClient, len(clus.snMCLs))
	for id, sn := range clus.snMCLs {
		ret[id] = sn
	}

	return ret
}

func (clus *VarlogCluster) CloseSN(t *testing.T, snID types.StorageNodeID) {
	clus.muSN.Lock()
	defer clus.muSN.Unlock()

	require.Contains(t, clus.storageNodes, snID)
	_ = clus.storageNodes[snID].Close()
	clus.snWGs[snID].Wait()

	log.Printf("SN.Close: %v", snID)
}

func (clus *VarlogCluster) LookupSN(t *testing.T, snID types.StorageNodeID) *storagenode.StorageNode {
	clus.muSN.Lock()
	defer clus.muSN.Unlock()
	require.Contains(t, clus.storageNodes, snID)
	return clus.storageNodes[snID]
}

func (clus *VarlogCluster) newMRClient(t *testing.T, idx int) {
	id := clus.mrIDs[idx]
	addr := clus.mrRPCEndpoints[idx]
	rpcConn, err := rpc.NewConn(context.Background(), addr)
	require.NoError(t, err)

	cl, err := mrc.NewMetadataRepositoryClientFromRPCConn(rpcConn)
	require.NoError(t, err)

	mcl, err := mrc.NewMetadataRepositoryManagementClientFromRPCConn(rpcConn)
	require.NoError(t, err)

	clus.mrCLs[id] = cl
	clus.mrMCLs[id] = mcl
}

func (clus *VarlogCluster) MetadataRepositories() []*metarepos.RaftMetadataRepository {
	clus.muMR.Lock()
	defer clus.muMR.Unlock()

	ret := make([]*metarepos.RaftMetadataRepository, len(clus.metadataRepositories))
	copy(ret, clus.metadataRepositories)
	return ret
}

func (clus *VarlogCluster) GetMR(t *testing.T) *metarepos.RaftMetadataRepository {
	return clus.GetMRByIndex(t, 0)
}

func (clus *VarlogCluster) GetMRByIndex(t *testing.T, idx int) *metarepos.RaftMetadataRepository {
	clus.muMR.Lock()
	defer clus.muMR.Unlock()

	require.Greater(t, len(clus.metadataRepositories), idx)
	return clus.metadataRepositories[idx]
}

func (clus *VarlogCluster) MRRPCEndpoints() []string {
	clus.muMR.Lock()
	defer clus.muMR.Unlock()

	ret := make([]string, len(clus.mrRPCEndpoints))
	copy(ret, clus.mrRPCEndpoints)
	return ret
}

func (clus *VarlogCluster) MRRPCEndpointAtIndex(t *testing.T, idx int) string {
	clus.muMR.Lock()
	defer clus.muMR.Unlock()

	require.Greater(t, len(clus.mrRPCEndpoints), idx)
	return clus.mrRPCEndpoints[idx]
}

func (clus *VarlogCluster) MRPeers() []string {
	clus.muMR.Lock()
	defer clus.muMR.Unlock()

	ret := make([]string, len(clus.mrPeers))
	copy(ret, clus.mrPeers)
	return ret
}

func (clus *VarlogCluster) MRPeerAtIndex(t *testing.T, idx int) string {
	clus.muMR.Lock()
	defer clus.muMR.Unlock()

	require.Greater(t, len(clus.mrPeers), idx)
	return clus.mrPeers[idx]
}

func (clus *VarlogCluster) MetadataRepositoryIDs() []types.NodeID {
	clus.muMR.Lock()
	defer clus.muMR.Unlock()

	ret := make([]types.NodeID, len(clus.mrIDs))
	copy(ret, clus.mrIDs)
	return ret
}

func (clus *VarlogCluster) MetadataRepositoryIDAt(t *testing.T, idx int) types.NodeID {
	clus.muMR.Lock()
	defer clus.muMR.Unlock()
	return clus.metadataRepositoryIDAt(t, idx)
}

func (clus *VarlogCluster) metadataRepositoryIDAt(t *testing.T, idx int) types.NodeID {
	require.Greater(t, len(clus.mrIDs), idx)
	return clus.mrIDs[idx]
}

func (clus *VarlogCluster) MRClientAt(t *testing.T, idx int) mrc.MetadataRepositoryClient {
	clus.muMR.Lock()
	defer clus.muMR.Unlock()

	require.Greater(t, len(clus.mrIDs), idx)
	id := clus.mrIDs[idx]
	require.Contains(t, clus.mrCLs, id)
	return clus.mrCLs[id]
}

func (clus *VarlogCluster) MRManagementClientAt(t *testing.T, idx int) mrc.MetadataRepositoryManagementClient {
	clus.muMR.Lock()
	defer clus.muMR.Unlock()

	require.Greater(t, len(clus.mrIDs), idx)
	id := clus.mrIDs[idx]
	require.Contains(t, clus.mrMCLs, id)
	return clus.mrMCLs[id]
}

func (clus *VarlogCluster) LookupMR(nodeID types.NodeID) (*metarepos.RaftMetadataRepository, bool) {
	clus.muMR.Lock()
	defer clus.muMR.Unlock()

	for idx, mrID := range clus.mrIDs {
		if nodeID == mrID {
			return clus.metadataRepositories[idx], true
		}
	}
	return nil, false
}

func (clus *VarlogCluster) GetVMS() *admin.Admin {
	clus.muVMS.Lock()
	defer clus.muVMS.Unlock()

	return clus.vmsServer
}

func (clus *VarlogCluster) getSN(t *testing.T, lsID types.LogStreamID, idx int) *storagenode.StorageNode {
	// FIXME: extract below codes to method
	clus.muMR.Lock()
	defer clus.muMR.Unlock()

	var (
		err error
		md  *varlogpb.MetadataDescriptor
	)
	require.Positive(t, len(clus.mrCLs))
	for _, mrcl := range clus.mrCLs {
		md, err = mrcl.GetMetadata(context.Background())
		if err == nil {
			break
		}
	}
	require.NoError(t, err)
	require.NotNil(t, md)

	lsd, err := md.HaveLogStream(lsID)
	require.NoError(t, err)
	require.Greater(t, len(lsd.GetReplicas()), idx)

	snid := lsd.GetReplicas()[idx].GetStorageNodeID()
	return clus.LookupSN(t, snid)
}

func (clus *VarlogCluster) newClient(t *testing.T) varlog.Log {
	cl, err := varlog.Open(context.Background(), clus.clusterID, clus.mrRPCEndpoints)
	require.NoError(t, err)
	return cl
}

func (clus *VarlogCluster) PrimaryStorageNodeIDOf(t *testing.T, lsID types.LogStreamID) types.StorageNodeID {
	sn := clus.getSN(t, lsID, 0)
	return storagenode.TestGetStorageNodeID(t, sn)
}

func (clus *VarlogCluster) BackupStorageNodeIDOf(t *testing.T, lsID types.LogStreamID) types.StorageNodeID {
	idx := 1
	if clus.nrRep > 2 {
		idx += rand.Intn(clus.nrRep - 1)
	}
	sn := clus.getSN(t, lsID, idx)
	return storagenode.TestGetStorageNodeID(t, sn)
}

/*
func (clus *VarlogCluster) NewLogIOClient(t *testing.T, lsID types.LogStreamID) *logclient.LogClient {
	snID := clus.PrimaryStorageNodeIDOf(t, lsID)
	snmd, err := clus.SNClientOf(t, snID).GetMetadata(context.Background())
	require.NoError(t, err)

	cl, err := logclient.NewLogIOClient(context.TODO(), snmd.GetStorageNode().GetAddress())
	require.NoError(t, err)
	return cl
}
*/

func (clus *VarlogCluster) initVMS(t *testing.T) {
	mrMgrOpts := append(clus.mrMgrOpts,
		mrmanager.WithAddresses(clus.mrRPCEndpoints...),
		mrmanager.WithLogger(clus.logger.Logger),
		mrmanager.WithClusterID(clus.clusterID),
		mrmanager.WithReplicationFactor(clus.nrRep),
	)
	mrMgr, err := mrmanager.New(context.TODO(), mrMgrOpts...)
	require.NoError(t, err)

	snMgr, err := snmanager.New(context.TODO(),
		snmanager.WithClusterID(clus.clusterID),
		snmanager.WithClusterMetadataView(mrMgr.ClusterMetadataView()),
		snmanager.WithLogger(clus.logger.Logger),
	)
	require.NoError(t, err)

	listenAddress := fmt.Sprintf("127.0.0.1:%d", clus.portLease.Base()+clus.vmsPortOffset)
	opts := append(clus.VMSOpts,
		admin.WithListenAddress(listenAddress),
		admin.WithClusterID(clus.clusterID),
		admin.WithReplicationFactor(clus.nrRep),
		admin.WithLogger(clus.logger.Named("admin")),
		admin.WithMetadataRepositoryManager(mrMgr),
		admin.WithStorageNodeManager(snMgr),
	)

	cm, err := admin.New(context.Background(), opts...)
	require.NoError(t, err)

	clus.wgVms.Add(1)
	go func() {
		defer clus.wgVms.Done()
		require.NoError(t, cm.Serve())
	}()

	require.Eventually(t, func() bool {
		rpcConn, err := rpc.NewConn(context.Background(), listenAddress)
		if err != nil {
			return false
		}
		defer func() {
			assert.NoError(t, rpcConn.Close())
		}()
		rsp, err := grpc_health_v1.NewHealthClient(rpcConn.Conn).Check(
			context.Background(),
			&grpc_health_v1.HealthCheckRequest{},
		)
		return err == nil && rsp.GetStatus() == grpc_health_v1.HealthCheckResponse_SERVING
	}, 3*time.Second, 10*time.Millisecond)

	clus.vmsServer = cm
}

func (clus *VarlogCluster) initVMSClient(t *testing.T) {
	addr := clus.vmsServer.Address()
	cmcli, err := varlog.NewAdmin(context.TODO(), addr)
	require.NoError(t, err)
	clus.vmsCL = cmcli
}

func (clus *VarlogCluster) StartVMS(t *testing.T) {
	clus.muVMS.Lock()
	defer clus.muVMS.Unlock()

	require.Nil(t, clus.vmsServer)
	clus.initVMS(t)
}

func (clus *VarlogCluster) RestartVMS(t *testing.T) {
	clus.muVMS.Lock()
	defer clus.muVMS.Unlock()

	require.NotNil(t, clus.vmsServer)
	require.NoError(t, clus.vmsServer.Close())
	clus.wgVms.Wait()

	clus.initVMS(t)

	if clus.vmsCL != nil {
		require.NoError(t, clus.vmsCL.Close())
	}
	clus.initVMSClient(t)
}

func (clus *VarlogCluster) GetVMSClient(t *testing.T) varlog.Admin {
	clus.muVMS.Lock()
	defer clus.muVMS.Unlock()

	if clus.vmsCL == nil {
		clus.initVMSClient(t)
	}

	return clus.vmsCL
}

func (clus *VarlogCluster) Logger() *zap.Logger {
	return clus.logger.Logger
}

func (clus *VarlogCluster) TopicIDs() []types.TopicID {
	clus.muLS.Lock()
	defer clus.muLS.Unlock()

	var ret []types.TopicID
	for topicID := range clus.topicLogStreamIDs {
		ret = append(ret, topicID)
	}

	sort.Slice(ret, func(i, j int) bool { return ret[i] < ret[j] })

	return ret
}

func (clus *VarlogCluster) LogStreamIDs(topicID types.TopicID) []types.LogStreamID {
	clus.muLS.Lock()
	defer clus.muLS.Unlock()

	logStreamIDs, ok := clus.topicLogStreamIDs[topicID]
	if !ok {
		return nil
	}
	ret := make([]types.LogStreamID, len(logStreamIDs))
	copy(ret, logStreamIDs)
	return ret
}

func (clus *VarlogCluster) LogStreamID(t *testing.T, topicID types.TopicID, idx int) types.LogStreamID {
	clus.muLS.Lock()
	defer clus.muLS.Unlock()

	logStreamIDs := clus.topicLogStreamIDs[topicID]
	require.Greater(t, len(logStreamIDs), idx)

	return logStreamIDs[idx]
}

func (clus *VarlogCluster) CloseMRClientAt(t *testing.T, idx int) {
	clus.muMR.Lock()
	defer clus.muMR.Unlock()
	clus.closeMRClientAt(t, idx)
}

func (clus *VarlogCluster) closeMRClientAt(t *testing.T, idx int) {
	nodeID := clus.metadataRepositoryIDAt(t, idx)

	require.Contains(t, clus.mrCLs, nodeID)
	require.NoError(t, clus.mrCLs[nodeID].Close())

	require.Contains(t, clus.mrMCLs, nodeID)
	require.NoError(t, clus.mrMCLs[nodeID].Close())
}

func (clus *VarlogCluster) closeMRClients(t *testing.T) {
	for id, cl := range clus.mrCLs {
		require.NoErrorf(t, cl.Close(), "NodeID=%d", id)
		delete(clus.mrCLs, id)
	}

	for id, mcl := range clus.mrMCLs {
		require.NoErrorf(t, mcl.Close(), "NodeID=%d", id)
		delete(clus.mrMCLs, id)
	}
}

func (clus *VarlogCluster) CloseSNClientOf(t *testing.T, snID types.StorageNodeID) {
	clus.muSN.Lock()
	defer clus.muSN.Unlock()
	clus.closeSNClientOf(t, snID)
}

func (clus *VarlogCluster) closeSNClientOf(t *testing.T, snID types.StorageNodeID) {
	require.Contains(t, clus.snMCLs, snID)
	require.NoErrorf(t, clus.snMCLs[snID].Close(), "StorageNodeID=%d", snID)
	delete(clus.snMCLs, snID)

	require.Contains(t, clus.reportCommitters, snID)
	require.NoErrorf(t, clus.reportCommitters[snID].Close(), "StorageNodeID=%d", snID)
	delete(clus.reportCommitters, snID)
}

func (clus *VarlogCluster) closeSNClients(t *testing.T) {
	for snID := range clus.snMCLs {
		clus.closeSNClientOf(t, snID)
	}
}

func (clus *VarlogCluster) closeReportCommitterClients(t *testing.T) {
	for snID := range clus.reportCommitters {
		clus.closeReportCommitterClientOf(t, snID)
	}
}

func (clus *VarlogCluster) closeReportCommitterClientOf(t *testing.T, snID types.StorageNodeID) {
	require.Contains(t, clus.reportCommitters, snID)
	require.NoErrorf(t, clus.reportCommitters[snID].Close(), "StorageNodeID=%d", snID)
	delete(clus.reportCommitters, snID)
}

func (clus *VarlogCluster) SNClientOf(t *testing.T, snID types.StorageNodeID) client.StorageNodeManagementClient {
	clus.muSN.Lock()
	defer clus.muSN.Unlock()

	return clus.storageNodeManagementClientOf(t, snID)
}

func (clus *VarlogCluster) ReportCommitterClientOf(t *testing.T, snID types.StorageNodeID) reportcommitter.Client {
	clus.muSN.Lock()
	defer clus.muSN.Unlock()
	require.Contains(t, clus.reportCommitters, snID)
	return clus.reportCommitters[snID]
}

func (clus *VarlogCluster) storageNodeManagementClientOf(t *testing.T, snID types.StorageNodeID) client.StorageNodeManagementClient {
	require.Contains(t, clus.snMCLs, snID)
	return clus.snMCLs[snID]
}

func (clus *VarlogCluster) ClusterID() types.ClusterID {
	return clus.clusterID
}

func (clus *VarlogCluster) ReplicationFactor() int {
	return clus.nrRep
}

func (clus *VarlogCluster) NumberOfMetadataRepositories() int {
	clus.muMR.Lock()
	defer clus.muMR.Unlock()
	return len(clus.metadataRepositories)
}

func (clus *VarlogCluster) StorageNodeIDAtIndex(t *testing.T, idx int) types.StorageNodeID {
	clus.muSN.Lock()
	defer clus.muSN.Unlock()

	require.Greater(t, len(clus.storageNodeIDs), idx)
	return clus.storageNodeIDs[idx]
}

func (clus *VarlogCluster) NumberOfStorageNodes() int {
	clus.muSN.Lock()
	defer clus.muSN.Unlock()
	return len(clus.storageNodes)
}

func (clus *VarlogCluster) NumberOfLogStreams(topicID types.TopicID) int {
	logStreamIDs := clus.topicLogStreamIDs[topicID]
	return len(logStreamIDs)
}

func (clus *VarlogCluster) NumberOfClients() int {
	clus.muCL.Lock()
	defer clus.muCL.Unlock()

	return len(clus.clients)
}

func (clus *VarlogCluster) ClientAtIndex(t *testing.T, idx int) varlog.Log {
	clus.muCL.Lock()
	defer clus.muCL.Unlock()

	require.Greater(t, len(clus.clients), idx)
	return clus.clients[idx]
}

// TODO: Use built-in refreshing mechanism in clients instead of this.
func (clus *VarlogCluster) ClientRefresh(t *testing.T) {
	clus.muCL.Lock()
	defer clus.muCL.Unlock()

	clus.closeClients(t)
	clus.clients = nil

	clus.initClients(t)
}

func WithTestCluster(t *testing.T, opts []Option, f func(env *VarlogCluster)) func() {
	return func() {
		env := NewVarlogCluster(t, opts...)

		defer func() {
			env.Close(t)
		}()

		f(env)
	}
}

func (clus *VarlogCluster) GetMetadata(t *testing.T) *varlogpb.MetadataDescriptor {
	clus.muMR.Lock()
	defer clus.muMR.Unlock()

	var (
		err error
		md  *varlogpb.MetadataDescriptor
	)
	require.Positive(t, len(clus.mrCLs))
	for _, mrcl := range clus.mrCLs {
		md, err = mrcl.GetMetadata(context.Background())
		if err == nil {
			break
		}
	}
	require.NoError(t, err)
	require.NotNil(t, md)

	clus.cachedMetadata = md
	return md
}

func (clus *VarlogCluster) storageNodeAddr(t *testing.T, snID types.StorageNodeID) string {
	require.Contains(t, clus.snAddrs, snID)
	return clus.snAddrs[snID]
}

func (clus *VarlogCluster) AppendUncommittedLog(t *testing.T, topicID types.TopicID, lsID types.LogStreamID, data []byte) {
	clus.muSN.Lock()
	defer clus.muSN.Unlock()

	clus.muLS.Lock()
	defer clus.muLS.Unlock()

	replicas := clus.replicasOf(t, lsID)
	require.Positive(t, len(replicas))

	for _, r := range replicas {
		func() {
			snID := r.GetStorageNodeID()
			addr := clus.storageNodeAddr(t, snID)

			lastWrittenLLSN := types.InvalidLLSN
			reportCommitter := clus.reportCommitters[snID]
			rsp, err := reportCommitter.GetReport()
			require.NoError(t, err)
			found := false
			for _, report := range rsp.GetUncommitReports() {
				if report.GetLogStreamID() == lsID {
					found = true
					lastWrittenLLSN = report.GetUncommittedLLSNOffset() + types.LLSN(report.GetUncommittedLLSNLength()) - 1
					break
				}
			}
			require.True(t, found)

			ctx, cancel := context.WithCancel(context.Background())
			var wg sync.WaitGroup
			wg.Add(1)
			defer func() {
				cancel()
				wg.Wait()
			}()

			go func() {
				defer wg.Done()

				cli, err := clus.logClientManager.GetOrConnect(context.TODO(), snID, addr)
				if !assert.NoError(t, err) {
					return
				}
				/*
					defer func() {
						_ = cli.Close()
					}()
				*/

				_, err = cli.Append(ctx, topicID, lsID, [][]byte{data})
				assert.Error(t, err)
			}()

			require.Eventually(t, func() bool {
				rsp, err := reportCommitter.GetReport()
				if !assert.NoError(t, err) {
					return false
				}
				for _, report := range rsp.GetUncommitReports() {
					if report.GetLogStreamID() == lsID && report.GetUncommittedLLSNOffset()+types.LLSN(report.GetUncommittedLLSNLength())-1 == lastWrittenLLSN+1 {
						return true
					}
				}
				return false
			}, time.Second, 10*time.Millisecond)
		}()
	}
}

func (clus *VarlogCluster) CommitWithoutMR(t *testing.T, lsID types.LogStreamID,
	committedLLSNOffset types.LLSN, committedGLSNOffset types.GLSN, committedGLSNLen uint64,
	version types.Version, highWatermark types.GLSN,
) {
	clus.muSN.Lock()
	defer clus.muSN.Unlock()

	clus.muLS.Lock()
	defer clus.muLS.Unlock()

	rds := clus.replicasOf(t, lsID)
	for _, r := range rds {
		req := snpb.CommitBatchRequest{
			StorageNodeID: r.StorageNodeID,
			CommitResults: []snpb.LogStreamCommitResult{{
				LogStreamID:         lsID,
				Version:             version,
				CommittedLLSNOffset: committedLLSNOffset,
				CommittedGLSNOffset: committedGLSNOffset,
				CommittedGLSNLength: committedGLSNLen,
				HighWatermark:       highWatermark,
			}},
		}

		reportCommitter := clus.reportCommitters[r.StorageNodeID]
		err := reportCommitter.CommitBatch(req)
		require.NoError(t, err)
	}
}

func (clus *VarlogCluster) WaitCommit(t *testing.T, lsID types.LogStreamID, version types.Version) {
	clus.muSN.Lock()
	defer clus.muSN.Unlock()

	clus.muLS.Lock()
	defer clus.muLS.Unlock()

	rds := clus.replicasOf(t, lsID)
	reportCommitters := make([]reportcommitter.Client, 0, len(rds))
	for _, rd := range rds {
		reportCommitter := clus.reportCommitters[rd.GetStorageNodeID()]
		reportCommitters = append(reportCommitters, reportCommitter)
	}

	require.Eventually(t, func() bool {
		committed := 0

		for _, reporter := range reportCommitters {
			rsp, err := reporter.GetReport()
			if !assert.NoError(t, err) {
				return false
			}

			reports := rsp.GetUncommitReports()
			for _, report := range reports {
				if report.GetLogStreamID() == lsID && report.GetVersion() == version {
					committed++
					break
				}
			}
		}

		return committed == clus.nrRep
	}, vtesting.TimeoutUnitTimesFactor(10), 10*time.Millisecond)
}

func (clus *VarlogCluster) WaitSealed(t *testing.T, lsID types.LogStreamID) {
	// FIXME: do not use vms.RELOAD_INTERVAL
	require.Eventually(t, func() bool {
		vmsMeta, err := clus.vmsServer.Metadata(context.Background()) //nolint:staticcheck
		return err == nil && vmsMeta.GetLogStream(lsID) != nil
	}, mrmanager.ReloadInterval*10, 100*time.Millisecond)

	require.Eventually(t, func() bool {
		snMCLs := clus.StorageNodesManagementClients()
		sealed := 0

		for _, snMCL := range snMCLs {
			meta, err := snMCL.GetMetadata(context.TODO())
			if err != nil {
				return false
			}

			lsmeta, ok := meta.GetLogStream(lsID)
			if !ok {
				continue
			}

			fmt.Printf("WaitSealed %+v\n", lsmeta)

			if lsmeta.Status == varlogpb.LogStreamStatusSealed {
				sealed++
			}
		}

		return sealed == clus.nrRep
	}, vtesting.TimeoutUnitTimesFactor(100), 100*time.Millisecond)
}

func (clus *VarlogCluster) GetUncommittedLLSNOffset(t *testing.T, lsID types.LogStreamID) types.LLSN {
	clus.muSN.Lock()
	defer clus.muSN.Unlock()

	clus.muLS.Lock()
	defer clus.muLS.Unlock()

	rds := clus.replicasOf(t, lsID)
	reportCommitters := make([]reportcommitter.Client, 0, len(rds))
	for _, rd := range rds {
		reportCommitter := clus.reportCommitters[rd.GetStorageNodeID()]
		reportCommitters = append(reportCommitters, reportCommitter)
	}

	for _, reporter := range reportCommitters {
		rsp, err := reporter.GetReport()
		require.NoError(t, err)

		reports := rsp.GetUncommitReports()
		for _, report := range reports {
			if report.GetLogStreamID() == lsID {
				return report.UncommittedLLSNEnd()
			}
		}
	}

	require.FailNow(t, "not found")

	return types.InvalidLLSN
}

// TODO (jun): non-nullable slice of replica descriptors
func (clus *VarlogCluster) ReplicasOf(t *testing.T, lsID types.LogStreamID) []varlogpb.ReplicaDescriptor {
	clus.muLS.Lock()
	defer clus.muLS.Unlock()

	return clus.replicasOf(t, lsID)
}

func (clus *VarlogCluster) replicasOf(t *testing.T, lsID types.LogStreamID) []varlogpb.ReplicaDescriptor {
	require.Contains(t, clus.replicas, lsID)
	ret := make([]varlogpb.ReplicaDescriptor, 0, len(clus.replicas[lsID]))
	for i := 0; i < len(clus.replicas[lsID]); i++ {
		ret = append(ret, *clus.replicas[lsID][i])
	}
	return ret
}
