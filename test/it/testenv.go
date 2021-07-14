package it

import (
	"context"
	"fmt"
	"log"
	"math"
	"math/rand"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"google.golang.org/grpc/health/grpc_health_v1"

	"github.com/kakao/varlog/internal/metadata_repository"
	"github.com/kakao/varlog/internal/storagenode"
	"github.com/kakao/varlog/internal/storagenode/reportcommitter"
	"github.com/kakao/varlog/internal/vms"
	"github.com/kakao/varlog/pkg/logc"
	"github.com/kakao/varlog/pkg/mrc"
	"github.com/kakao/varlog/pkg/rpc"
	"github.com/kakao/varlog/pkg/snc"
	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/pkg/util/testutil"
	"github.com/kakao/varlog/pkg/util/testutil/ports"
	"github.com/kakao/varlog/pkg/varlog"
	"github.com/kakao/varlog/proto/snpb"
	"github.com/kakao/varlog/proto/varlogpb"
	"github.com/kakao/varlog/vtesting"
)

type VarlogCluster struct {
	config

	// metadata repository
	muMR                 sync.Mutex
	metadataRepositories []*metadata_repository.RaftMetadataRepository
	mrPeers              []string
	mrRPCEndpoints       []string
	mrIDs                []types.NodeID
	mrCLs                map[types.NodeID]mrc.MetadataRepositoryClient
	mrMCLs               map[types.NodeID]mrc.MetadataRepositoryManagementClient
	cachedMetadata       *varlogpb.MetadataDescriptor

	// storage node
	muSN             sync.Mutex
	storageNodes     map[types.StorageNodeID]*storagenode.StorageNode
	snMCLs           map[types.StorageNodeID]snc.StorageNodeManagementClient
	reportCommitters map[types.StorageNodeID]reportcommitter.Client
	volumes          map[types.StorageNodeID]storagenode.Volume
	snAddrs          map[types.StorageNodeID]string
	storageNodeIDs   []types.StorageNodeID
	nextSNID         types.StorageNodeID
	manualNextLSID   types.LogStreamID
	snWGs            map[types.StorageNodeID]*sync.WaitGroup

	// log streams
	muLS         sync.Mutex
	logStreamIDs []types.LogStreamID
	// FIXME: type of value
	replicas map[types.LogStreamID][]*varlogpb.ReplicaDescriptor

	// clients
	clients []varlog.Varlog
	muCL    sync.Mutex

	muVMS     sync.Mutex
	vmsServer vms.ClusterManager
	vmsCL     varlog.ClusterManagerClient

	portLease *ports.Lease

	rng *rand.Rand
}

func NewVarlogCluster(t *testing.T, opts ...Option) *VarlogCluster {
	cfg := newConfig(t, opts)
	clus := &VarlogCluster{
		config:               cfg,
		mrPeers:              make([]string, cfg.nrMR),
		mrRPCEndpoints:       make([]string, cfg.nrMR),
		metadataRepositories: make([]*metadata_repository.RaftMetadataRepository, cfg.nrMR),
		mrIDs:                make([]types.NodeID, cfg.nrMR),
		mrCLs:                make(map[types.NodeID]mrc.MetadataRepositoryClient),
		mrMCLs:               make(map[types.NodeID]mrc.MetadataRepositoryManagementClient),
		storageNodes:         make(map[types.StorageNodeID]*storagenode.StorageNode),
		snMCLs:               make(map[types.StorageNodeID]snc.StorageNodeManagementClient),
		volumes:              make(map[types.StorageNodeID]storagenode.Volume),
		snAddrs:              make(map[types.StorageNodeID]string),
		reportCommitters:     make(map[types.StorageNodeID]reportcommitter.Client),
		replicas:             make(map[types.LogStreamID][]*varlogpb.ReplicaDescriptor),
		snWGs:                make(map[types.StorageNodeID]*sync.WaitGroup),
		nextSNID:             types.StorageNodeID(1),
		manualNextLSID:       types.LogStreamID(math.MaxUint32),
		rng:                  rand.New(rand.NewSource(time.Now().UnixNano())),
	}

	// ports
	portLease, err := ports.ReserveWeaklyWithRetry(clus.portBase)
	require.NoError(t, err)
	clus.portLease = portLease

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
		clus.createMR(t, i, false, clus.unsafeNoWAL, false)
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

func (clus *VarlogCluster) initLS(t *testing.T) {
	for i := 0; i < clus.numLS; i++ {
		clus.AddLS(t)
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
	smlPath := fmt.Sprintf("%s/sml/%d", vtesting.TestRaftDir(), nodeID)

	require.NoError(t, os.RemoveAll(walPath))
	require.NoError(t, os.RemoveAll(snapPath))
	require.NoError(t, os.RemoveAll(smlPath))

	t.Logf("MetadataRepository was cleared: idx=%d, nid=%v, wal=%s, snap=%s, sml=%s",
		idx,
		nodeID, walPath,
		snapPath,
		smlPath,
	)
}

func (clus *VarlogCluster) createMR(t *testing.T, idx int, join, unsafeNoWal, recoverFromSML bool) {
	require.GreaterOrEqual(t, idx, 0)
	require.Less(t, idx, len(clus.metadataRepositories))

	nodeID := types.NewNodeIDFromURL(clus.mrPeers[idx])
	require.NotEqual(t, types.InvalidNodeID, nodeID)

	peers := clus.mrPeers

	var syncStorageNodes []string
	if recoverFromSML {
		for _, addr := range clus.snAddrs {
			syncStorageNodes = append(syncStorageNodes, addr)
		}

		fmt.Printf("sync %+v\n", syncStorageNodes)

		peers = nil
	}

	opts := &metadata_repository.MetadataRepositoryOptions{
		RaftOptions: metadata_repository.RaftOptions{
			Join:        join,
			UnsafeNoWal: unsafeNoWal,
			EnableSML:   unsafeNoWal,
			SnapCount:   uint64(clus.snapCount),
			RaftTick:    vtesting.TestRaftTick(),
			RaftDir:     vtesting.TestRaftDir(),
			Peers:       peers,
		},

		ClusterID:                      clus.clusterID,
		RaftAddress:                    clus.mrPeers[idx],
		RPCTimeout:                     vtesting.TimeoutAccordingToProcCnt(metadata_repository.DefaultRPCTimeout),
		NumRep:                         clus.nrRep,
		RecoverFromSML:                 recoverFromSML,
		SyncStorageNodes:               syncStorageNodes,
		RPCBindAddress:                 clus.mrRPCEndpoints[idx],
		ReporterClientFac:              clus.reporterClientFac,
		StorageNodeManagementClientFac: clus.snManagementClientFac,
		Logger:                         clus.logger,
	}

	opts.CollectorName = "nop"
	if clus.collectorName != "" {
		opts.CollectorName = clus.collectorName
	}
	opts.CollectorEndpoint = "localhost:55680"

	clus.mrIDs[idx] = nodeID
	clus.metadataRepositories[idx] = metadata_repository.NewRaftMetadataRepository(opts)

	t.Logf("MetadataRepository was created: idx=%d, nid=%v", idx, nodeID)
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
	clus.createMR(t, idx, true, clus.unsafeNoWAL, false)
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
	clus.metadataRepositories = []*metadata_repository.RaftMetadataRepository{nil}

	clus.createMR(t, idx, false, clus.unsafeNoWAL, true)
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
	clus.createMR(t, idx, false, clus.unsafeNoWAL, clus.unsafeNoWAL)
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

// Close closes all cluster MRs
func (clus *VarlogCluster) Close(t *testing.T) {
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
	}

	// mr
	for i := range clus.mrPeers {
		clus.closeMR(t, i)
	}
	require.NoError(t, os.RemoveAll(vtesting.TestRaftDir()))

	// sn
	for _, sn := range clus.storageNodes {
		sn.Close()
	}
	for _, wg := range clus.snWGs {
		wg.Wait()
	}
	clus.logStreamIDs = nil

	require.NoError(t, clus.portLease.Release())
}

func (clus *VarlogCluster) healthCheckForMR(t *testing.T, idx int) {
	endpoint := clus.mrRPCEndpoints[idx]
	require.Eventually(t, func() bool {
		conn, err := rpc.NewConn(context.TODO(), endpoint)
		if !assert.NoError(t, err) {
			return false
		}
		defer conn.Close()

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

	volumeDir := t.TempDir()
	volume, err := storagenode.NewVolume(volumeDir)
	require.NoError(t, err)

	sn, err := storagenode.New(context.TODO(),
		storagenode.WithListenAddress("127.0.0.1:0"),
		storagenode.WithClusterID(clus.clusterID),
		storagenode.WithStorageNodeID(snID),
		storagenode.WithVolumes(volumeDir),
	)
	require.NoError(t, err)

	if _, ok := clus.snWGs[snID]; !ok {
		clus.snWGs[snID] = new(sync.WaitGroup)
	}

	clus.snWGs[snID].Add(1)
	go func() {
		defer clus.snWGs[snID].Done()
		_ = sn.Run()
	}()

	log.Printf("SN.New: %v", snID)

	require.Eventually(t, func() bool {
		meta, err := sn.GetMetadata(context.Background())
		return assert.NoError(t, err) && meta.GetStorageNode().GetAddress() != ""
	}, time.Second, 10*time.Millisecond)

	log.Printf("SN(%v) GetMetadata", snID)

	snmd, err := sn.GetMetadata(context.Background())
	require.NoError(t, err)
	addr := snmd.GetStorageNode().GetAddress()

	log.Printf("SN(%v) GetMetadata: %s", snID, addr)

	_, err = clus.vmsCL.AddStorageNode(context.Background(), addr)
	require.NoError(t, err)

	log.Printf("SN(%v) AddStorageNode", snID)

	mcl, err := snc.NewManagementClient(context.Background(), clus.clusterID, addr, clus.logger)
	require.NoError(t, err)

	log.Printf("SN(%v) MCL", snID)

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

	snmd, err := clus.storageNodes[snID].GetMetadata(context.Background())
	require.NoError(t, err)

	addr := snmd.GetStorageNode().GetAddress()
	mcl, err := snc.NewManagementClient(context.Background(), clus.clusterID, addr, clus.logger)
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

	sn, err := storagenode.New(context.TODO(),
		storagenode.WithClusterID(clus.clusterID),
		storagenode.WithStorageNodeID(snID),
		storagenode.WithListenAddress(addr),
		storagenode.WithVolumes(string(volume)),
	)
	require.NoError(t, err)

	if _, ok := clus.snWGs[snID]; !ok {
		clus.snWGs[snID] = new(sync.WaitGroup)
	}

	clus.snWGs[snID].Add(1)
	go func() {
		defer clus.snWGs[snID].Done()
		_ = sn.Run()
	}()

	require.Eventually(t, func() bool {
		meta, err := sn.GetMetadata(context.Background())
		return assert.NoError(t, err) && meta.GetStorageNode().GetAddress() != ""
	}, time.Second, 10*time.Millisecond)

	clus.storageNodes[snID] = sn

	return sn
}

func (clus *VarlogCluster) AddLS(t *testing.T) types.LogStreamID {
	clus.muSN.Lock()
	defer clus.muSN.Unlock()

	clus.muLS.Lock()
	defer clus.muLS.Unlock()

	log.Println("AddLS")

	require.GreaterOrEqual(t, len(clus.storageNodes), clus.nrRep)

	rsp, err := clus.vmsCL.AddLogStream(context.Background(), nil)
	require.NoError(t, err)
	log.Printf("AddLS: AddLogStream: %+v", rsp)

	logStreamDesc := rsp.GetLogStream()
	logStreamID := logStreamDesc.GetLogStreamID()

	// FIXME: use map to store logstream and its replicas
	clus.logStreamIDs = append(clus.logStreamIDs, logStreamID)
	clus.replicas[logStreamID] = logStreamDesc.GetReplicas()
	return logStreamID
}

func (clus *VarlogCluster) UpdateLS(t *testing.T, lsID types.LogStreamID, oldsn, newsn types.StorageNodeID) {
	clus.muSN.Lock()
	defer clus.muSN.Unlock()

	clus.muLS.Lock()
	defer clus.muLS.Unlock()

	// check replicas
	require.Contains(t, clus.replicas, lsID)
	rds := clus.replicas[lsID]
	require.Condition(t, func() bool {
		for _, rd := range rds {
			if rd.GetStorageNodeID() == oldsn {
				return true
			}
		}
		return false
	})

	require.Contains(t, clus.snMCLs, newsn)
	snmd, err := clus.snMCLs[newsn].GetMetadata(context.TODO())
	require.NoError(t, err)
	path := snmd.GetStorageNode().GetStorages()[0].GetPath()

	newReplica := &varlogpb.ReplicaDescriptor{
		StorageNodeID: newsn,
		Path:          path,
	}
	oldReplica := &varlogpb.ReplicaDescriptor{
		StorageNodeID: oldsn,
	}

	_, err = clus.vmsCL.UpdateLogStream(context.Background(), lsID, oldReplica, newReplica)
	require.NoError(t, err)

	// update replicas
	for i := range rds {
		if rds[i].GetStorageNodeID() == oldsn {
			rds[i] = newReplica
		}
	}
}

func (clus *VarlogCluster) AddLSWithoutMR(t *testing.T) types.LogStreamID {
	clus.muSN.Lock()
	defer clus.muSN.Unlock()

	clus.muLS.Lock()
	defer clus.muLS.Unlock()

	require.GreaterOrEqual(t, len(clus.storageNodes), clus.nrRep)

	lsID := clus.manualNextLSID
	clus.manualNextLSID--

	rds := make([]*varlogpb.ReplicaDescriptor, 0, clus.nrRep)
	replicas := make([]snpb.Replica, 0, clus.nrRep)
	for idx := range clus.rng.Perm(len(clus.storageNodeIDs))[:clus.nrRep] {
		snID := clus.storageNodeIDs[idx]
		replicas = append(replicas, snpb.Replica{
			StorageNodeID: snID,
			LogStreamID:   lsID,
			Address:       clus.snAddrs[snID],
		})

		snmd, err := clus.storageNodeManagementClientOf(t, snID).GetMetadata(context.Background())
		require.NoError(t, err)
		path := snmd.GetStorageNode().GetStorages()[0].GetPath()
		rds = append(rds, &varlogpb.ReplicaDescriptor{
			StorageNodeID: snID,
			Path:          path,
		})
	}

	for _, rd := range rds {
		snID := rd.StorageNodeID
		path := rd.Path
		require.NoError(t, clus.storageNodeManagementClientOf(t, snID).AddLogStream(
			context.Background(),
			lsID,
			path,
		))

		status, _, err := clus.storageNodeManagementClientOf(t, snID).Seal(context.Background(), lsID, types.InvalidGLSN)
		require.NoError(t, err)
		require.Equal(t, varlogpb.LogStreamStatusSealed, status)

		require.NoError(t, clus.storageNodeManagementClientOf(t, snID).Unseal(context.Background(), lsID, replicas))
	}

	// FIXME: use map to store logstream and its replicas
	clus.logStreamIDs = append(clus.logStreamIDs, lsID)
	clus.replicas[lsID] = rds
	t.Logf("AddLS without MR: lsid=%d, replicas=%+v", lsID, replicas)
	return lsID
}

func (clus *VarlogCluster) AddLSIncomplete(t *testing.T) types.LogStreamID {
	clus.muSN.Lock()
	defer clus.muSN.Unlock()

	clus.muLS.Lock()
	defer clus.muLS.Unlock()

	log.Println("AddLS Incomplete")

	require.GreaterOrEqual(t, len(clus.storageNodes), clus.nrRep)

	lsID := clus.manualNextLSID
	clus.manualNextLSID--

	replicas := make([]snpb.Replica, 0, clus.nrRep-1)
	for idx := range clus.rng.Perm(len(clus.storageNodeIDs))[:clus.nrRep-1] {
		snID := clus.storageNodeIDs[idx]
		replicas = append(replicas, snpb.Replica{
			StorageNodeID: snID,
			LogStreamID:   lsID,
			Address:       clus.snAddrs[snID],
		})
	}

	for _, replica := range replicas {
		snID := replica.StorageNodeID
		snmd, err := clus.storageNodeManagementClientOf(t, snID).GetMetadata(context.Background())
		require.NoError(t, err)
		path := snmd.GetStorageNode().GetStorages()[0].GetPath()

		require.NoError(t, clus.storageNodeManagementClientOf(t, snID).AddLogStream(
			context.Background(),
			lsID,
			path,
		))
	}
	t.Logf("AddLS incompletely: lsid=%d, replicas=%+v", lsID, replicas)
	return lsID
}

func (clus *VarlogCluster) UpdateLSWithoutMR(t *testing.T, logStreamID types.LogStreamID, storageNodeID types.StorageNodeID, clear bool) {
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

			_, _, err = clus.snMCLs[snid].Seal(context.Background(), logStreamID, lsmd.HighWatermark)
			require.NoError(t, err)
		}
		return true
	}, 3*time.Second, 100*time.Millisecond)

	victim := replicas[0]

	meta, err := clus.snMCLs[storageNodeID].GetMetadata(context.Background())
	require.NoError(t, err)

	path := meta.GetStorageNode().GetStorages()[0].GetPath()

	require.NoError(t, clus.snMCLs[storageNodeID].AddLogStream(context.Background(), logStreamID, path))

	replicas[0] = &varlogpb.ReplicaDescriptor{
		StorageNodeID: storageNodeID,
		Path:          path,
	}

	clus.replicas[logStreamID] = replicas

	if clear {
		require.NoError(t, clus.snMCLs[victim.StorageNodeID].RemoveLogStream(context.Background(), logStreamID))
	}
}

func (clus *VarlogCluster) UnsealWithoutMR(t *testing.T, logStreamID types.LogStreamID, expectedHighWatermark types.GLSN) {
	clus.muSN.Lock()
	defer clus.muSN.Unlock()

	clus.muLS.Lock()
	defer clus.muLS.Unlock()

	log.Println("Unseal without MR")

	rds, ok := clus.replicas[logStreamID]
	require.Equal(t, ok, true)

	replicas := make([]snpb.Replica, 0, len(rds))
	for _, rd := range rds {
		snid := rd.GetStorageNodeID()
		require.Contains(t, clus.snMCLs, snid)

		snmd, err := clus.snMCLs[snid].GetMetadata(context.Background())
		require.NoError(t, err)

		lsmd, ok := snmd.GetLogStream(logStreamID)
		require.True(t, ok)
		require.NotEqual(t, lsmd.GetStatus(), varlogpb.LogStreamStatusRunning)

		require.Equal(t, expectedHighWatermark, lsmd.GetHighWatermark())

		replicas = append(replicas, snpb.Replica{
			StorageNodeID: snid,
			LogStreamID:   logStreamID,
		})
	}

	for _, rd := range rds {
		snid := rd.GetStorageNodeID()

		snmd, err := clus.snMCLs[snid].GetMetadata(context.Background())
		require.NoError(t, err)

		lsmd, ok := snmd.GetLogStream(logStreamID)
		require.True(t, ok)

		if lsmd.GetStatus() == varlogpb.LogStreamStatusSealing {
			_, _, err = clus.snMCLs[snid].Seal(context.Background(), logStreamID, types.InvalidGLSN)
			require.NoError(t, err)
		}

		err = clus.snMCLs[snid].Unseal(context.Background(), logStreamID, replicas)
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

func (clus *VarlogCluster) StorageNodesManagementClients() map[types.StorageNodeID]snc.StorageNodeManagementClient {
	clus.muSN.Lock()
	defer clus.muSN.Unlock()

	ret := make(map[types.StorageNodeID]snc.StorageNodeManagementClient, len(clus.snMCLs))
	for id, sn := range clus.snMCLs {
		ret[id] = sn
	}

	return ret
}

func (clus *VarlogCluster) CloseSN(t *testing.T, snID types.StorageNodeID) {
	clus.muSN.Lock()
	defer clus.muSN.Unlock()

	require.Contains(t, clus.storageNodes, snID)
	clus.storageNodes[snID].Close()
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

	cl, err := mrc.NewMetadataRepositoryClientFromRpcConn(rpcConn)
	require.NoError(t, err)

	mcl, err := mrc.NewMetadataRepositoryManagementClientFromRpcConn(rpcConn)
	require.NoError(t, err)

	clus.mrCLs[id] = cl
	clus.mrMCLs[id] = mcl
}

func (clus *VarlogCluster) MetadataRepositories() []*metadata_repository.RaftMetadataRepository {
	clus.muMR.Lock()
	defer clus.muMR.Unlock()

	ret := make([]*metadata_repository.RaftMetadataRepository, len(clus.metadataRepositories))
	for i, mr := range clus.metadataRepositories {
		ret[i] = mr
	}
	return ret
}

func (clus *VarlogCluster) GetMR(t *testing.T) *metadata_repository.RaftMetadataRepository {
	return clus.GetMRByIndex(t, 0)
}

func (clus *VarlogCluster) GetMRByIndex(t *testing.T, idx int) *metadata_repository.RaftMetadataRepository {
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

func (clus *VarlogCluster) LookupMR(nodeID types.NodeID) (*metadata_repository.RaftMetadataRepository, bool) {
	clus.muMR.Lock()
	defer clus.muMR.Unlock()

	for idx, mrID := range clus.mrIDs {
		if nodeID == mrID {
			return clus.metadataRepositories[idx], true
		}
	}
	return nil, false
}

func (clus *VarlogCluster) GetVMS() vms.ClusterManager {
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

func (clus *VarlogCluster) newClient(t *testing.T) varlog.Varlog {
	cl, err := varlog.Open(context.Background(), clus.clusterID, clus.mrRPCEndpoints)
	require.NoError(t, err)
	return cl
}

func (clus *VarlogCluster) PrimaryStorageNodeIDOf(t *testing.T, lsID types.LogStreamID) types.StorageNodeID {
	return clus.getSN(t, lsID, 0).StorageNodeID()
}

func (clus *VarlogCluster) BackupStorageNodeIDOf(t *testing.T, lsID types.LogStreamID) types.StorageNodeID {
	idx := 1
	if clus.nrRep > 2 {
		idx += rand.Intn(clus.nrRep - 1)
	}
	return clus.getSN(t, lsID, idx).StorageNodeID()
}

func (clus *VarlogCluster) NewLogIOClient(t *testing.T, lsID types.LogStreamID) logc.LogIOClient {
	snID := clus.PrimaryStorageNodeIDOf(t, lsID)
	snmd, err := clus.SNClientOf(t, snID).GetMetadata(context.Background())
	require.NoError(t, err)

	cl, err := logc.NewLogIOClient(context.TODO(), snmd.GetStorageNode().GetAddress())
	require.NoError(t, err)
	return cl
}

func (clus *VarlogCluster) initVMS(t *testing.T) {
	clus.VMSOpts.ListenAddress = fmt.Sprintf("127.0.0.1:%d", clus.portLease.Base()+clus.vmsPortOffset)
	clus.VMSOpts.ClusterID = clus.clusterID
	clus.VMSOpts.MetadataRepositoryAddresses = clus.mrRPCEndpoints
	clus.VMSOpts.ReplicationFactor = uint(clus.nrRep)

	cm, err := vms.NewClusterManager(context.Background(), clus.VMSOpts)
	require.NoError(t, err)

	require.NoError(t, cm.Run())

	require.Eventually(t, func() bool {
		rpcConn, err := rpc.NewConn(context.Background(), clus.VMSOpts.ListenAddress)
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
	cmcli, err := varlog.NewClusterManagerClient(context.TODO(), addr)
	require.NoError(t, err)
	clus.vmsCL = cmcli
}

func (clus *VarlogCluster) StartVMS(t *testing.T) {
	clus.muVMS.Lock()
	defer clus.muVMS.Unlock()

	require.Nil(t, clus.vmsServer)
	clus.initVMS(t)
}

func (clus *VarlogCluster) GetVMSClient(t *testing.T) varlog.ClusterManagerClient {
	clus.muVMS.Lock()
	defer clus.muVMS.Unlock()

	if clus.vmsCL == nil {
		clus.initVMSClient(t)
	}

	return clus.vmsCL
}

func (clus *VarlogCluster) Logger() *zap.Logger {
	return clus.logger
}

func (clus *VarlogCluster) LogStreamIDs() []types.LogStreamID {
	clus.muLS.Lock()
	defer clus.muLS.Unlock()

	ret := make([]types.LogStreamID, len(clus.logStreamIDs))
	copy(ret, clus.logStreamIDs)
	return ret
}

func (clus *VarlogCluster) LogStreamID(t *testing.T, idx int) types.LogStreamID {
	clus.muLS.Lock()
	defer clus.muLS.Unlock()

	require.Greater(t, len(clus.logStreamIDs), idx)
	return clus.logStreamIDs[idx]
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

func (clus *VarlogCluster) SNClientOf(t *testing.T, snID types.StorageNodeID) snc.StorageNodeManagementClient {
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

func (clus *VarlogCluster) storageNodeManagementClientOf(t *testing.T, snID types.StorageNodeID) snc.StorageNodeManagementClient {
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

func (clus *VarlogCluster) NumberOfLogStreams() int {
	return len(clus.logStreamIDs)
}

func (clus *VarlogCluster) NumberOfClients() int {
	clus.muCL.Lock()
	defer clus.muCL.Unlock()

	return len(clus.clients)
}

func (clus *VarlogCluster) ClientAtIndex(t *testing.T, idx int) varlog.Varlog {
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
			testutil.GC()
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

func (clus *VarlogCluster) getCachedMetadata() *varlogpb.MetadataDescriptor {
	clus.muMR.Lock()
	defer clus.muMR.Unlock()

	return clus.cachedMetadata
}

func (clus *VarlogCluster) AppendUncommittedLog(t *testing.T, lsID types.LogStreamID, data []byte) {
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
			rsp, err := reportCommitter.GetReport(context.Background())
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

				cli, err := logc.NewLogIOClient(context.TODO(), addr)
				if !assert.NoError(t, err) {
					return
				}
				defer cli.Close()

				_, err = cli.Append(ctx, lsID, data)
				assert.Error(t, err)
			}()

			require.Eventually(t, func() bool {
				rsp, err := reportCommitter.GetReport(ctx)
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
	prevHighWatermark, highWatermark types.GLSN) {

	cr := snpb.LogStreamCommitResult{
		LogStreamID:         lsID,
		CommittedLLSNOffset: committedLLSNOffset,
		CommittedGLSNOffset: committedGLSNOffset,
		CommittedGLSNLength: committedGLSNLen,
		PrevHighWatermark:   prevHighWatermark,
		HighWatermark:       highWatermark,
	}

	clus.muSN.Lock()
	defer clus.muSN.Unlock()

	clus.muLS.Lock()
	defer clus.muLS.Unlock()

	rds := clus.replicasOf(t, lsID)
	for _, r := range rds {
		cr := &snpb.CommitRequest{
			StorageNodeID: r.StorageNodeID,
			CommitResults: []snpb.LogStreamCommitResult{cr},
		}

		reportCommitter := clus.reportCommitters[r.StorageNodeID]
		err := reportCommitter.Commit(context.TODO(), cr)
		require.NoError(t, err)
	}
}

func (clus *VarlogCluster) WaitCommit(t *testing.T, lsID types.LogStreamID, highWatermark types.GLSN) {
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
			rsp, err := reporter.GetReport(context.Background())
			if !assert.NoError(t, err) {
				return false
			}

			reports := rsp.GetUncommitReports()
			for _, report := range reports {
				if report.GetLogStreamID() == lsID && report.GetHighWatermark() == highWatermark {
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
		vmsMeta, err := clus.vmsServer.Metadata(context.Background())
		return err == nil && vmsMeta.GetLogStream(lsID) != nil
	}, vms.RELOAD_INTERVAL*10, 100*time.Millisecond)

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
		rsp, err := reporter.GetReport(context.Background())
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
