package test

import (
	"context"
	"fmt"
	"log"
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
	"github.com/kakao/varlog/internal/vms"
	"github.com/kakao/varlog/pkg/logc"
	"github.com/kakao/varlog/pkg/mrc"
	"github.com/kakao/varlog/pkg/rpc"
	"github.com/kakao/varlog/pkg/snc"
	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/pkg/util/testutil"
	"github.com/kakao/varlog/pkg/util/testutil/ports"
	"github.com/kakao/varlog/pkg/varlog"
	"github.com/kakao/varlog/proto/varlogpb"
	"github.com/kakao/varlog/vtesting"
)

const varlogClusterPortBase = 10000
const varlogClusterManagerPortBase = 999

type VarlogClusterOptions struct {
	NrRep                 int
	NrMR                  int
	SnapCount             int
	CollectorName         string
	UnsafeNoWal           bool
	ReporterClientFac     metadata_repository.ReporterClientFactory
	SNManagementClientFac metadata_repository.StorageNodeManagementClientFactory
	VMSOpts               *vms.Options
}

type VarlogCluster struct {
	VarlogClusterOptions

	muMRs                sync.Mutex
	metadataRepositories []*metadata_repository.RaftMetadataRepository
	MRPeers              []string
	MRRPCEndpoints       []string
	MRIDs                []types.NodeID
	mrCLs                map[types.NodeID]mrc.MetadataRepositoryClient
	mrMCLs               map[types.NodeID]mrc.MetadataRepositoryManagementClient

	muSNs          sync.Mutex
	storageNodes   map[types.StorageNodeID]*storagenode.StorageNode
	snMCLs         map[types.StorageNodeID]snc.StorageNodeManagementClient
	volumes        map[types.StorageNodeID]storagenode.Volume
	snRPCEndpoints map[types.StorageNodeID]string
	snidGen        types.StorageNodeID
	snWG           sync.WaitGroup

	vmsServer vms.ClusterManager
	vmsCL     varlog.ClusterManagerClient

	lsID               types.LogStreamID
	issuedLogStreamIDs []types.LogStreamID
	ClusterID          types.ClusterID
	logger             *zap.Logger

	portLease *ports.Lease
}

func NewVarlogCluster(t *testing.T, opts VarlogClusterOptions) *VarlogCluster {
	portLease, err := ports.ReserveWeaklyWithRetry(varlogClusterPortBase)
	require.NoError(t, err)

	mrPeers := make([]string, opts.NrMR)
	mrRPCEndpoints := make([]string, opts.NrMR)
	MRs := make([]*metadata_repository.RaftMetadataRepository, opts.NrMR)
	mrIDs := make([]types.NodeID, opts.NrMR)

	for i := range mrPeers {
		raftPort := i*2 + portLease.Base()
		rpcPort := i*2 + 1 + portLease.Base()

		mrPeers[i] = fmt.Sprintf("http://127.0.0.1:%d", raftPort)
		mrRPCEndpoints[i] = fmt.Sprintf("127.0.0.1:%d", rpcPort)
	}

	clus := &VarlogCluster{
		VarlogClusterOptions: opts,
		logger:               zap.L(),
		MRPeers:              mrPeers,
		MRRPCEndpoints:       mrRPCEndpoints,
		MRIDs:                mrIDs,
		metadataRepositories: MRs,
		mrCLs:                make(map[types.NodeID]mrc.MetadataRepositoryClient),
		mrMCLs:               make(map[types.NodeID]mrc.MetadataRepositoryManagementClient),
		storageNodes:         make(map[types.StorageNodeID]*storagenode.StorageNode),
		snMCLs:               make(map[types.StorageNodeID]snc.StorageNodeManagementClient),
		volumes:              make(map[types.StorageNodeID]storagenode.Volume),
		snRPCEndpoints:       make(map[types.StorageNodeID]string),
		ClusterID:            types.ClusterID(1),
		portLease:            portLease,
	}

	for i := range clus.MRPeers {
		clus.clearMR(t, i)
		clus.createMR(t, i, false, clus.UnsafeNoWal, false)
	}

	return clus
}

func (clus *VarlogCluster) clearMR(t *testing.T, idx int) {
	require.GreaterOrEqual(t, idx, 0)
	require.Less(t, idx, len(clus.metadataRepositories))

	nodeID := types.NewNodeIDFromURL(clus.MRPeers[idx])
	require.NotEqual(t, types.InvalidNodeID, nodeID)

	require.NoError(t, os.RemoveAll(fmt.Sprintf("%s/wal/%d", vtesting.TestRaftDir(), nodeID)))
	require.NoError(t, os.RemoveAll(fmt.Sprintf("%s/snap/%d", vtesting.TestRaftDir(), nodeID)))
	require.NoError(t, os.RemoveAll(fmt.Sprintf("%s/sml/%d", vtesting.TestRaftDir(), nodeID)))
}

func (clus *VarlogCluster) createMR(t *testing.T, idx int, join, unsafeNoWal, recoverFromSML bool) {
	require.GreaterOrEqual(t, idx, 0)
	require.Less(t, idx, len(clus.metadataRepositories))

	nodeID := types.NewNodeIDFromURL(clus.MRPeers[idx])
	require.NotEqual(t, types.InvalidNodeID, nodeID)

	opts := &metadata_repository.MetadataRepositoryOptions{
		RaftOptions: metadata_repository.RaftOptions{
			Join:        join,
			UnsafeNoWal: unsafeNoWal,
			EnableSML:   unsafeNoWal,
			SnapCount:   uint64(clus.SnapCount),
			RaftTick:    vtesting.TestRaftTick(),
			RaftDir:     vtesting.TestRaftDir(),
			Peers:       clus.MRPeers,
		},

		ClusterID:                      clus.ClusterID,
		RaftAddress:                    clus.MRPeers[idx],
		RPCTimeout:                     vtesting.TimeoutAccordingToProcCnt(metadata_repository.DefaultRPCTimeout),
		NumRep:                         clus.NrRep,
		RecoverFromSML:                 recoverFromSML,
		RPCBindAddress:                 clus.MRRPCEndpoints[idx],
		ReporterClientFac:              clus.ReporterClientFac,
		StorageNodeManagementClientFac: clus.SNManagementClientFac,
		Logger:                         clus.logger,
	}

	opts.CollectorName = "nop"
	if clus.CollectorName != "" {
		opts.CollectorName = clus.CollectorName
	}
	opts.CollectorEndpoint = "localhost:55680"

	clus.MRIDs[idx] = nodeID
	clus.metadataRepositories[idx] = metadata_repository.NewRaftMetadataRepository(opts)
}

func (clus *VarlogCluster) AppendMR(t *testing.T) {
	clus.muMRs.Lock()
	defer clus.muMRs.Unlock()

	idx := len(clus.metadataRepositories)
	raftPort := 2*idx + clus.portLease.Base()
	rpcPort := 2*idx + 1 + clus.portLease.Base()
	clus.MRPeers = append(clus.MRPeers, fmt.Sprintf("http://127.0.0.1:%d", raftPort))
	clus.MRRPCEndpoints = append(clus.MRRPCEndpoints, fmt.Sprintf("127.0.0.1:%d", rpcPort))
	clus.MRIDs = append(clus.MRIDs, types.InvalidNodeID)
	clus.metadataRepositories = append(clus.metadataRepositories, nil)

	clus.clearMR(t, idx)

	clus.createMR(t, idx, true, clus.UnsafeNoWal, false)
}

func (clus *VarlogCluster) StartMR(t *testing.T, idx int) {
	require.GreaterOrEqual(t, idx, 0)
	require.Less(t, idx, len(clus.metadataRepositories))
	clus.metadataRepositories[idx].Run()
}

func (clus *VarlogCluster) Start(t *testing.T) {
	clus.logger.Info("cluster start")
	for i := range clus.metadataRepositories {
		clus.StartMR(t, i)
	}
	clus.logger.Info("cluster complete")
}

func (clus *VarlogCluster) StopMR(t *testing.T, idx int) {
	require.GreaterOrEqual(t, idx, 0)
	require.Less(t, idx, len(clus.metadataRepositories))
	require.NoError(t, clus.metadataRepositories[idx].Close())
}

func (clus *VarlogCluster) RestartMR(t *testing.T, idx int) {
	require.GreaterOrEqual(t, idx, 0)
	require.Less(t, idx, len(clus.metadataRepositories))

	clus.StopMR(t, idx)
	clus.createMR(t, idx, false, clus.UnsafeNoWal, clus.UnsafeNoWal)
	clus.StartMR(t, idx)
}

func (clus *VarlogCluster) CloseMR(t *testing.T, idx int) {
	require.GreaterOrEqual(t, idx, 0)
	require.Less(t, idx, len(clus.metadataRepositories))

	clus.StopMR(t, idx)
	clus.clearMR(t, idx)

	clus.logger.Info("cluster node stop", zap.Int("idx", idx))
}

// Close closes all cluster MRs
func (clus *VarlogCluster) Close(t *testing.T) {
	// VMS
	if clus.vmsServer != nil {
		require.NoError(t, clus.vmsServer.Close())
	}

	// MR
	for i := range clus.MRPeers {
		clus.CloseMR(t, i)
	}
	require.NoError(t, os.RemoveAll(vtesting.TestRaftDir()))

	// SN
	for _, sn := range clus.storageNodes {
		sn.Close()
	}
	clus.snWG.Wait()
	clus.issuedLogStreamIDs = nil

	require.NoError(t, clus.portLease.Release())
}

func (clus *VarlogCluster) HealthCheck(t *testing.T) bool {
	healthCheck := func(endpoint string) bool {
		conn, err := rpc.NewConn(context.TODO(), endpoint)
		if !assert.NoError(t, err) {
			return false
		}
		defer conn.Close()

		healthClient := grpc_health_v1.NewHealthClient(conn.Conn)
		rsp, err := healthClient.Check(context.Background(), &grpc_health_v1.HealthCheckRequest{})
		return err == nil && rsp.GetStatus() == grpc_health_v1.HealthCheckResponse_SERVING
	}

	for _, endpoint := range clus.MRRPCEndpoints {
		if !healthCheck(endpoint) {
			return false
		}
	}

	return true
}

func (clus *VarlogCluster) Leader() int {
	leader := -1
	for i, n := range clus.metadataRepositories {
		cinfo, _ := n.GetClusterInfo(context.TODO(), clus.ClusterID)
		if cinfo.GetLeader() != types.InvalidNodeID && clus.MRIDs[i] == cinfo.GetLeader() {
			leader = i
			break
		}
	}

	return leader
}

func (clus *VarlogCluster) LeaderElected() bool {
	for _, n := range clus.metadataRepositories {
		if cinfo, _ := n.GetClusterInfo(context.TODO(), clus.ClusterID); cinfo.GetLeader() == types.InvalidNodeID {
			return false
		}
	}

	return true
}

func (clus *VarlogCluster) LeaderFail(t *testing.T) bool {
	leader := clus.Leader()
	if leader < 0 {
		return false
	}

	clus.StopMR(t, leader)
	return true
}

func (clus *VarlogCluster) AddSN(t *testing.T) types.StorageNodeID {
	clus.muSNs.Lock()
	defer clus.muSNs.Unlock()

	snID := clus.snidGen
	clus.snidGen++

	volume, err := storagenode.NewVolume(t.TempDir())
	require.NoError(t, err)

	sn, err := storagenode.New(context.TODO(),
		storagenode.WithListenAddress("127.0.0.1:0"),
		storagenode.WithClusterID(clus.ClusterID),
		storagenode.WithStorageNodeID(snID),
		storagenode.WithVolumes(volume),
	)
	require.NoError(t, err)

	clus.snWG.Add(1)
	go func() {
		defer clus.snWG.Done()
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

	mcl, err := snc.NewManagementClient(context.Background(), clus.ClusterID, addr, clus.logger)
	require.NoError(t, err)

	log.Printf("SN(%v) MCL", snID)

	clus.storageNodes[snID] = sn
	clus.volumes[snID] = volume
	clus.snRPCEndpoints[snID] = addr
	clus.snMCLs[snID] = mcl
	return snID
}

func (clus *VarlogCluster) RecoverSN(t *testing.T, snID types.StorageNodeID) *storagenode.StorageNode {
	clus.muSNs.Lock()
	defer clus.muSNs.Unlock()

	// TODO: make sure that sn is stopped by this framework.

	require.Contains(t, clus.volumes, snID)
	require.Contains(t, clus.snRPCEndpoints, snID)
	volume := clus.volumes[snID]
	addr := clus.snRPCEndpoints[snID]

	sn, err := storagenode.New(context.TODO(),
		storagenode.WithClusterID(clus.ClusterID),
		storagenode.WithStorageNodeID(snID),
		storagenode.WithListenAddress(addr),
		storagenode.WithVolumes(volume),
	)
	require.NoError(t, err)

	clus.snWG.Add(1)
	go func() {
		defer clus.snWG.Done()
		_ = sn.Run()
	}()

	clus.storageNodes[snID] = sn

	return sn
}

func (clus *VarlogCluster) AddLS(t *testing.T) types.LogStreamID {
	clus.muSNs.Lock()
	defer clus.muSNs.Unlock()

	log.Println("AddLS")

	require.GreaterOrEqual(t, len(clus.storageNodes), clus.NrRep)

	rsp, err := clus.vmsCL.AddLogStream(context.Background(), nil)
	require.NoError(t, err)
	log.Printf("AddLS: AddLogStream: %+v", rsp)

	logStreamDesc := rsp.GetLogStream()
	logStreamID := logStreamDesc.GetLogStreamID()

	// FIXME: Can ReplicaDescriptor have address of storage node?
	require.Eventually(t, func() bool {
		for _, rd := range logStreamDesc.GetReplicas() {
			snid := rd.GetStorageNodeID()
			if !assert.Contains(t, clus.snMCLs, snid) {
				return false
			}

			snmd, err := clus.snMCLs[snid].GetMetadata(context.Background())
			require.NoError(t, err)

			lsmd, ok := snmd.GetLogStream(logStreamID)
			require.True(t, ok)

			if lsmd.GetStatus() != varlogpb.LogStreamStatusSealed {
				return false
			}
		}
		return true
	}, 3*time.Second, 100*time.Millisecond)

	log.Printf("AddLS: AddLogStream (%v): Sealed", logStreamID)

	require.Eventually(t, func() bool {
		// FIXME: Should the Unseal is called repeatedly?
		_, err := clus.vmsCL.Unseal(context.Background(), logStreamID)
		if !assert.NoError(t, err) {
			return false
		}

		for _, rd := range logStreamDesc.GetReplicas() {
			snid := rd.GetStorageNodeID()
			if !assert.Contains(t, clus.snMCLs, snid) {
				return false
			}

			snmd, err := clus.snMCLs[snid].GetMetadata(context.Background())
			if !assert.NoError(t, err) {
				return false
			}

			lsmd, ok := snmd.GetLogStream(logStreamID)
			if !assert.True(t, ok) || !lsmd.GetStatus().Running() {
				return false
			}
		}

		// TODO: use RPC API to get metadata
		md, err := clus.vmsServer.Metadata(context.Background())
		if !assert.NoError(t, err) {
			return false
		}
		lsd, err := md.HaveLogStream(logStreamID)
		return assert.NoError(t, err) && lsd.GetStatus().Running()
	}, 3*time.Second, 100*time.Millisecond)

	log.Printf("AddLS: AddLogStream (%v): Running", logStreamID)

	// FIXME: use map to store logstream and its replicas
	clus.issuedLogStreamIDs = append(clus.issuedLogStreamIDs, logStreamID)
	return logStreamID
}

func (clus *VarlogCluster) UpdateLS(t *testing.T, lsID types.LogStreamID, oldsn, newsn types.StorageNodeID) {
	clus.muSNs.Lock()
	defer clus.muSNs.Unlock()

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
}

func (clus *VarlogCluster) StorageNodes() map[types.StorageNodeID]*storagenode.StorageNode {
	clus.muSNs.Lock()
	defer clus.muSNs.Unlock()
	ret := make(map[types.StorageNodeID]*storagenode.StorageNode, len(clus.storageNodes))
	for id, sn := range clus.storageNodes {
		ret[id] = sn
	}
	return ret
}

func (clus *VarlogCluster) CloseSN(t *testing.T, snID types.StorageNodeID) {
	clus.muSNs.Lock()
	defer clus.muSNs.Unlock()

	require.Contains(t, clus.storageNodes, snID)
	clus.storageNodes[snID].Close()
}

func (clus *VarlogCluster) LookupSN(t *testing.T, snID types.StorageNodeID) *storagenode.StorageNode {
	clus.muSNs.Lock()
	defer clus.muSNs.Unlock()
	require.Contains(t, clus.storageNodes, snID)
	return clus.storageNodes[snID]
}

func (clus *VarlogCluster) connectMR(t *testing.T) {
	clus.muMRs.Lock()
	defer clus.muMRs.Unlock()

	for idx := range clus.metadataRepositories {
		clus.createMRClient(t, idx)
	}
}

func (clus *VarlogCluster) createMRClient(t *testing.T, idx int) {
	id := clus.MRIDs[idx]
	addr := clus.MRRPCEndpoints[idx]
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
	clus.muMRs.Lock()
	defer clus.muMRs.Unlock()

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
	clus.muMRs.Lock()
	defer clus.muMRs.Unlock()

	require.Greater(t, len(clus.metadataRepositories), idx)
	return clus.metadataRepositories[idx]
}

func (clus *VarlogCluster) MRClient(t *testing.T, idx int) mrc.MetadataRepositoryClient {
	clus.muMRs.Lock()
	defer clus.muMRs.Unlock()

	require.Greater(t, len(clus.MRIDs), idx)
	id := clus.MRIDs[idx]
	require.Contains(t, clus.mrCLs, id)
	return clus.mrCLs[id]
}

func (clus *VarlogCluster) LookupMR(nodeID types.NodeID) (*metadata_repository.RaftMetadataRepository, bool) {
	clus.muMRs.Lock()
	defer clus.muMRs.Unlock()
	for idx, mrID := range clus.MRIDs {
		if nodeID == mrID {
			return clus.metadataRepositories[idx], true
		}
	}
	return nil, false
}

func (clus *VarlogCluster) GetVMS() vms.ClusterManager {
	return clus.vmsServer
}

func (clus *VarlogCluster) getSN(t *testing.T, lsID types.LogStreamID, idx int) *storagenode.StorageNode {
	// FIXME: extract below codes to method
	clus.muMRs.Lock()
	defer clus.muMRs.Unlock()

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

func (clus *VarlogCluster) GetPrimarySN(t *testing.T, lsID types.LogStreamID) *storagenode.StorageNode {
	return clus.getSN(t, lsID, 0)
}

func (clus *VarlogCluster) GetBackupSN(t *testing.T, lsID types.LogStreamID /*, idx int*/) *storagenode.StorageNode {
	idx := rand.Intn(clus.NrRep) + 1
	return clus.getSN(t, lsID, idx)
}

func (clus *VarlogCluster) NewLogIOClient(t *testing.T, lsID types.LogStreamID) logc.LogIOClient {
	sn := clus.GetPrimarySN(t, lsID)

	snmd, err := sn.GetMetadata(context.TODO())
	require.NoError(t, err)

	cl, err := logc.NewLogIOClient(context.TODO(), snmd.GetStorageNode().GetAddress())
	require.NoError(t, err)
	return cl
}

func (clus *VarlogCluster) RunClusterManager(t *testing.T, mrAddrs []string, opts *vms.Options) vms.ClusterManager {
	require.Positive(t, clus.VarlogClusterOptions.NrRep)

	if opts == nil {
		vmOpts := vms.DefaultOptions()
		opts = &vmOpts
		opts.Logger = clus.logger
	}

	opts.ListenAddress = fmt.Sprintf("127.0.0.1:%d", clus.portLease.Base()+varlogClusterManagerPortBase)
	opts.ClusterID = clus.ClusterID
	opts.MetadataRepositoryAddresses = mrAddrs
	opts.ReplicationFactor = uint(clus.VarlogClusterOptions.NrRep)

	cm, err := vms.NewClusterManager(context.Background(), opts)
	require.NoError(t, err)

	require.NoError(t, cm.Run())

	return cm
}

func (clus *VarlogCluster) NewClusterManagerClient(t *testing.T) varlog.ClusterManagerClient {
	addr := clus.vmsServer.Address()
	cmcli, err := varlog.NewClusterManagerClient(context.TODO(), addr)
	require.NoError(t, err)
	return cmcli
}

func (clus *VarlogCluster) GetClusterManagerClient() varlog.ClusterManagerClient {
	return clus.vmsCL
}

func (clus *VarlogCluster) Logger() *zap.Logger {
	return clus.logger
}

func (clus *VarlogCluster) IssuedLogStreams() []types.LogStreamID {
	ret := make([]types.LogStreamID, len(clus.issuedLogStreamIDs))
	copy(ret, clus.issuedLogStreamIDs)
	return ret
}

func (clus *VarlogCluster) closeMRClients(t *testing.T) {
	clus.muMRs.Lock()
	defer clus.muMRs.Unlock()

	for id, cl := range clus.mrCLs {
		require.NoErrorf(t, cl.Close(), "NodeID=%d", id)
		delete(clus.mrCLs, id)
	}

	for id, mcl := range clus.mrMCLs {
		require.NoErrorf(t, mcl.Close(), "NodeID=%d", id)
		delete(clus.mrMCLs, id)
	}
}

func (clus *VarlogCluster) closeSNClients(t *testing.T) {
	clus.muSNs.Lock()
	defer clus.muSNs.Unlock()

	for id, cl := range clus.snMCLs {
		require.NoErrorf(t, cl.Close(), "StorageNodeID=%d", id)
		delete(clus.snMCLs, id)
	}
}

func WithTestCluster(t *testing.T, opts VarlogClusterOptions, f func(env *VarlogCluster)) func() {
	return func() {
		env := NewVarlogCluster(t, opts)
		env.Start(t)

		require.Eventually(t, func() bool {
			return env.HealthCheck(t)
		}, 3*time.Second, 10*time.Millisecond)

		mr := env.GetMR(t)
		require.Eventually(t, func() bool {
			return mr.GetServerAddr() != ""
		}, time.Second, 10*time.Millisecond)
		mrAddr := mr.GetServerAddr()

		// connect mr
		env.connectMR(t)

		// VMS Server
		env.vmsServer = env.RunClusterManager(t, []string{mrAddr}, opts.VMSOpts)

		// VMS Client
		env.vmsCL = env.NewClusterManagerClient(t)

		defer func() {
			// Close all clients
			require.NoError(t, env.vmsCL.Close())
			env.closeMRClients(t)
			env.closeSNClients(t)

			env.Close(t)
			testutil.GC()
		}()

		f(env)
	}
}

func CreateTestCluster(t *testing.T) (*VarlogCluster, func()) {
	vmsOpts := vms.DefaultOptions()
	vmsOpts.Tick = 100 * time.Millisecond
	vmsOpts.HeartbeatTimeout = 30
	vmsOpts.ReportInterval = 10
	vmsOpts.Logger = zap.L()

	opts := VarlogClusterOptions{
		NrMR:                  1,
		NrRep:                 3,
		ReporterClientFac:     metadata_repository.NewReporterClientFactory(),
		SNManagementClientFac: metadata_repository.NewStorageNodeManagementClientFactory(),
		VMSOpts:               &vmsOpts,
	}

	clus := NewVarlogCluster(t, opts)
	clus.Start(t)

	// MR HealthCheck
	require.Eventually(t, func() bool {
		return clus.HealthCheck(t)
	}, 3*time.Second, 10*time.Millisecond)

	require.Eventually(t, func() bool {
		return clus.GetMR(t).GetServerAddr() != ""
	}, time.Second, 10*time.Millisecond)

	// connect mr
	clus.connectMR(t)

	// VMS
	clus.vmsServer = clus.RunClusterManager(t, []string{clus.GetMR(t).GetServerAddr()}, opts.VMSOpts)

	clus.vmsCL = clus.NewClusterManagerClient(t)

	closer := func() {
		// Close all clients
		require.NoError(t, clus.vmsCL.Close())
		clus.closeMRClients(t)
		clus.closeSNClients(t)

		clus.Close(t)
		testutil.GC()
	}

	return clus, closer
}

func StartTestSN(t *testing.T, clus *VarlogCluster, num int) {
	for i := 0; i < num; i++ {
		_ = clus.AddSN(t)
	}

	// TODO: move this assertions to integration testing framework
	rsp, err := clus.vmsCL.GetStorageNodes(context.TODO())
	require.NoError(t, err)
	require.Len(t, rsp.GetStoragenodes(), num)

	t.Logf("Started %d SNs", num)
}
