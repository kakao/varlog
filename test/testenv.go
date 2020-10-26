package main

import (
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net/url"
	"os"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/urfave/cli/v2"
	"github.daumkakao.com/varlog/varlog/internal/metadata_repository"
	"github.daumkakao.com/varlog/varlog/internal/storagenode"
	"github.daumkakao.com/varlog/varlog/internal/vms"
	"github.daumkakao.com/varlog/varlog/pkg/varlog"
	"github.daumkakao.com/varlog/varlog/pkg/varlog/types"
	"github.daumkakao.com/varlog/varlog/proto/snpb"
	"github.daumkakao.com/varlog/varlog/proto/varlogpb"
	"github.daumkakao.com/varlog/varlog/vtesting"
	"go.uber.org/zap"
)

const (
	MR_PORT_BASE     = 10000
	MR_RPC_PORT_BASE = 11000
)

type VarlogClusterOptions struct {
	NrRep             int
	NrMR              int
	SnapCount         int
	ReporterClientFac metadata_repository.ReporterClientFactory
	VMSOpts           *vms.Options
}

type VarlogCluster struct {
	VarlogClusterOptions
	MRs            []*metadata_repository.RaftMetadataRepository
	SNs            map[types.StorageNodeID]*storagenode.StorageNode
	CM             vms.ClusterManager
	CMCli          varlog.ClusterManagerClient
	mrPeers        []string
	mrRPCEndpoints []string
	mrIDs          []types.NodeID
	snID           types.StorageNodeID
	lsID           types.LogStreamID
	ClusterID      types.ClusterID
	logger         *zap.Logger
}

func NewVarlogCluster(opts VarlogClusterOptions) *VarlogCluster {
	mrPeers := make([]string, opts.NrMR)
	mrRPCEndpoints := make([]string, opts.NrMR)
	MRs := make([]*metadata_repository.RaftMetadataRepository, opts.NrMR)
	mrIDs := make([]types.NodeID, opts.NrMR)

	for i := range mrPeers {
		mrPeers[i] = fmt.Sprintf("http://127.0.0.1:%d", MR_PORT_BASE+i)
		mrRPCEndpoints[i] = fmt.Sprintf("127.0.0.1:%d", MR_RPC_PORT_BASE+i)
	}

	clus := &VarlogCluster{
		VarlogClusterOptions: opts,
		logger:               zap.L(),
		mrPeers:              mrPeers,
		mrRPCEndpoints:       mrRPCEndpoints,
		mrIDs:                mrIDs,
		MRs:                  MRs,
		SNs:                  make(map[types.StorageNodeID]*storagenode.StorageNode),
	}

	for i := range clus.mrPeers {
		clus.clearMR(i)
		clus.createMR(i, false)
	}

	return clus
}

func (clus *VarlogCluster) clearMR(idx int) {
	if idx < 0 || idx >= len(clus.MRs) {
		return
	}

	url, _ := url.Parse(clus.mrPeers[idx])
	nodeID := types.NewNodeID(url.Host)

	os.RemoveAll(fmt.Sprintf("raft-%d", nodeID))
	os.RemoveAll(fmt.Sprintf("raft-%d-snap", nodeID))

	return
}

func (clus *VarlogCluster) createMR(idx int, join bool) error {
	if idx < 0 || idx >= len(clus.MRs) {
		return errors.New("out of range")
	}

	url, _ := url.Parse(clus.mrPeers[idx])
	nodeID := types.NewNodeID(url.Host)

	opts := &metadata_repository.MetadataRepositoryOptions{
		ClusterID:         clus.ClusterID,
		NodeID:            nodeID,
		Join:              join,
		SnapCount:         uint64(clus.SnapCount),
		RaftTick:          vtesting.TestRaftTick(),
		RPCTimeout:        vtesting.TimeoutAccordingToProcCnt(metadata_repository.DefaultRPCTimeout),
		NumRep:            clus.NrRep,
		PeerList:          *cli.NewStringSlice(clus.mrPeers...),
		RPCBindAddress:    clus.mrRPCEndpoints[idx],
		ReporterClientFac: clus.ReporterClientFac,
		Logger:            clus.logger,
	}

	clus.mrIDs[idx] = nodeID
	clus.MRs[idx] = metadata_repository.NewRaftMetadataRepository(opts)
	return nil
}

func (clus *VarlogCluster) AppendMR() error {
	idx := len(clus.MRs)
	clus.mrPeers = append(clus.mrPeers, fmt.Sprintf("http://127.0.0.1:%d", MR_PORT_BASE+idx))
	clus.mrRPCEndpoints = append(clus.mrRPCEndpoints, fmt.Sprintf("127.0.0.1:%d", MR_RPC_PORT_BASE+idx))
	clus.mrIDs = append(clus.mrIDs, types.InvalidNodeID)
	clus.MRs = append(clus.MRs, nil)

	clus.clearMR(idx)

	return clus.createMR(idx, true)
}

func (clus *VarlogCluster) StartMR(idx int) error {
	if idx < 0 || idx >= len(clus.MRs) {
		return errors.New("out of range")
	}

	clus.MRs[idx].Run()

	return nil
}

func (clus *VarlogCluster) Start() {
	clus.logger.Info("cluster start")
	for i := range clus.MRs {
		clus.StartMR(i)
	}
	clus.logger.Info("cluster complete")
}

func (clus *VarlogCluster) StopMR(idx int) error {
	if idx < 0 || idx >= len(clus.MRs) {
		return errors.New("out or range")
	}

	return clus.MRs[idx].Close()
}

func (clus *VarlogCluster) RestartMR(idx int) error {
	if idx < 0 || idx >= len(clus.MRs) {
		return errors.New("out of range")
	}

	clus.StopMR(idx)
	clus.createMR(idx, false)
	return clus.StartMR(idx)
}

func (clus *VarlogCluster) CloseMR(idx int) error {
	if idx < 0 || idx >= len(clus.MRs) {
		return errors.New("out or range")
	}

	err := clus.StopMR(idx)
	clus.clearMR(idx)

	clus.logger.Info("cluster node stop", zap.Int("idx", idx))

	return err
}

// Close closes all cluster MRs
func (clus *VarlogCluster) Close() error {
	var err error

	if clus.CM != nil {
		clus.CM.Close()
	}

	for i := range clus.mrPeers {
		if erri := clus.CloseMR(i); erri != nil {
			err = erri
		}
	}

	for _, sn := range clus.SNs {
		// TODO (jun): remove temporary directories
		snmeta, err := sn.GetMetadata(clus.ClusterID, snpb.MetadataTypeHeartbeat)
		if err != nil {
			clus.logger.Warn("could not get meta", zap.Error(err))
		}
		for _, storage := range snmeta.GetStorageNode().GetStorages() {
			dbpath := storage.GetPath()
			if dbpath == ":memory:" {
				continue
			}
			/* comment out for test
			if err := os.RemoveAll(dbpath); err != nil {
				clus.logger.Warn("could not remove dbpath", zap.String("path", dbpath), zap.Error(err))
			}
			*/
		}

		// TODO:: sn.Close() does not close connect
		sn.Close()
	}

	return err
}

func (clus *VarlogCluster) Leader() int {
	leader := -1
	for i, n := range clus.MRs {
		cinfo, _ := n.GetClusterInfo(context.TODO(), clus.ClusterID)
		if cinfo.GetLeader() != types.InvalidNodeID && clus.mrIDs[i] == cinfo.GetLeader() {
			leader = i
			break
		}
	}

	return leader
}

func (clus *VarlogCluster) LeaderElected() bool {
	for _, n := range clus.MRs {
		if cinfo, _ := n.GetClusterInfo(context.TODO(), clus.ClusterID); cinfo.GetLeader() == types.InvalidNodeID {
			return false
		}
	}

	return true
}

func (clus *VarlogCluster) LeaderFail() bool {
	leader := clus.Leader()
	if leader < 0 {
		return false
	}

	clus.StopMR(leader)
	return true
}

func (clus *VarlogCluster) AddSN() (types.StorageNodeID, error) {
	snID := clus.snID
	clus.snID += types.StorageNodeID(1)

	datadir, err := ioutil.TempDir("", "test_*")
	if err != nil {
		return types.StorageNodeID(0), err
	}
	volume, err := storagenode.NewVolume(datadir)
	if err != nil {
		return types.StorageNodeID(0), err
	}
	opts := &storagenode.Options{
		RPCOptions:               storagenode.RPCOptions{RPCBindAddress: ":0"},
		LogStreamExecutorOptions: storagenode.DefaultLogStreamExecutorOptions,
		LogStreamReporterOptions: storagenode.DefaultLogStreamReporterOptions,
		ClusterID:                clus.ClusterID,
		StorageNodeID:            snID,
		StorageName:              storagenode.DefaultStorageName,
		Verbose:                  true,
		Logger:                   clus.logger,
		Volumes:                  map[storagenode.Volume]struct{}{volume: {}},
	}

	sn, err := storagenode.NewStorageNode(opts)
	if err != nil {
		return types.StorageNodeID(0), err
	}
	err = sn.Run()
	if err != nil {
		return types.StorageNodeID(0), err
	}

	clus.SNs[snID] = sn

	meta, err := sn.GetMetadata(clus.ClusterID, snpb.MetadataTypeHeartbeat)
	if err != nil {
		return types.StorageNodeID(0), err
	}

	err = clus.MRs[0].RegisterStorageNode(context.TODO(), meta.GetStorageNode())
	return snID, err
}

func (clus *VarlogCluster) AddSNByVMS() (types.StorageNodeID, error) {
	snID := clus.snID
	clus.snID += types.StorageNodeID(1)

	datadir, err := ioutil.TempDir("", "test_*")
	if err != nil {
		return types.StorageNodeID(0), err
	}
	volume, err := storagenode.NewVolume(datadir)
	if err != nil {
		return types.StorageNodeID(0), err
	}
	opts := &storagenode.Options{
		RPCOptions:               storagenode.RPCOptions{RPCBindAddress: ":0"},
		LogStreamExecutorOptions: storagenode.DefaultLogStreamExecutorOptions,
		LogStreamReporterOptions: storagenode.DefaultLogStreamReporterOptions,
		ClusterID:                clus.ClusterID,
		StorageNodeID:            snID,
		StorageName:              storagenode.DefaultStorageName,
		Verbose:                  true,
		Logger:                   clus.logger,
		Volumes:                  map[storagenode.Volume]struct{}{volume: {}},
	}
	sn, err := storagenode.NewStorageNode(opts)
	if err != nil {
		return types.StorageNodeID(0), err
	}

	var meta *varlogpb.StorageNodeMetadataDescriptor
	if err = sn.Run(); err != nil {
		goto err_out
	}

	meta, err = sn.GetMetadata(clus.ClusterID, snpb.MetadataTypeHeartbeat)
	if err != nil {
		goto err_out
	}

	_, err = clus.CM.AddStorageNode(context.TODO(), meta.StorageNode.Address)
	if err != nil {
		goto err_out
	}

	clus.SNs[snID] = sn
	return meta.StorageNode.StorageNodeID, nil

err_out:
	sn.Close()
	return types.StorageNodeID(0), err
}

func (clus *VarlogCluster) AddLS() (types.LogStreamID, error) {
	if len(clus.SNs) < clus.NrRep {
		return types.LogStreamID(0), varlog.ErrInvalid
	}

	lsID := clus.lsID
	clus.lsID += types.LogStreamID(1)

	snIDs := make([]types.StorageNodeID, 0, len(clus.SNs))
	for snID := range clus.SNs {
		snIDs = append(snIDs, snID)
	}

	rand.Seed(time.Now().UnixNano())
	rand.Shuffle(len(snIDs), func(i, j int) { snIDs[i], snIDs[j] = snIDs[j], snIDs[i] })

	replicas := make([]*varlogpb.ReplicaDescriptor, 0, clus.NrRep)
	for i := 0; i < clus.NrRep; i++ {
		storageNodeID := snIDs[i]
		storageNode := clus.SNs[storageNodeID]
		snmeta, err := storageNode.GetMetadata(clus.ClusterID, snpb.MetadataTypeHeartbeat)
		if err != nil {
			return types.LogStreamID(0), err
		}
		targetpath := snmeta.GetStorageNode().GetStorages()[0].Path
		replicas = append(replicas, &varlogpb.ReplicaDescriptor{StorageNodeID: snIDs[i], Path: targetpath})
	}

	for _, r := range replicas {
		sn, _ := clus.SNs[r.StorageNodeID]
		if _, err := sn.AddLogStream(clus.ClusterID, r.StorageNodeID, lsID, r.Path); err != nil {
			return types.LogStreamID(0), err
		}
	}

	ls := &varlogpb.LogStreamDescriptor{
		LogStreamID: lsID,
		Replicas:    replicas,
	}

	err := clus.MRs[0].RegisterLogStream(context.TODO(), ls)
	return lsID, err
}

func (clus *VarlogCluster) AddLSByVMS() (types.LogStreamID, error) {
	if len(clus.SNs) < clus.NrRep {
		return types.LogStreamID(0), varlog.ErrInvalid
	}

	logStreamDesc, err := clus.CM.AddLogStream(context.TODO(), nil)
	if err != nil {
		return types.LogStreamID(0), err
	}

	lsID := logStreamDesc.GetLogStreamID()
	clus.lsID = lsID + types.LogStreamID(1)

	return lsID, nil
}

func (clus *VarlogCluster) UpdateLS(lsID types.LogStreamID, oldsn, newsn types.StorageNodeID) error {
	sn := clus.LookupSN(newsn)
	_, err := sn.AddLogStream(clus.ClusterID, newsn, lsID, "path")
	if err != nil {
		return err
	}

	meta, err := clus.GetMR().GetMetadata(context.TODO())
	if err != nil {
		return err
	}

	oldLSDesc := meta.GetLogStream(lsID)
	if oldLSDesc == nil {
		return errors.New("logStream is not exist")
	}
	newLSDesc := proto.Clone(oldLSDesc).(*varlogpb.LogStreamDescriptor)

	exist := false
	for _, r := range newLSDesc.Replicas {
		if r.StorageNodeID == oldsn {
			r.StorageNodeID = newsn
			exist = true
		}
	}

	if !exist {
		return errors.New("invalid victim")
	}

	return clus.GetMR().UpdateLogStream(context.TODO(), newLSDesc)
}

func (clus *VarlogCluster) UpdateLSByVMS(lsID types.LogStreamID, oldsn, newsn types.StorageNodeID) error {
	sn := clus.LookupSN(newsn)

	snmeta, err := sn.GetMetadata(clus.ClusterID, snpb.MetadataTypeLogStreams)
	if err != nil {
		return err
	}
	path := snmeta.GetStorageNode().GetStorages()[0].GetPath()
	_, err = sn.AddLogStream(clus.ClusterID, newsn, lsID, path)
	if err != nil {
		return err
	}

	meta, err := clus.CM.Metadata(context.TODO())
	if err != nil {
		return err
	}

	oldLSDesc := meta.GetLogStream(lsID)
	if oldLSDesc == nil {
		return errors.New("logStream is not exist")
	}
	newLSDesc := proto.Clone(oldLSDesc).(*varlogpb.LogStreamDescriptor)

	exist := false
	for _, r := range newLSDesc.Replicas {
		if r.StorageNodeID == oldsn {
			r.StorageNodeID = newsn
			exist = true
		}
	}

	if !exist {
		return errors.New("invalid victim")
	}

	return clus.CM.UpdateLogStream(context.TODO(), lsID, newLSDesc.Replicas)
}

func (clus *VarlogCluster) LookupSN(snID types.StorageNodeID) *storagenode.StorageNode {
	sn, _ := clus.SNs[snID]
	return sn
}

func (clus *VarlogCluster) GetMR() *metadata_repository.RaftMetadataRepository {
	if len(clus.MRs) == 0 {
		return nil
	}

	return clus.MRs[0]
}

func (clus *VarlogCluster) GetVMS() vms.ClusterManager {
	return clus.CM
}

func (clus *VarlogCluster) getSN(lsID types.LogStreamID, idx int) (*storagenode.StorageNode, error) {
	if len(clus.MRs) == 0 {
		return nil, varlog.ErrInvalid
	}

	var meta *varlogpb.MetadataDescriptor
	var err error
	for _, mr := range clus.MRs {
		meta, err = mr.GetMetadata(context.TODO())
		if meta != nil {
			break
		}
	}

	if meta == nil {
		return nil, err
	}

	ls := meta.GetLogStream(lsID)
	if ls == nil {
		return nil, varlog.ErrNotExist
	}

	if len(ls.Replicas) < idx+1 {
		return nil, varlog.ErrInvalid
	}

	sn := clus.LookupSN(ls.Replicas[idx].StorageNodeID)
	if sn == nil {
		return nil, varlog.ErrInternal
	}

	return sn, nil
}

func (clus *VarlogCluster) GetPrimarySN(lsID types.LogStreamID) (*storagenode.StorageNode, error) {
	return clus.getSN(lsID, 0)
}

func (clus *VarlogCluster) GetBackupSN(lsID types.LogStreamID) (*storagenode.StorageNode, error) {
	return clus.getSN(lsID, 1)
}

func (clus *VarlogCluster) NewLogIOClient(lsID types.LogStreamID) (varlog.LogIOClient, error) {
	sn, err := clus.GetPrimarySN(lsID)
	if err != nil {
		return nil, err
	}

	snMeta, err := sn.GetMetadata(clus.ClusterID, snpb.MetadataTypeHeartbeat)
	if err != nil {
		return nil, err
	}

	return varlog.NewLogIOClient(snMeta.StorageNode.Address)
}

func (clus *VarlogCluster) RunClusterManager(mrAddrs []string, opts *vms.Options) (vms.ClusterManager, error) {
	if clus.VarlogClusterOptions.NrRep < 1 {
		return nil, varlog.ErrInvalidArgument
	}

	if opts == nil {
		opts = &vms.DefaultOptions
		opts.Logger = clus.logger
	}

	opts.MetadataRepositoryAddresses = mrAddrs
	opts.ReplicationFactor = uint(clus.VarlogClusterOptions.NrRep)

	cm, err := vms.NewClusterManager(context.TODO(), opts)
	if err != nil {
		return nil, err
	}
	if err := cm.Run(); err != nil {
		return nil, err
	}
	clus.CM = cm
	return cm, nil
}

func (clus *VarlogCluster) NewClusterManagerClient() (varlog.ClusterManagerClient, error) {
	addr := clus.CM.Address()
	cmcli, err := varlog.NewClusterManagerClient(addr)
	if err != nil {
		return nil, err
	}
	clus.CMCli = cmcli
	return cmcli, err
}

func (clus *VarlogCluster) GetClusterManagerClient() varlog.ClusterManagerClient {
	return clus.CMCli
}
