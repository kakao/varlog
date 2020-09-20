package main

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"net/url"
	"os"
	"time"

	"github.com/kakao/varlog/internal/metadata_repository"
	"github.com/kakao/varlog/internal/storage"
	"github.com/kakao/varlog/pkg/varlog"
	"github.com/kakao/varlog/pkg/varlog/types"
	"github.com/kakao/varlog/proto/storage_node"
	snpb "github.com/kakao/varlog/proto/storage_node"
	vpb "github.com/kakao/varlog/proto/varlog"

	"github.com/urfave/cli/v2"
	"go.uber.org/zap"
)

const (
	MR_PORT_BASE = 10000
)

type VarlogClusterOptions struct {
	NrRep             int
	NrMR              int
	SnapCount         int
	ReporterClientFac metadata_repository.ReporterClientFactory
}

type VarlogCluster struct {
	VarlogClusterOptions
	MRs       []*metadata_repository.RaftMetadataRepository
	SNs       map[types.StorageNodeID]*storage.StorageNode
	mrPeers   []string
	mrIDs     []types.NodeID
	snID      types.StorageNodeID
	lsID      types.LogStreamID
	ClusterID types.ClusterID
	logger    *zap.Logger
}

func NewVarlogCluster(opts VarlogClusterOptions) *VarlogCluster {
	mrPeers := make([]string, opts.NrMR)
	MRs := make([]*metadata_repository.RaftMetadataRepository, opts.NrMR)
	mrIDs := make([]types.NodeID, opts.NrMR)

	for i := range mrPeers {
		mrPeers[i] = fmt.Sprintf("http://127.0.0.1:%d", MR_PORT_BASE+i)
	}

	logger := zap.NewNop()
	//logger, _ := zap.NewDevelopment()
	clus := &VarlogCluster{
		VarlogClusterOptions: opts,
		logger:               logger,
		mrPeers:              mrPeers,
		mrIDs:                mrIDs,
		MRs:                  MRs,
		SNs:                  make(map[types.StorageNodeID]*storage.StorageNode),
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
		NumRep:            clus.NrRep,
		PeerList:          *cli.NewStringSlice(clus.mrPeers...),
		RPCBindAddress:    ":0",
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

	for i := range clus.mrPeers {
		if erri := clus.CloseMR(i); erri != nil {
			err = erri
		}
	}

	for _, sn := range clus.SNs {
		// TODO:: sn.Close() does not close connect
		sn.Close()
	}

	return err
}

func (clus *VarlogCluster) Leader() int {
	leader := -1
	for i, n := range clus.MRs {
		l, _, _ := n.GetClusterInfo(context.TODO(), clus.ClusterID)
		if l != types.InvalidNodeID && clus.mrIDs[i] == l {
			leader = i
			break
		}
	}

	return leader
}

func (clus *VarlogCluster) LeaderElected() bool {
	for _, n := range clus.MRs {
		if l, _, _ := n.GetClusterInfo(context.TODO(), clus.ClusterID); l == types.InvalidNodeID {
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

	opts := &storage.StorageNodeOptions{
		RPCOptions:               storage.RPCOptions{RPCBindAddress: ":0"},
		LogStreamExecutorOptions: storage.DefaultLogStreamExecutorOptions,
		LogStreamReporterOptions: storage.DefaultLogStreamReporterOptions,
		ClusterID:                clus.ClusterID,
		StorageNodeID:            snID,
		Verbose:                  true,
		Logger:                   clus.logger,
	}

	sn, err := storage.NewStorageNode(opts)
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

	snd := &vpb.StorageNodeDescriptor{
		StorageNodeID: snID,
		Address:       meta.StorageNode.Address,
		Status:        meta.StorageNode.Status,
		Storages: []*vpb.StorageDescriptor{
			&vpb.StorageDescriptor{
				Path:  "tmp",
				Used:  0,
				Total: 100,
			},
		},
	}

	err = clus.MRs[0].RegisterStorageNode(context.TODO(), snd)
	return snID, err
}

func (clus *VarlogCluster) AddLS() (types.LogStreamID, error) {
	if len(clus.SNs) < clus.NrRep {
		return types.LogStreamID(0), varlog.ErrInvalid
	}

	lsID := clus.lsID
	clus.lsID += types.LogStreamID(1)

	snIDs := make([]types.StorageNodeID, 0, len(clus.SNs))
	for snID, _ := range clus.SNs {
		snIDs = append(snIDs, snID)
	}

	rand.Seed(time.Now().UnixNano())
	rand.Shuffle(len(snIDs), func(i, j int) { snIDs[i], snIDs[j] = snIDs[j], snIDs[i] })

	replicas := make([]*vpb.ReplicaDescriptor, 0, clus.NrRep)
	for i := 0; i < clus.NrRep; i++ {
		replicas = append(replicas, &vpb.ReplicaDescriptor{StorageNodeID: snIDs[i], Path: "tmp"})
	}

	for _, r := range replicas {
		sn, _ := clus.SNs[r.StorageNodeID]
		if _, err := sn.AddLogStream(clus.ClusterID, r.StorageNodeID, lsID, "tmp"); err != nil {
			return types.LogStreamID(0), err
		}
	}

	ls := &vpb.LogStreamDescriptor{
		LogStreamID: lsID,
		Replicas:    replicas,
	}

	err := clus.MRs[0].RegisterLogStream(context.TODO(), ls)
	return lsID, err
}

func (clus *VarlogCluster) LookupSN(snID types.StorageNodeID) *storage.StorageNode {
	sn, _ := clus.SNs[snID]
	return sn
}

func (clus *VarlogCluster) GetMR() *metadata_repository.RaftMetadataRepository {
	if len(clus.MRs) == 0 {
		return nil
	}

	return clus.MRs[0]
}

func (clus *VarlogCluster) getSN(lsID types.LogStreamID, idx int) (*storage.StorageNode, error) {
	if len(clus.MRs) == 0 {
		return nil, varlog.ErrInvalid
	}

	meta, err := clus.MRs[0].GetMetadata(context.TODO())
	if err != nil {
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

func (clus *VarlogCluster) GetPrimarySN(lsID types.LogStreamID) (*storage.StorageNode, error) {
	return clus.getSN(lsID, 0)
}

func (clus *VarlogCluster) GetBackupSN(lsID types.LogStreamID) (*storage.StorageNode, error) {
	return clus.getSN(lsID, 1)
}

func (clus *VarlogCluster) NewLogIOClient(lsID types.LogStreamID) (varlog.LogIOClient, error) {
	sn, err := clus.GetPrimarySN(lsID)
	if err != nil {
		return nil, err
	}

	snMeta, err := sn.GetMetadata(clus.ClusterID, storage_node.MetadataTypeHeartbeat)
	if err != nil {
		return nil, err
	}

	return varlog.NewLogIOClient(snMeta.StorageNode.Address)
}
