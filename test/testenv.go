package test

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
	"go.uber.org/zap"
	"google.golang.org/grpc/health/grpc_health_v1"

	"github.daumkakao.com/varlog/varlog/internal/metadata_repository"
	"github.daumkakao.com/varlog/varlog/internal/storagenode"
	"github.daumkakao.com/varlog/varlog/internal/vms"
	"github.daumkakao.com/varlog/varlog/pkg/logc"
	"github.daumkakao.com/varlog/varlog/pkg/rpc"
	"github.daumkakao.com/varlog/varlog/pkg/types"
	"github.daumkakao.com/varlog/varlog/pkg/varlog"
	"github.daumkakao.com/varlog/varlog/pkg/verrors"
	"github.daumkakao.com/varlog/varlog/proto/varlogpb"
	"github.daumkakao.com/varlog/varlog/vtesting"
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
	volumes        map[types.StorageNodeID]storagenode.Volume
	snRPCEndpoints map[types.StorageNodeID]string
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
		volumes:              make(map[types.StorageNodeID]storagenode.Volume),
		snRPCEndpoints:       make(map[types.StorageNodeID]string),
		ClusterID:            types.ClusterID(1),
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

	os.RemoveAll(fmt.Sprintf("%s/wal/%d", vtesting.TestRaftDir(), nodeID))
	os.RemoveAll(fmt.Sprintf("%s/snap/%d", vtesting.TestRaftDir(), nodeID))

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
		RaftAddress:       clus.mrPeers[idx],
		Join:              join,
		SnapCount:         uint64(clus.SnapCount),
		RaftTick:          vtesting.TestRaftTick(),
		RaftDir:           vtesting.TestRaftDir(),
		RPCTimeout:        vtesting.TimeoutAccordingToProcCnt(metadata_repository.DefaultRPCTimeout),
		NumRep:            clus.NrRep,
		Peers:             clus.mrPeers,
		RPCBindAddress:    clus.mrRPCEndpoints[idx],
		ReporterClientFac: clus.ReporterClientFac,
		Logger:            clus.logger,
	}

	opts.CollectorName = "nop"
	opts.CollectorEndpoint = "localhost:55680"

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

	os.RemoveAll(vtesting.TestRaftDir())

	for _, sn := range clus.SNs {
		// TODO (jun): remove temporary directories
		snmeta, err := sn.GetMetadata(context.TODO())
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

func (clus *VarlogCluster) HealthCheck() bool {
	for _, endpoint := range clus.mrRPCEndpoints {
		conn, err := rpc.NewBlockingConn(endpoint)
		if err != nil {
			return false
		}
		defer conn.Close()

		healthClient := grpc_health_v1.NewHealthClient(conn.Conn)
		if _, err := healthClient.Check(context.Background(), &grpc_health_v1.HealthCheckRequest{}); err != nil {
			return false
		}
	}

	return true
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
	opts := storagenode.DefaultOptions()
	opts.ListenAddress = "127.0.0.1:0"
	opts.ClusterID = clus.ClusterID
	opts.StorageNodeID = snID
	opts.Logger = clus.logger
	opts.Volumes = map[storagenode.Volume]struct{}{volume: {}}

	sn, err := storagenode.NewStorageNode(&opts)
	if err != nil {
		return types.StorageNodeID(0), err
	}
	err = sn.Run()
	if err != nil {
		return types.StorageNodeID(0), err
	}

	meta, err := sn.GetMetadata(context.TODO())
	if err != nil {
		return types.StorageNodeID(0), err
	}

	clus.SNs[snID] = sn
	clus.volumes[snID] = volume
	clus.snRPCEndpoints[snID] = meta.StorageNode.Address

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

	opts := storagenode.DefaultOptions()
	opts.ListenAddress = "127.0.0.1:0"
	opts.ClusterID = clus.ClusterID
	opts.StorageNodeID = snID
	opts.Logger = clus.logger
	opts.Volumes = map[storagenode.Volume]struct{}{volume: {}}
	opts.CollectorName = "nop"
	opts.CollectorEndpoint = "localhost:55680"

	sn, err := storagenode.NewStorageNode(&opts)
	if err != nil {
		return types.StorageNodeID(0), err
	}

	var meta *varlogpb.StorageNodeMetadataDescriptor
	if err = sn.Run(); err != nil {
		goto err_out
	}

	meta, err = sn.GetMetadata(context.TODO())
	if err != nil {
		goto err_out
	}

	_, err = clus.CM.AddStorageNode(context.TODO(), meta.StorageNode.Address)
	if err != nil {
		goto err_out
	}

	clus.SNs[snID] = sn
	clus.volumes[snID] = volume
	clus.snRPCEndpoints[snID] = meta.StorageNode.Address
	return meta.StorageNode.StorageNodeID, nil

err_out:
	sn.Close()
	return types.StorageNodeID(0), err
}

func (clus *VarlogCluster) RecoverSN(snID types.StorageNodeID) (*storagenode.StorageNode, error) {
	volume, ok := clus.volumes[snID]
	if !ok {
		return nil, errors.New("no volume")
	}

	addr, _ := clus.snRPCEndpoints[snID]

	opts := storagenode.DefaultOptions()
	opts.ListenAddress = addr
	opts.ClusterID = clus.ClusterID
	opts.StorageNodeID = snID
	opts.Logger = clus.logger
	opts.Volumes = map[storagenode.Volume]struct{}{volume: {}}

	sn, err := storagenode.NewStorageNode(&opts)
	if err != nil {
		return nil, err
	}
	err = sn.Run()
	if err != nil {
		return nil, err
	}

	clus.SNs[snID] = sn

	return sn, nil
}

func (clus *VarlogCluster) AddLS() (types.LogStreamID, error) {
	if len(clus.SNs) < clus.NrRep {
		return types.LogStreamID(0), verrors.ErrInvalid
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
		snmeta, err := storageNode.GetMetadata(context.TODO())
		if err != nil {
			return types.LogStreamID(0), err
		}
		targetpath := snmeta.GetStorageNode().GetStorages()[0].Path
		replicas = append(replicas, &varlogpb.ReplicaDescriptor{StorageNodeID: snIDs[i], Path: targetpath})
	}

	for _, r := range replicas {
		sn, _ := clus.SNs[r.StorageNodeID]
		if _, err := sn.AddLogStream(context.TODO(), lsID, r.Path); err != nil {
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
		return types.LogStreamID(0), verrors.ErrInvalid
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
	_, err := sn.AddLogStream(context.TODO(), lsID, "path")
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

	snmeta, err := sn.GetMetadata(context.TODO())
	if err != nil {
		return err
	}
	path := snmeta.GetStorageNode().GetStorages()[0].GetPath()
	/*
		_, err = sn.AddLogStream(clus.ClusterID, newsn, lsID, path)
		if err != nil {
			return err
		}
	*/

	newReplica := &varlogpb.ReplicaDescriptor{
		StorageNodeID: newsn,
		Path:          path,
	}
	oldReplica := &varlogpb.ReplicaDescriptor{
		StorageNodeID: oldsn,
	}

	/*
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
	*/

	_, err = clus.CM.UpdateLogStream(context.TODO(), lsID, oldReplica, newReplica)
	return err
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

func (clus *VarlogCluster) LookupMR(nodeID types.NodeID) (*metadata_repository.RaftMetadataRepository, bool) {
	for idx, mrID := range clus.mrIDs {
		if nodeID == mrID {
			return clus.MRs[idx], true
		}
	}
	return nil, false
}

func (clus *VarlogCluster) GetVMS() vms.ClusterManager {
	return clus.CM
}

func (clus *VarlogCluster) getSN(lsID types.LogStreamID, idx int) (*storagenode.StorageNode, error) {
	if len(clus.MRs) == 0 {
		return nil, verrors.ErrInvalid
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
		return nil, verrors.ErrNotExist
	}

	if len(ls.Replicas) < idx+1 {
		return nil, verrors.ErrInvalid
	}

	sn := clus.LookupSN(ls.Replicas[idx].StorageNodeID)
	if sn == nil {
		return nil, verrors.ErrInternal
	}

	return sn, nil
}

func (clus *VarlogCluster) GetPrimarySN(lsID types.LogStreamID) (*storagenode.StorageNode, error) {
	return clus.getSN(lsID, 0)
}

func (clus *VarlogCluster) GetBackupSN(lsID types.LogStreamID, idx int) (*storagenode.StorageNode, error) {
	return clus.getSN(lsID, idx)
}

func (clus *VarlogCluster) NewLogIOClient(lsID types.LogStreamID) (logc.LogIOClient, error) {
	sn, err := clus.GetPrimarySN(lsID)
	if err != nil {
		return nil, err
	}

	snMeta, err := sn.GetMetadata(context.TODO())
	if err != nil {
		return nil, err
	}

	return logc.NewLogIOClient(snMeta.StorageNode.Address)
}

func (clus *VarlogCluster) RunClusterManager(mrAddrs []string, opts *vms.Options) (vms.ClusterManager, error) {
	if clus.VarlogClusterOptions.NrRep < 1 {
		return nil, verrors.ErrInvalidArgument
	}

	if opts == nil {
		vmOpts := vms.DefaultOptions()
		opts = &vmOpts
		opts.Logger = clus.logger
	}

	opts.ClusterID = clus.ClusterID
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
