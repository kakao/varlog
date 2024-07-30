package metarepos

import (
	"context"
	"fmt"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/pkg/errors"
	. "github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.uber.org/goleak"
	"go.uber.org/multierr"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"

	"github.com/kakao/varlog/internal/vtesting"
	"github.com/kakao/varlog/pkg/rpc"
	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/pkg/util/testutil"
	"github.com/kakao/varlog/pkg/util/testutil/ports"
	"github.com/kakao/varlog/proto/mrpb"
	"github.com/kakao/varlog/proto/snpb"
	"github.com/kakao/varlog/proto/varlogpb"
)

const rpcTimeout = 3 * time.Second

type metadataRepoCluster struct {
	nrRep             int
	peers             []string
	nodes             []*RaftMetadataRepository
	reporterClientFac ReporterClientFactory
	logger            *zap.Logger
	portLease         *ports.Lease
	rpcTimeout        time.Duration // conn + call
}

var testSnapCount uint64

func newMetadataRepoCluster(n, nrRep int, increseUncommit bool) *metadataRepoCluster {
	portLease, err := ports.ReserveWeaklyWithRetry(10000)
	if err != nil {
		panic(err)
	}

	peers := make([]string, n)
	nodes := make([]*RaftMetadataRepository, n)

	for i := range peers {
		peers[i] = fmt.Sprintf("http://127.0.0.1:%d", portLease.Base()+i)
	}

	sncf := NewDummyStorageNodeClientFactory(1, !increseUncommit)
	clus := &metadataRepoCluster{
		nrRep:             nrRep,
		peers:             peers,
		nodes:             nodes,
		reporterClientFac: sncf,
		logger:            zap.L(),
		portLease:         portLease,
		rpcTimeout:        rpcTimeout,
	}

	for i := range clus.peers {
		clus.clear(i)
		So(clus.createMetadataRepo(i, false), ShouldBeNil)
	}

	return clus
}

func (clus *metadataRepoCluster) clear(idx int) {
	if idx < 0 || idx >= len(clus.nodes) {
		return
	}
	nodeID := types.NewNodeIDFromURL(clus.peers[idx])
	os.RemoveAll(fmt.Sprintf("%s/wal/%d", vtesting.TestRaftDir(), nodeID))  //nolint:errcheck,revive // TODO:: Handle an error returned.
	os.RemoveAll(fmt.Sprintf("%s/snap/%d", vtesting.TestRaftDir(), nodeID)) //nolint:errcheck,revive // TODO:: Handle an error returned.
	os.RemoveAll(fmt.Sprintf("%s/sml/%d", vtesting.TestRaftDir(), nodeID))  //nolint:errcheck,revive // TODO:: Handle an error returned.
}

func (clus *metadataRepoCluster) createMetadataRepo(idx int, join bool) error {
	if idx < 0 || idx >= len(clus.nodes) {
		return errors.New("out of range")
	}

	peers := make([]string, len(clus.peers))
	copy(peers, clus.peers)

	opts := []Option{
		WithClusterID(1),
		WithReplicationFactor(clus.nrRep),
		WithRaftAddress(clus.peers[idx]),
		WithRPCAddress(":0"),
		WithRPCTimeout(vtesting.TimeoutAccordingToProcCnt(DefaultRPCTimeout)),
		WithTelemetryCollectorName("nop"),
		WithTelemetryCollectorEndpoint("localhost:55680"),
		WithLogger(clus.logger),
		WithSnapshotCount(testSnapCount),
		WithSnapshotCatchUpCount(testSnapCount),
		WithPeers(peers...),
		WithRaftDirectory(vtesting.TestRaftDir()),
		WithRaftTick(vtesting.TestRaftTick()),
		WithReporterClientFactory(clus.reporterClientFac),
		WithCommitTick(vtesting.TestCommitTick()),
	}
	if join {
		opts = append(opts, JoinCluster())
	}

	clus.nodes[idx] = NewRaftMetadataRepository(opts...)
	return nil
}

func (clus *metadataRepoCluster) appendMetadataRepo() error {
	idx := len(clus.nodes)
	clus.peers = append(clus.peers, fmt.Sprintf("http://127.0.0.1:%d", clus.portLease.Base()+idx))
	clus.nodes = append(clus.nodes, nil)

	clus.clear(idx)

	return clus.createMetadataRepo(idx, true)
}

func (clus *metadataRepoCluster) start(idx int) error {
	if idx < 0 || idx >= len(clus.nodes) {
		return errors.New("out of range")
	}

	clus.nodes[idx].Run()

	return nil
}

func (clus *metadataRepoCluster) Start() error {
	clus.logger.Info("cluster start")
	for i := range clus.nodes {
		if err := clus.start(i); err != nil {
			return errors.WithStack(err)
		}
	}
	clus.logger.Info("cluster complete")
	return nil
}

func (clus *metadataRepoCluster) stop(idx int) error {
	if idx < 0 || idx >= len(clus.nodes) {
		return errors.New("out or range")
	}

	return clus.nodes[idx].Close()
}

func (clus *metadataRepoCluster) restart(idx int) error {
	if idx < 0 || idx >= len(clus.nodes) {
		return errors.New("out of range")
	}

	if err := clus.stop(idx); err != nil {
		return errors.WithStack(err)
	}
	if err := clus.createMetadataRepo(idx, false); err != nil {
		return errors.WithStack(err)
	}
	return clus.start(idx)
}

func (clus *metadataRepoCluster) close(idx int) error {
	if idx < 0 || idx >= len(clus.nodes) {
		return errors.New("out or range")
	}

	err := clus.stop(idx)
	clus.clear(idx)
	clus.logger.Info("cluster node stop", zap.Int("idx", idx))
	return err
}

func (clus *metadataRepoCluster) getSnapshot(idx int) *raftpb.Snapshot {
	if idx < 0 || idx >= len(clus.nodes) {
		return nil
	}

	return clus.nodes[idx].raftNode.loadSnapshot()
}

func (clus *metadataRepoCluster) getMembersFromSnapshot(idx int) map[types.NodeID]*mrpb.MetadataRepositoryDescriptor_PeerDescriptor {
	snapshot := clus.getSnapshot(idx)
	if snapshot == nil {
		return nil
	}

	stateMachine := &mrpb.MetadataRepositoryDescriptor{}
	err := stateMachine.Unmarshal(snapshot.Data)
	if err != nil {
		return nil
	}

	members := make(map[types.NodeID]*mrpb.MetadataRepositoryDescriptor_PeerDescriptor)
	for nodeID, peer := range stateMachine.PeersMap.Peers {
		if !peer.IsLearner {
			members[nodeID] = peer
		}
	}

	return members
}

func (clus *metadataRepoCluster) getMetadataFromSnapshot(idx int) *varlogpb.MetadataDescriptor {
	snapshot := clus.getSnapshot(idx)
	if snapshot == nil {
		return nil
	}

	stateMachine := &mrpb.MetadataRepositoryDescriptor{}
	err := stateMachine.Unmarshal(snapshot.Data)
	if err != nil {
		return nil
	}

	return stateMachine.Metadata
}

// Close closes all cluster nodes.
func (clus *metadataRepoCluster) Close() error {
	var err error
	for i := range clus.peers {
		err = multierr.Append(err, clus.close(i))
	}
	err = multierr.Append(err, os.RemoveAll(vtesting.TestRaftDir()))
	err = multierr.Append(err, clus.portLease.Release())
	return err
}

func (clus *metadataRepoCluster) healthCheck(idx int) bool {
	f := clus.nodes[idx].endpointAddr.Load()
	if f == nil {
		return false
	}
	_, port, _ := net.SplitHostPort(f.(string))
	endpoint := fmt.Sprintf("localhost:%s", port)

	ctx, cancel := context.WithTimeout(context.TODO(), clus.rpcTimeout)
	defer cancel()

	conn, err := rpc.NewConn(ctx, endpoint)
	if err != nil {
		return false
	}
	defer conn.Close() //nolint:errcheck,revive // TODO:: Handle an error returned.

	healthClient := grpc_health_v1.NewHealthClient(conn.Conn)
	if _, err := healthClient.Check(ctx, &grpc_health_v1.HealthCheckRequest{}); err != nil {
		return false
	}

	return true
}

func (clus *metadataRepoCluster) healthCheckAll() bool {
	for i := range clus.nodes {
		if !clus.healthCheck(i) {
			return false
		}
	}

	return true
}

func (clus *metadataRepoCluster) leader() int {
	leader := -1
	for i, n := range clus.nodes {
		if n.isLeader() {
			leader = i
			break
		}
	}

	return leader
}

func (clus *metadataRepoCluster) leaderFail() bool {
	leader := clus.leader()
	if leader < 0 {
		return false
	}

	clus.stop(leader) //nolint:errcheck,revive // TODO:: Handle an error returned.
	return true
}

func (clus *metadataRepoCluster) closeNoErrors(t *testing.T) {
	clus.logger.Info("cluster stop")
	if err := clus.Close(); err != nil {
		t.Log(err)
	}
	clus.logger.Info("cluster stop complete")
}

func (clus *metadataRepoCluster) initDummyStorageNode(nrSN, nrTopic int) error {
	for i := 0; i < nrTopic; i++ {
		if err := clus.nodes[0].RegisterTopic(context.TODO(), types.TopicID(i%nrTopic)); err != nil {
			return err
		}
	}

	for i := 0; i < nrSN; i++ {
		snID := types.MinStorageNodeID + types.StorageNodeID(i)

		sn := &varlogpb.StorageNodeDescriptor{
			StorageNode: varlogpb.StorageNode{
				StorageNodeID: snID,
			},
		}

		rctx, cancel := context.WithTimeout(context.Background(), vtesting.TimeoutUnitTimesFactor(50))
		defer cancel()

		if err := clus.nodes[0].RegisterStorageNode(rctx, sn); err != nil {
			return err
		}

		lsID := types.LogStreamID(snID)
		ls := makeLogStream(types.TopicID(i%nrTopic), lsID, []types.StorageNodeID{snID})

		rctx, cancel = context.WithTimeout(context.Background(), vtesting.TimeoutUnitTimesFactor(50))
		defer cancel()

		if err := clus.nodes[0].RegisterLogStream(rctx, ls); err != nil {
			return err
		}
	}

	return nil
}

func (clus *metadataRepoCluster) getSNIDs() []types.StorageNodeID {
	return clus.reporterClientFac.(*DummyStorageNodeClientFactory).getClientIDs()
}

func makeUncommitReport(snID types.StorageNodeID, ver types.Version, hwm types.GLSN, lsID types.LogStreamID, offset types.LLSN, length uint64) *mrpb.Report {
	report := &mrpb.Report{
		StorageNodeID: snID,
	}
	u := snpb.LogStreamUncommitReport{
		LogStreamID:           lsID,
		Version:               ver,
		HighWatermark:         hwm,
		UncommittedLLSNOffset: offset,
		UncommittedLLSNLength: length,
	}
	report.UncommitReport = append(report.UncommitReport, u)

	return report
}

func appendUncommitReport(report *mrpb.Report, ver types.Version, hwm types.GLSN, lsID types.LogStreamID, offset types.LLSN, length uint64) *mrpb.Report {
	u := snpb.LogStreamUncommitReport{
		LogStreamID:           lsID,
		Version:               ver,
		HighWatermark:         hwm,
		UncommittedLLSNOffset: offset,
		UncommittedLLSNLength: length,
	}
	report.UncommitReport = append(report.UncommitReport, u)

	return report
}

func makeLogStream(topicID types.TopicID, lsID types.LogStreamID, snIDs []types.StorageNodeID) *varlogpb.LogStreamDescriptor {
	ls := &varlogpb.LogStreamDescriptor{
		TopicID:     topicID,
		LogStreamID: lsID,
		Status:      varlogpb.LogStreamStatusRunning,
	}

	for _, snID := range snIDs {
		r := &varlogpb.ReplicaDescriptor{
			StorageNodeID: snID,
		}

		ls.Replicas = append(ls.Replicas, r)
	}

	return ls
}

func TestMRApplyReport(t *testing.T) {
	Convey("Report Should not be applied if not register LogStream", t, func(ctx C) {
		rep := 2
		clus := newMetadataRepoCluster(1, rep, false)
		Reset(func() {
			clus.closeNoErrors(t)
		})
		mr := clus.nodes[0]

		tn := &varlogpb.TopicDescriptor{
			TopicID: types.TopicID(1),
			Status:  varlogpb.TopicStatusRunning,
		}

		err := mr.storage.registerTopic(tn)
		So(err, ShouldBeNil)

		snIDs := make([]types.StorageNodeID, rep)
		for i := range snIDs {
			snIDs[i] = types.MinStorageNodeID + types.StorageNodeID(i)

			sn := &varlogpb.StorageNodeDescriptor{
				StorageNode: varlogpb.StorageNode{
					StorageNodeID: snIDs[i],
				},
			}

			err := mr.storage.registerStorageNode(sn)
			So(err, ShouldBeNil)
		}
		lsID := types.MinLogStreamID
		notExistSnID := types.MinStorageNodeID + types.StorageNodeID(rep)

		report := makeUncommitReport(snIDs[0], types.InvalidVersion, types.InvalidGLSN, lsID, types.MinLLSN, 2)
		mr.applyReport(&mrpb.Reports{Reports: []*mrpb.Report{report}}) //nolint:errcheck,revive // TODO:: Handle an error returned.

		for _, snID := range snIDs {
			_, ok := mr.storage.LookupUncommitReport(lsID, snID)
			So(ok, ShouldBeFalse)
		}

		Convey("UncommitReport should register when register LogStream", func(ctx C) {
			err := mr.storage.registerTopic(&varlogpb.TopicDescriptor{TopicID: types.TopicID(1)})
			So(err, ShouldBeNil)

			ls := makeLogStream(types.TopicID(1), lsID, snIDs)
			err = mr.storage.registerLogStream(ls)
			So(err, ShouldBeNil)

			for _, snID := range snIDs {
				_, ok := mr.storage.LookupUncommitReport(lsID, snID)
				So(ok, ShouldBeTrue)
			}

			Convey("Report should not apply if snID is not exist in UncommitReport", func(ctx C) {
				report := makeUncommitReport(notExistSnID, types.InvalidVersion, types.InvalidGLSN, lsID, types.MinLLSN, 2)
				mr.applyReport(&mrpb.Reports{Reports: []*mrpb.Report{report}}) //nolint:errcheck,revive // TODO:: Handle an error returned.

				_, ok := mr.storage.LookupUncommitReport(lsID, notExistSnID)
				So(ok, ShouldBeFalse)
			})

			Convey("Report should apply if snID is exist in UncommitReport", func(ctx C) {
				snID := snIDs[0]
				report := makeUncommitReport(snID, types.InvalidVersion, types.InvalidGLSN, lsID, types.MinLLSN, 2)
				mr.applyReport(&mrpb.Reports{Reports: []*mrpb.Report{report}}) //nolint:errcheck,revive // TODO:: Handle an error returned.

				r, ok := mr.storage.LookupUncommitReport(lsID, snID)
				So(ok, ShouldBeTrue)
				So(r.UncommittedLLSNEnd(), ShouldEqual, types.MinLLSN+types.LLSN(2))

				Convey("Report which have bigger END LLSN Should be applied", func(ctx C) {
					report := makeUncommitReport(snID, types.InvalidVersion, types.InvalidGLSN, lsID, types.MinLLSN, 3)
					mr.applyReport(&mrpb.Reports{Reports: []*mrpb.Report{report}}) //nolint:errcheck,revive // TODO:: Handle an error returned.

					r, ok := mr.storage.LookupUncommitReport(lsID, snID)
					So(ok, ShouldBeTrue)
					So(r.UncommittedLLSNEnd(), ShouldEqual, types.MinLLSN+types.LLSN(3))
				})

				Convey("Report which have smaller END LLSN Should Not be applied", func(ctx C) {
					report := makeUncommitReport(snID, types.InvalidVersion, types.InvalidGLSN, lsID, types.MinLLSN, 1)
					mr.applyReport(&mrpb.Reports{Reports: []*mrpb.Report{report}}) //nolint:errcheck,revive // TODO:: Handle an error returned.

					r, ok := mr.storage.LookupUncommitReport(lsID, snID)
					So(ok, ShouldBeTrue)
					So(r.UncommittedLLSNEnd(), ShouldNotEqual, types.MinLLSN+types.LLSN(1))
				})
			})
		})
	})
}

func TestMRApplyInvalidReport(t *testing.T) {
	Convey("Given LogStream", t, func(ctx C) {
		rep := 2
		clus := newMetadataRepoCluster(1, rep, false)
		Reset(func() {
			clus.closeNoErrors(t)
		})
		mr := clus.nodes[0]

		tn := &varlogpb.TopicDescriptor{
			TopicID: types.TopicID(1),
			Status:  varlogpb.TopicStatusRunning,
		}

		err := mr.storage.registerTopic(tn)
		So(err, ShouldBeNil)

		snIDs := make([]types.StorageNodeID, rep)
		for i := range snIDs {
			snIDs[i] = types.MinStorageNodeID + types.StorageNodeID(i)

			sn := &varlogpb.StorageNodeDescriptor{
				StorageNode: varlogpb.StorageNode{
					StorageNodeID: snIDs[i],
				},
			}

			err := mr.storage.registerStorageNode(sn)
			So(err, ShouldBeNil)
		}

		lsID := types.MinLogStreamID
		ls := makeLogStream(types.TopicID(1), lsID, snIDs)
		err = mr.storage.registerLogStream(ls)
		So(err, ShouldBeNil)

		for _, snID := range snIDs {
			report := makeUncommitReport(snID, types.InvalidVersion, types.InvalidGLSN, lsID, types.MinLLSN, 1)
			mr.applyReport(&mrpb.Reports{Reports: []*mrpb.Report{report}}) //nolint:errcheck,revive // TODO:: Handle an error returned.

			r, ok := mr.storage.LookupUncommitReport(lsID, snID)
			So(ok, ShouldBeTrue)
			So(r.Invalid(), ShouldBeFalse)
		}

		Convey("When Some LogStream reports invalid report", func(ctx C) {
			report := makeUncommitReport(snIDs[0], types.InvalidVersion, types.InvalidGLSN, lsID, types.InvalidLLSN, 2)
			mr.applyReport(&mrpb.Reports{Reports: []*mrpb.Report{report}}) //nolint:errcheck,revive // TODO:: Handle an error returned.

			r, ok := mr.storage.LookupUncommitReport(lsID, snIDs[0])
			So(ok, ShouldBeTrue)
			So(r.Invalid(), ShouldBeFalse)
		})
	})
}

func TestMRIgnoreDirtyReport(t *testing.T) {
	Convey("Given LogStream", t, func(ctx C) {
		rep := 2
		clus := newMetadataRepoCluster(1, rep, false)
		Reset(func() {
			clus.closeNoErrors(t)
		})
		mr := clus.nodes[0]

		tn := &varlogpb.TopicDescriptor{
			TopicID: types.TopicID(1),
			Status:  varlogpb.TopicStatusRunning,
		}

		err := mr.storage.registerTopic(tn)
		So(err, ShouldBeNil)

		snIDs := make([]types.StorageNodeID, rep)
		for i := range snIDs {
			snIDs[i] = types.MinStorageNodeID + types.StorageNodeID(i)

			sn := &varlogpb.StorageNodeDescriptor{
				StorageNode: varlogpb.StorageNode{
					StorageNodeID: snIDs[i],
				},
			}

			err := mr.storage.registerStorageNode(sn)
			So(err, ShouldBeNil)
		}

		lsID := types.MinLogStreamID
		ls := makeLogStream(types.TopicID(1), lsID, snIDs)
		err = mr.storage.registerLogStream(ls)
		So(err, ShouldBeNil)

		curLen := uint64(2)
		curVer := types.Version(1)

		for _, snID := range snIDs {
			report := makeUncommitReport(snID, curVer, types.InvalidGLSN, lsID, types.MinLLSN, curLen)
			mr.applyReport(&mrpb.Reports{Reports: []*mrpb.Report{report}}) //nolint:errcheck,revive // TODO:: Handle an error returned.
		}

		Convey("When Some LogStream reports dirty report", func(ctx C) {
			report := makeUncommitReport(snIDs[0], curVer+1, types.InvalidGLSN, lsID, types.MinLLSN, curLen-1)
			mr.applyReport(&mrpb.Reports{Reports: []*mrpb.Report{report}}) //nolint:errcheck,revive // TODO:: Handle an error returned.

			Convey("Then, it should ignored", func(ctx C) {
				r, ok := mr.storage.LookupUncommitReport(lsID, snIDs[0])
				So(ok, ShouldBeTrue)
				So(r.UncommittedLLSNLength, ShouldEqual, curLen)
			})
		})
	})
}

func TestMRCalculateCommit(t *testing.T) {
	Convey("Calculate commit", t, func(ctx C) {
		clus := newMetadataRepoCluster(1, 2, false)
		Reset(func() {
			clus.closeNoErrors(t)
		})
		mr := clus.nodes[0]

		snIDs := make([]types.StorageNodeID, 2)
		for i := range snIDs {
			snIDs[i] = types.MinStorageNodeID + types.StorageNodeID(i)
			sn := &varlogpb.StorageNodeDescriptor{
				StorageNode: varlogpb.StorageNode{
					StorageNodeID: snIDs[i],
				},
			}

			err := mr.storage.registerStorageNode(sn)
			So(err, ShouldBeNil)
		}

		err := mr.storage.registerTopic(&varlogpb.TopicDescriptor{TopicID: types.TopicID(1)})
		So(err, ShouldBeNil)

		lsID := types.MinLogStreamID
		ls := makeLogStream(types.TopicID(1), lsID, snIDs)
		err = mr.storage.registerLogStream(ls)
		So(err, ShouldBeNil)

		Convey("LogStream which all reports have not arrived cannot be commit", func(ctx C) {
			report := makeUncommitReport(snIDs[0], types.InvalidVersion, types.InvalidGLSN, lsID, types.MinLLSN, 2)
			mr.applyReport(&mrpb.Reports{Reports: []*mrpb.Report{report}}) //nolint:errcheck,revive // TODO:: Handle an error returned.

			replicas := mr.storage.LookupUncommitReports(lsID)
			_, minVer, _, nrCommit := mr.calculateCommit(replicas)
			So(nrCommit, ShouldEqual, 0)
			So(minVer, ShouldEqual, types.InvalidVersion)
		})

		Convey("LogStream which all reports are disjoint cannot be commit", func(ctx C) {
			report := makeUncommitReport(snIDs[0], types.Version(10), types.GLSN(10), lsID, types.MinLLSN+types.LLSN(5), 1)
			mr.applyReport(&mrpb.Reports{Reports: []*mrpb.Report{report}}) //nolint:errcheck,revive // TODO:: Handle an error returned.

			report = makeUncommitReport(snIDs[1], types.Version(7), types.GLSN(7), lsID, types.MinLLSN+types.LLSN(3), 2)
			mr.applyReport(&mrpb.Reports{Reports: []*mrpb.Report{report}}) //nolint:errcheck,revive // TODO:: Handle an error returned.

			replicas := mr.storage.LookupUncommitReports(lsID)
			knownVer, minVer, _, nrCommit := mr.calculateCommit(replicas)
			So(nrCommit, ShouldEqual, 0)
			So(knownVer, ShouldEqual, types.Version(10))
			So(minVer, ShouldEqual, types.Version(7))
		})

		Convey("LogStream Should be commit where replication is completed", func(ctx C) {
			report := makeUncommitReport(snIDs[0], types.Version(10), types.GLSN(10), lsID, types.MinLLSN+types.LLSN(3), 3)
			mr.applyReport(&mrpb.Reports{Reports: []*mrpb.Report{report}}) //nolint:errcheck,revive // TODO:: Handle an error returned.

			report = makeUncommitReport(snIDs[1], types.Version(9), types.GLSN(9), lsID, types.MinLLSN+types.LLSN(3), 2)
			mr.applyReport(&mrpb.Reports{Reports: []*mrpb.Report{report}}) //nolint:errcheck,revive // TODO:: Handle an error returned.

			replicas := mr.storage.LookupUncommitReports(lsID)
			knownVer, minVer, _, nrCommit := mr.calculateCommit(replicas)
			So(nrCommit, ShouldEqual, 2)
			So(minVer, ShouldEqual, types.Version(9))
			So(knownVer, ShouldEqual, types.Version(10))
		})
	})
}

func TestMRGlobalCommit(t *testing.T) {
	Convey("Calculate commit", t, func(ctx C) {
		topicID := types.TopicID(1)
		rep := 2
		clus := newMetadataRepoCluster(1, rep, false)
		Reset(func() {
			clus.closeNoErrors(t)
		})

		mr := clus.nodes[0]

		snIDs := make([][]types.StorageNodeID, 2)
		for i := range snIDs {
			snIDs[i] = make([]types.StorageNodeID, rep)
			for j := range snIDs[i] {
				snIDs[i][j] = types.MinStorageNodeID + types.StorageNodeID(i*2+j)

				sn := &varlogpb.StorageNodeDescriptor{
					StorageNode: varlogpb.StorageNode{
						StorageNodeID: snIDs[i][j],
					},
				}

				err := mr.storage.registerStorageNode(sn)
				So(err, ShouldBeNil)
			}
		}

		err := mr.storage.registerTopic(&varlogpb.TopicDescriptor{TopicID: types.TopicID(1)})
		So(err, ShouldBeNil)

		lsIds := make([]types.LogStreamID, 2)
		for i := range lsIds {
			lsIds[i] = types.MinLogStreamID + types.LogStreamID(i)
		}

		for i, lsID := range lsIds {
			ls := makeLogStream(types.TopicID(1), lsID, snIDs[i])
			err := mr.storage.registerLogStream(ls)
			So(err, ShouldBeNil)
		}

		So(clus.Start(), ShouldBeNil)
		So(testutil.CompareWaitN(10, func() bool {
			return clus.healthCheckAll()
		}), ShouldBeTrue)

		Convey("global commit", func(ctx C) {
			So(testutil.CompareWaitN(10, func() bool {
				report := makeUncommitReport(snIDs[0][0], types.InvalidVersion, types.InvalidGLSN, lsIds[0], types.MinLLSN, 2)
				return mr.proposeReport(report.StorageNodeID, report.UncommitReport) == nil
			}), ShouldBeTrue)

			So(testutil.CompareWaitN(10, func() bool {
				report := makeUncommitReport(snIDs[0][1], types.InvalidVersion, types.InvalidGLSN, lsIds[0], types.MinLLSN, 2)
				return mr.proposeReport(report.StorageNodeID, report.UncommitReport) == nil
			}), ShouldBeTrue)

			So(testutil.CompareWaitN(10, func() bool {
				report := makeUncommitReport(snIDs[1][0], types.InvalidVersion, types.InvalidGLSN, lsIds[1], types.MinLLSN, 4)
				return mr.proposeReport(report.StorageNodeID, report.UncommitReport) == nil
			}), ShouldBeTrue)

			So(testutil.CompareWaitN(10, func() bool {
				report := makeUncommitReport(snIDs[1][1], types.InvalidVersion, types.InvalidGLSN, lsIds[1], types.MinLLSN, 3)
				return mr.proposeReport(report.StorageNodeID, report.UncommitReport) == nil
			}), ShouldBeTrue)

			// global commit (2, 3) highest glsn: 5
			So(testutil.CompareWaitN(10, func() bool {
				hwm, _ := mr.GetLastCommitResults().LastHighWatermark(topicID, -1)
				return hwm == types.GLSN(5)
			}), ShouldBeTrue)

			Convey("LogStream should be dedup", func(ctx C) {
				So(testutil.CompareWaitN(10, func() bool {
					report := makeUncommitReport(snIDs[0][0], types.InvalidVersion, types.InvalidGLSN, lsIds[0], types.MinLLSN, 3)
					return mr.proposeReport(report.StorageNodeID, report.UncommitReport) == nil
				}), ShouldBeTrue)

				So(testutil.CompareWaitN(10, func() bool {
					report := makeUncommitReport(snIDs[0][1], types.InvalidVersion, types.InvalidGLSN, lsIds[0], types.MinLLSN, 2)
					return mr.proposeReport(report.StorageNodeID, report.UncommitReport) == nil
				}), ShouldBeTrue)

				time.Sleep(vtesting.TimeoutUnitTimesFactor(1))

				So(testutil.CompareWaitN(50, func() bool {
					hwm, _ := mr.GetLastCommitResults().LastHighWatermark(topicID, -1)
					return hwm == types.GLSN(5)
				}), ShouldBeTrue)
			})

			Convey("LogStream which have wrong Version but have uncommitted should commit", func(ctx C) {
				So(testutil.CompareWaitN(10, func() bool {
					report := makeUncommitReport(snIDs[0][0], types.InvalidVersion, types.InvalidGLSN, lsIds[0], types.MinLLSN, 6)
					return mr.proposeReport(report.StorageNodeID, report.UncommitReport) == nil
				}), ShouldBeTrue)

				So(testutil.CompareWaitN(10, func() bool {
					report := makeUncommitReport(snIDs[0][1], types.InvalidVersion, types.InvalidGLSN, lsIds[0], types.MinLLSN, 6)
					return mr.proposeReport(report.StorageNodeID, report.UncommitReport) == nil
				}), ShouldBeTrue)

				So(testutil.CompareWaitN(10, func() bool {
					hwm, _ := mr.GetLastCommitResults().LastHighWatermark(topicID, -1)
					return hwm == types.GLSN(9)
				}), ShouldBeTrue)
			})

			Convey("The LastHighWatermark of the new topic should be InvalidGLSN", func(ctx C) {
				hwm, _ := mr.GetLastCommitResults().LastHighWatermark(topicID, -1)
				So(hwm, ShouldNotEqual, types.InvalidGLSN)

				hwm, _ = mr.GetLastCommitResults().LastHighWatermark(topicID+1, -1)
				So(hwm, ShouldEqual, types.InvalidGLSN)
			})
		})
	})
}

func TestMRGlobalCommitConsistency(t *testing.T) {
	Convey("Given 2 mr nodes & 5 log streams", t, func(ctx C) {
		rep := 1
		nrNodes := 2
		nrLS := 5
		topicID := types.TopicID(1)

		clus := newMetadataRepoCluster(nrNodes, rep, false)
		Reset(func() {
			clus.closeNoErrors(t)
		})

		snIDs := make([]types.StorageNodeID, rep)
		for i := range snIDs {
			snIDs[i] = types.MinStorageNodeID + types.StorageNodeID(i)

			sn := &varlogpb.StorageNodeDescriptor{
				StorageNode: varlogpb.StorageNode{
					StorageNodeID: snIDs[i],
				},
			}

			for j := 0; j < nrNodes; j++ {
				err := clus.nodes[j].storage.registerStorageNode(sn)
				So(err, ShouldBeNil)
			}
		}

		for i := 0; i < nrNodes; i++ {
			err := clus.nodes[i].storage.registerTopic(&varlogpb.TopicDescriptor{TopicID: types.TopicID(1)})
			So(err, ShouldBeNil)
		}

		lsIDs := make([]types.LogStreamID, nrLS)
		for i := range lsIDs {
			lsIDs[i] = types.MinLogStreamID + types.LogStreamID(i)
		}

		for _, lsID := range lsIDs {
			ls := makeLogStream(types.TopicID(1), lsID, snIDs)
			for i := 0; i < nrNodes; i++ {
				err := clus.nodes[i].storage.registerLogStream(ls)
				So(err, ShouldBeNil)
			}
		}

		So(clus.Start(), ShouldBeNil)
		So(testutil.CompareWaitN(10, func() bool {
			return clus.healthCheckAll()
		}), ShouldBeTrue)

		Convey("Then, it should calculate same glsn for each log streams", func(ctx C) {
			for i := 0; i < 100; i++ {
				var report *mrpb.Report
				for j, lsID := range lsIDs {
					if j == 0 {
						report = makeUncommitReport(snIDs[0], types.Version(i), types.GLSN(i*nrLS), lsID, types.LLSN(i+1), 1)
					} else {
						report = appendUncommitReport(report, types.Version(i), types.GLSN(i*nrLS), lsID, types.LLSN(i+1), 1)
					}
				}

				So(testutil.CompareWaitN(10, func() bool {
					return clus.nodes[0].proposeReport(report.StorageNodeID, report.UncommitReport) == nil
				}), ShouldBeTrue)

				for j := 0; j < nrNodes; j++ {
					So(testutil.CompareWaitN(10, func() bool {
						return clus.nodes[j].storage.GetLastCommitVersion() == types.Version(i+1)
					}), ShouldBeTrue)

					for k, lsID := range lsIDs {
						So(clus.nodes[j].getLastCommitted(topicID, lsID, -1), ShouldEqual, types.GLSN((nrLS*i)+k+1))
					}
				}
			}
		})
	})
}

func TestMRSimpleReportNCommit(t *testing.T) {
	Convey("UncommitReport should be committed", t, func(ctx C) {
		clus := newMetadataRepoCluster(1, 1, false)
		Reset(func() {
			clus.closeNoErrors(t)
		})
		So(clus.Start(), ShouldBeNil)
		So(testutil.CompareWaitN(10, func() bool {
			return clus.healthCheckAll()
		}), ShouldBeTrue)

		snID := types.MinStorageNodeID
		snIDs := make([]types.StorageNodeID, 0, 1)
		snIDs = append(snIDs, snID)

		lsID := types.LogStreamID(snID)

		sn := &varlogpb.StorageNodeDescriptor{
			StorageNode: varlogpb.StorageNode{
				StorageNodeID: snID,
			},
		}

		rctx, cancel := context.WithTimeout(context.Background(), vtesting.TimeoutUnitTimesFactor(50))
		defer cancel()

		err := clus.nodes[0].RegisterStorageNode(rctx, sn)
		So(err, ShouldBeNil)

		So(testutil.CompareWaitN(50, func() bool {
			return clus.reporterClientFac.(*DummyStorageNodeClientFactory).lookupClient(snID) != nil
		}), ShouldBeTrue)

		err = clus.nodes[0].RegisterTopic(context.TODO(), types.TopicID(1))
		So(err, ShouldBeNil)

		ls := makeLogStream(types.TopicID(1), lsID, snIDs)
		rctx, cancel = context.WithTimeout(context.Background(), vtesting.TimeoutUnitTimesFactor(50))
		defer cancel()

		err = clus.nodes[0].RegisterLogStream(rctx, ls)
		So(err, ShouldBeNil)

		reporterClient := clus.reporterClientFac.(*DummyStorageNodeClientFactory).lookupClient(snID)
		reporterClient.increaseUncommitted(0)

		So(testutil.CompareWaitN(50, func() bool {
			return reporterClient.numUncommitted(0) == 0
		}), ShouldBeTrue)
	})
}

func TestMRRequestMap(t *testing.T) {
	Convey("requestMap should have request when wait ack", t, func(ctx C) {
		clus := newMetadataRepoCluster(1, 1, false)
		Reset(func() {
			clus.closeNoErrors(t)
		})
		mr := clus.nodes[0]

		sn := &varlogpb.StorageNodeDescriptor{
			StorageNode: varlogpb.StorageNode{
				StorageNodeID: types.StorageNodeID(0),
			},
		}

		requestNum := mr.requestNum.Load()

		var wg sync.WaitGroup
		var st sync.WaitGroup

		st.Add(1)
		wg.Add(1)
		go func() {
			defer wg.Done()
			rctx, cancel := context.WithTimeout(context.Background(), vtesting.TimeoutUnitTimesFactor(1))
			defer cancel()
			st.Done()
			mr.RegisterStorageNode(rctx, sn) //nolint:errcheck,revive // TODO:: Handle an error returned.
		}()

		st.Wait()
		So(testutil.CompareWaitN(1, func() bool {
			_, ok := mr.requestMap.Load(requestNum + 1)
			return ok
		}), ShouldBeTrue)

		wg.Wait()
	})

	Convey("requestMap should ignore request that have different nodeIndex", t, func(ctx C) {
		clus := newMetadataRepoCluster(1, 1, false)
		Reset(func() {
			clus.closeNoErrors(t)
		})
		mr := clus.nodes[0]

		sn := &varlogpb.StorageNodeDescriptor{
			StorageNode: varlogpb.StorageNode{
				StorageNodeID: types.StorageNodeID(0),
			},
		}

		var st sync.WaitGroup
		var wg sync.WaitGroup
		st.Add(1)
		wg.Add(1)
		go func() {
			defer wg.Done()
			st.Done()

			testutil.CompareWaitN(50, func() bool {
				_, ok := mr.requestMap.Load(uint64(1))
				return ok
			})

			dummy := &committedEntry{
				entry: &mrpb.RaftEntry{
					NodeIndex:    2,
					RequestIndex: uint64(1),
				},
			}
			mr.commitC <- dummy
		}()

		st.Wait()
		rctx, cancel := context.WithTimeout(context.Background(), vtesting.TimeoutUnitTimesFactor(2))
		defer cancel()
		err := mr.RegisterStorageNode(rctx, sn)

		wg.Wait()
		So(err, ShouldNotBeNil)
	})

	Convey("requestMap should delete request when context timeout", t, func(ctx C) {
		clus := newMetadataRepoCluster(1, 1, false)
		Reset(func() {
			clus.closeNoErrors(t)
		})
		mr := clus.nodes[0]

		sn := &varlogpb.StorageNodeDescriptor{
			StorageNode: varlogpb.StorageNode{
				StorageNodeID: types.StorageNodeID(0),
			},
		}

		rctx, cancel := context.WithTimeout(context.Background(), vtesting.TimeoutUnitTimesFactor(1))
		defer cancel()

		requestNum := mr.requestNum.Load()
		err := mr.RegisterStorageNode(rctx, sn)
		So(err, ShouldNotBeNil)

		_, ok := mr.requestMap.Load(requestNum + 1)
		So(ok, ShouldBeFalse)
	})

	Convey("requestMap should delete after ack", t, func(ctx C) {
		clus := newMetadataRepoCluster(1, 1, false)
		Reset(func() {
			clus.closeNoErrors(t)
		})
		So(clus.Start(), ShouldBeNil)
		So(testutil.CompareWaitN(50, func() bool {
			return clus.healthCheckAll()
		}), ShouldBeTrue)

		mr := clus.nodes[0]

		So(testutil.CompareWaitN(50, func() bool {
			return mr.storage.LookupEndpoint(mr.nodeID) != ""
		}), ShouldBeTrue)

		sn := &varlogpb.StorageNodeDescriptor{
			StorageNode: varlogpb.StorageNode{
				StorageNodeID: types.StorageNodeID(0),
			},
		}

		requestNum := mr.requestNum.Load()
		err := mr.RegisterStorageNode(context.TODO(), sn)
		So(err, ShouldBeNil)

		_, ok := mr.requestMap.Load(requestNum + 1)
		So(ok, ShouldBeFalse)
	})
}

func TestMRGetLastCommitted(t *testing.T) {
	Convey("getLastCommitted", t, func(ctx C) {
		rep := 2
		topicID := types.TopicID(1)
		clus := newMetadataRepoCluster(1, rep, false)
		Reset(func() {
			clus.closeNoErrors(t)
		})

		mr := clus.nodes[0]

		snIDs := make([][]types.StorageNodeID, 2)
		for i := range snIDs {
			snIDs[i] = make([]types.StorageNodeID, rep)
			for j := range snIDs[i] {
				snIDs[i][j] = types.MinStorageNodeID + types.StorageNodeID(i*2+j)

				sn := &varlogpb.StorageNodeDescriptor{
					StorageNode: varlogpb.StorageNode{
						StorageNodeID: snIDs[i][j],
					},
				}

				err := mr.storage.registerStorageNode(sn)
				So(err, ShouldBeNil)
			}
		}

		lsIds := make([]types.LogStreamID, 2)
		for i := range lsIds {
			lsIds[i] = types.MinLogStreamID + types.LogStreamID(i)
		}

		err := mr.storage.registerTopic(&varlogpb.TopicDescriptor{TopicID: types.TopicID(1)})
		So(err, ShouldBeNil)

		for i, lsID := range lsIds {
			ls := makeLogStream(types.TopicID(1), lsID, snIDs[i])
			err := mr.storage.registerLogStream(ls)
			So(err, ShouldBeNil)
		}

		So(clus.Start(), ShouldBeNil)
		So(testutil.CompareWaitN(10, func() bool {
			return clus.healthCheckAll()
		}), ShouldBeTrue)

		Convey("getLastCommitted should return last committed Version", func(ctx C) {
			preVersion := mr.storage.GetLastCommitVersion()

			So(testutil.CompareWaitN(10, func() bool {
				report := makeUncommitReport(snIDs[0][0], types.InvalidVersion, types.InvalidGLSN, lsIds[0], types.MinLLSN, 2)
				return mr.proposeReport(report.StorageNodeID, report.UncommitReport) == nil
			}), ShouldBeTrue)

			So(testutil.CompareWaitN(10, func() bool {
				report := makeUncommitReport(snIDs[0][1], types.InvalidVersion, types.InvalidGLSN, lsIds[0], types.MinLLSN, 2)
				return mr.proposeReport(report.StorageNodeID, report.UncommitReport) == nil
			}), ShouldBeTrue)

			So(testutil.CompareWaitN(10, func() bool {
				report := makeUncommitReport(snIDs[1][0], types.InvalidVersion, types.InvalidGLSN, lsIds[1], types.MinLLSN, 4)
				return mr.proposeReport(report.StorageNodeID, report.UncommitReport) == nil
			}), ShouldBeTrue)

			So(testutil.CompareWaitN(10, func() bool {
				report := makeUncommitReport(snIDs[1][1], types.InvalidVersion, types.InvalidGLSN, lsIds[1], types.MinLLSN, 3)
				return mr.proposeReport(report.StorageNodeID, report.UncommitReport) == nil
			}), ShouldBeTrue)

			// global commit (2, 3) highest glsn: 5
			So(testutil.CompareWaitN(10, func() bool {
				hwm, _ := mr.GetLastCommitResults().LastHighWatermark(topicID, -1)
				return hwm == types.GLSN(5)
			}), ShouldBeTrue)

			latest := mr.storage.getLastCommitResultsNoLock()
			base := mr.storage.lookupNextCommitResultsNoLock(preVersion)

			So(mr.numCommitSince(topicID, lsIds[0], base, latest, -1), ShouldEqual, 2)
			So(mr.numCommitSince(topicID, lsIds[1], base, latest, -1), ShouldEqual, 3)

			Convey("getLastCommitted should return same if not committed", func(ctx C) {
				for i := 0; i < 10; i++ {
					preVersion := mr.storage.GetLastCommitVersion()

					So(testutil.CompareWaitN(10, func() bool {
						report := makeUncommitReport(snIDs[1][0], types.InvalidVersion, types.InvalidGLSN, lsIds[1], types.MinLLSN, uint64(4+i))
						return mr.proposeReport(report.StorageNodeID, report.UncommitReport) == nil
					}), ShouldBeTrue)

					So(testutil.CompareWaitN(10, func() bool {
						report := makeUncommitReport(snIDs[1][1], types.InvalidVersion, types.InvalidGLSN, lsIds[1], types.MinLLSN, uint64(4+i))
						return mr.proposeReport(report.StorageNodeID, report.UncommitReport) == nil
					}), ShouldBeTrue)

					So(testutil.CompareWaitN(50, func() bool {
						hwm, _ := mr.GetLastCommitResults().LastHighWatermark(topicID, -1)
						return hwm == types.GLSN(6+i)
					}), ShouldBeTrue)

					latest := mr.storage.getLastCommitResultsNoLock()
					base := mr.storage.lookupNextCommitResultsNoLock(preVersion)

					So(mr.numCommitSince(topicID, lsIds[0], base, latest, -1), ShouldEqual, 0)
					So(mr.numCommitSince(topicID, lsIds[1], base, latest, -1), ShouldEqual, 1)
				}
			})

			Convey("getLastCommitted should return same for sealed LS", func(ctx C) {
				rctx, cancel := context.WithTimeout(context.Background(), vtesting.TimeoutUnitTimesFactor(50))
				defer cancel()
				_, err := mr.Seal(rctx, lsIds[1])
				So(err, ShouldBeNil)

				for i := 0; i < 10; i++ {
					preVersion := mr.storage.GetLastCommitVersion()

					So(testutil.CompareWaitN(10, func() bool {
						report := makeUncommitReport(snIDs[0][0], types.InvalidVersion, types.InvalidGLSN, lsIds[0], types.MinLLSN, uint64(3+i))
						return mr.proposeReport(report.StorageNodeID, report.UncommitReport) == nil
					}), ShouldBeTrue)

					So(testutil.CompareWaitN(10, func() bool {
						report := makeUncommitReport(snIDs[0][1], types.InvalidVersion, types.InvalidGLSN, lsIds[0], types.MinLLSN, uint64(3+i))
						return mr.proposeReport(report.StorageNodeID, report.UncommitReport) == nil
					}), ShouldBeTrue)

					So(testutil.CompareWaitN(10, func() bool {
						report := makeUncommitReport(snIDs[1][0], types.InvalidVersion, types.InvalidGLSN, lsIds[1], types.MinLLSN, uint64(4+i))
						return mr.proposeReport(report.StorageNodeID, report.UncommitReport) == nil
					}), ShouldBeTrue)

					So(testutil.CompareWaitN(10, func() bool {
						report := makeUncommitReport(snIDs[1][1], types.InvalidVersion, types.InvalidGLSN, lsIds[1], types.MinLLSN, uint64(4+i))
						return mr.proposeReport(report.StorageNodeID, report.UncommitReport) == nil
					}), ShouldBeTrue)

					So(testutil.CompareWaitN(10, func() bool {
						hwm, _ := mr.GetLastCommitResults().LastHighWatermark(topicID, -1)
						return hwm == types.GLSN(6+i)
					}), ShouldBeTrue)

					latest := mr.storage.getLastCommitResultsNoLock()
					base := mr.storage.lookupNextCommitResultsNoLock(preVersion)

					So(mr.numCommitSince(topicID, lsIds[0], base, latest, -1), ShouldEqual, 1)
					So(mr.numCommitSince(topicID, lsIds[1], base, latest, -1), ShouldEqual, 0)
				}
			})
		})
	})
}

func TestMRSeal(t *testing.T) {
	Convey("seal", t, func(ctx C) {
		rep := 2
		topicID := types.TopicID(1)

		clus := newMetadataRepoCluster(1, rep, false)
		Reset(func() {
			clus.closeNoErrors(t)
		})

		mr := clus.nodes[0]

		snIDs := make([][]types.StorageNodeID, 2)
		for i := range snIDs {
			snIDs[i] = make([]types.StorageNodeID, rep)
			for j := range snIDs[i] {
				snIDs[i][j] = types.MinStorageNodeID + types.StorageNodeID(i*2+j)

				sn := &varlogpb.StorageNodeDescriptor{
					StorageNode: varlogpb.StorageNode{
						StorageNodeID: snIDs[i][j],
					},
				}

				err := mr.storage.registerStorageNode(sn)
				So(err, ShouldBeNil)
			}
		}

		lsIDs := make([]types.LogStreamID, 2)
		for i := range lsIDs {
			lsIDs[i] = types.MinLogStreamID + types.LogStreamID(i)
		}

		err := mr.storage.registerTopic(&varlogpb.TopicDescriptor{TopicID: types.TopicID(1)})
		So(err, ShouldBeNil)

		for i, lsID := range lsIDs {
			ls := makeLogStream(types.TopicID(1), lsID, snIDs[i])
			err := mr.storage.registerLogStream(ls)
			So(err, ShouldBeNil)
		}

		So(clus.Start(), ShouldBeNil)
		So(testutil.CompareWaitN(10, func() bool {
			return clus.healthCheckAll()
		}), ShouldBeTrue)

		Convey("Seal should commit and return last committed", func(ctx C) {
			So(testutil.CompareWaitN(10, func() bool {
				report := makeUncommitReport(snIDs[0][0], types.InvalidVersion, types.InvalidGLSN, lsIDs[0], types.MinLLSN, 2)
				return mr.proposeReport(report.StorageNodeID, report.UncommitReport) == nil
			}), ShouldBeTrue)

			So(testutil.CompareWaitN(10, func() bool {
				report := makeUncommitReport(snIDs[0][1], types.InvalidVersion, types.InvalidGLSN, lsIDs[0], types.MinLLSN, 2)
				return mr.proposeReport(report.StorageNodeID, report.UncommitReport) == nil
			}), ShouldBeTrue)

			So(testutil.CompareWaitN(10, func() bool {
				report := makeUncommitReport(snIDs[1][0], types.InvalidVersion, types.InvalidGLSN, lsIDs[1], types.MinLLSN, 4)
				return mr.proposeReport(report.StorageNodeID, report.UncommitReport) == nil
			}), ShouldBeTrue)

			So(testutil.CompareWaitN(10, func() bool {
				report := makeUncommitReport(snIDs[1][1], types.InvalidVersion, types.InvalidGLSN, lsIDs[1], types.MinLLSN, 3)
				return mr.proposeReport(report.StorageNodeID, report.UncommitReport) == nil
			}), ShouldBeTrue)

			rctx, cancel := context.WithTimeout(context.Background(), vtesting.TimeoutUnitTimesFactor(50))
			defer cancel()
			lc, err := mr.Seal(rctx, lsIDs[1])
			So(err, ShouldBeNil)
			So(lc, ShouldEqual, mr.getLastCommitted(topicID, lsIDs[1], -1))

			Convey("Seal should return same last committed", func(ctx C) {
				for i := 0; i < 10; i++ {
					rctx, cancel := context.WithTimeout(context.Background(), vtesting.TimeoutUnitTimesFactor(50))
					defer cancel()
					lc2, err := mr.Seal(rctx, lsIDs[1])
					So(err, ShouldBeNil)
					So(lc2, ShouldEqual, lc)
				}
			})
		})
	})
}

func TestMRUnseal(t *testing.T) {
	Convey("unseal", t, func(ctx C) {
		rep := 2
		topicID := types.TopicID(1)

		clus := newMetadataRepoCluster(1, rep, false)
		Reset(func() {
			clus.closeNoErrors(t)
		})

		mr := clus.nodes[0]

		snIDs := make([][]types.StorageNodeID, 2)
		for i := range snIDs {
			snIDs[i] = make([]types.StorageNodeID, rep)
			for j := range snIDs[i] {
				snIDs[i][j] = types.MinStorageNodeID + types.StorageNodeID(i*2+j)

				sn := &varlogpb.StorageNodeDescriptor{
					StorageNode: varlogpb.StorageNode{
						StorageNodeID: snIDs[i][j],
					},
				}

				err := mr.storage.registerStorageNode(sn)
				So(err, ShouldBeNil)
			}
		}

		lsIDs := make([]types.LogStreamID, 2)
		for i := range lsIDs {
			lsIDs[i] = types.MinLogStreamID + types.LogStreamID(i)
		}

		err := mr.storage.registerTopic(&varlogpb.TopicDescriptor{TopicID: types.TopicID(1)})
		So(err, ShouldBeNil)

		for i, lsID := range lsIDs {
			ls := makeLogStream(types.TopicID(1), lsID, snIDs[i])
			err := mr.storage.registerLogStream(ls)
			So(err, ShouldBeNil)
		}

		So(clus.Start(), ShouldBeNil)
		So(testutil.CompareWaitN(10, func() bool {
			return clus.healthCheckAll()
		}), ShouldBeTrue)

		So(testutil.CompareWaitN(10, func() bool {
			report := makeUncommitReport(snIDs[0][0], types.InvalidVersion, types.InvalidGLSN, lsIDs[0], types.MinLLSN, 2)
			return mr.proposeReport(report.StorageNodeID, report.UncommitReport) == nil
		}), ShouldBeTrue)

		So(testutil.CompareWaitN(10, func() bool {
			report := makeUncommitReport(snIDs[0][1], types.InvalidVersion, types.InvalidGLSN, lsIDs[0], types.MinLLSN, 2)
			return mr.proposeReport(report.StorageNodeID, report.UncommitReport) == nil
		}), ShouldBeTrue)

		So(testutil.CompareWaitN(10, func() bool {
			report := makeUncommitReport(snIDs[1][0], types.InvalidVersion, types.InvalidGLSN, lsIDs[1], types.MinLLSN, 4)
			return mr.proposeReport(report.StorageNodeID, report.UncommitReport) == nil
		}), ShouldBeTrue)

		So(testutil.CompareWaitN(10, func() bool {
			report := makeUncommitReport(snIDs[1][1], types.InvalidVersion, types.InvalidGLSN, lsIDs[1], types.MinLLSN, 3)
			return mr.proposeReport(report.StorageNodeID, report.UncommitReport) == nil
		}), ShouldBeTrue)

		So(testutil.CompareWaitN(10, func() bool {
			hwm, _ := mr.GetLastCommitResults().LastHighWatermark(topicID, -1)
			return hwm == types.GLSN(5)
		}), ShouldBeTrue)

		rctx, cancel := context.WithTimeout(context.Background(), vtesting.TimeoutUnitTimesFactor(50))
		defer cancel()
		sealedHWM, err := mr.Seal(rctx, lsIDs[1])

		So(err, ShouldBeNil)
		So(sealedHWM, ShouldEqual, mr.getLastCommitted(topicID, lsIDs[1], -1))

		So(testutil.CompareWaitN(10, func() bool {
			sealedVer := mr.getLastCommitVersion(topicID, lsIDs[1])

			for _, snID := range snIDs[1] {
				report := makeUncommitReport(snID, sealedVer, sealedHWM, lsIDs[1], types.LLSN(4), 0)
				if err := mr.proposeReport(report.StorageNodeID, report.UncommitReport); err != nil {
					return false
				}
			}

			meta, err := mr.GetMetadata(context.TODO())
			if err != nil {
				return false
			}

			ls := meta.GetLogStream(lsIDs[1])
			return ls.Status == varlogpb.LogStreamStatusSealed
		}), ShouldBeTrue)

		sealedVer := mr.getLastCommitVersion(topicID, lsIDs[1])

		Convey("Unealed LS should update report", func(ctx C) {
			rctx, cancel := context.WithTimeout(context.Background(), vtesting.TimeoutUnitTimesFactor(50))
			defer cancel()
			err := mr.Unseal(rctx, lsIDs[1])
			So(err, ShouldBeNil)

			for i := 1; i < 10; i++ {
				preVersion := mr.storage.GetLastCommitVersion()

				So(testutil.CompareWaitN(10, func() bool {
					report := makeUncommitReport(snIDs[1][0], sealedVer, sealedHWM, lsIDs[1], types.LLSN(4), uint64(i))
					return mr.proposeReport(report.StorageNodeID, report.UncommitReport) == nil
				}), ShouldBeTrue)

				So(testutil.CompareWaitN(10, func() bool {
					report := makeUncommitReport(snIDs[1][1], sealedVer, sealedHWM, lsIDs[1], types.LLSN(4), uint64(i))
					return mr.proposeReport(report.StorageNodeID, report.UncommitReport) == nil
				}), ShouldBeTrue)

				So(testutil.CompareWaitN(50, func() bool {
					hwm, _ := mr.GetLastCommitResults().LastHighWatermark(topicID, -1)
					return hwm == types.GLSN(5+i)
				}), ShouldBeTrue)

				latest := mr.storage.getLastCommitResultsNoLock()
				base := mr.storage.lookupNextCommitResultsNoLock(preVersion)

				So(mr.numCommitSince(topicID, lsIDs[1], base, latest, -1), ShouldEqual, 1)
			}
		})
	})
}

func TestMRUpdateLogStream(t *testing.T) {
	Convey("Given MR cluster", t, func(ctx C) {
		rep := 2
		nrStorageNode := rep + 1
		clus := newMetadataRepoCluster(1, rep, false)
		Reset(func() {
			clus.closeNoErrors(t)
		})

		So(clus.Start(), ShouldBeNil)
		So(testutil.CompareWaitN(10, func() bool {
			return clus.healthCheckAll()
		}), ShouldBeTrue)

		mr := clus.nodes[0]

		lsID := types.MinLogStreamID
		snIDs := make([]types.StorageNodeID, nrStorageNode)
		for i := range snIDs {
			snIDs[i] = types.MinStorageNodeID + types.StorageNodeID(i)

			clus.reporterClientFac.(*DummyStorageNodeClientFactory).registerLogStream(snIDs[i], []types.LogStreamID{lsID})

			sn := &varlogpb.StorageNodeDescriptor{
				StorageNode: varlogpb.StorageNode{
					StorageNodeID: snIDs[i],
				},
			}

			err := mr.RegisterStorageNode(context.TODO(), sn)
			So(err, ShouldBeNil)
		}

		err := mr.RegisterTopic(context.TODO(), types.TopicID(1))
		So(err, ShouldBeNil)

		ls := makeLogStream(types.TopicID(1), lsID, snIDs[0:rep])
		err = mr.RegisterLogStream(context.TODO(), ls)
		So(err, ShouldBeNil)
		So(testutil.CompareWaitN(1, func() bool {
			return mr.reportCollector.NumCommitter() == rep
		}), ShouldBeTrue)

		Convey("When Update LogStream", func() {
			rctx, cancel := context.WithTimeout(context.TODO(), vtesting.TimeoutUnitTimesFactor(50))
			defer cancel()
			_, err = mr.Seal(rctx, lsID)
			So(err, ShouldBeNil)

			updatedls := makeLogStream(types.TopicID(1), lsID, snIDs[1:rep+1])
			err = mr.UpdateLogStream(context.TODO(), updatedls)
			So(err, ShouldBeNil)

			Convey("Then unused report collector should be closed", func() {
				So(testutil.CompareWaitN(1, func() bool {
					return mr.reportCollector.NumCommitter() == rep
				}), ShouldBeTrue)

				err = mr.UnregisterStorageNode(context.TODO(), snIDs[0])
				So(err, ShouldBeNil)

				So(testutil.CompareWaitN(10, func() bool {
					reporterClient := clus.reporterClientFac.(*DummyStorageNodeClientFactory).lookupClient(snIDs[0])
					return reporterClient == nil
				}), ShouldBeTrue)

				So(testutil.CompareWaitN(1, func() bool {
					return mr.reportCollector.NumCommitter() == rep
				}), ShouldBeTrue)

				So(clus.reporterClientFac.(*DummyStorageNodeClientFactory).lookupClient(snIDs[1]) == nil, ShouldBeFalse)
				So(clus.reporterClientFac.(*DummyStorageNodeClientFactory).lookupClient(snIDs[2]) == nil, ShouldBeFalse)
			})
		})
	})
}

func TestMRUpdateLogStreamExclusive(t *testing.T) {
	Convey("Given MR cluster", t, func(ctx C) {
		rep := 2
		nrStorageNode := rep + 2
		clus := newMetadataRepoCluster(1, rep, false)
		Reset(func() {
			clus.closeNoErrors(t)
		})

		So(clus.Start(), ShouldBeNil)
		So(testutil.CompareWaitN(10, func() bool {
			return clus.healthCheckAll()
		}), ShouldBeTrue)

		mr := clus.nodes[0]

		lsID := types.MinLogStreamID
		snIDs := make([]types.StorageNodeID, nrStorageNode)
		for i := range snIDs {
			snIDs[i] = types.MinStorageNodeID + types.StorageNodeID(i)

			clus.reporterClientFac.(*DummyStorageNodeClientFactory).registerLogStream(snIDs[i], []types.LogStreamID{lsID})

			sn := &varlogpb.StorageNodeDescriptor{
				StorageNode: varlogpb.StorageNode{
					StorageNodeID: snIDs[i],
				},
			}

			err := mr.RegisterStorageNode(context.TODO(), sn)
			So(err, ShouldBeNil)
		}

		err := mr.RegisterTopic(context.TODO(), types.TopicID(1))
		So(err, ShouldBeNil)

		ls := makeLogStream(types.TopicID(1), lsID, snIDs[0:rep])
		err = mr.RegisterLogStream(context.TODO(), ls)
		So(err, ShouldBeNil)
		So(testutil.CompareWaitN(1, func() bool {
			return mr.reportCollector.NumCommitter() == rep
		}), ShouldBeTrue)

		Convey("When Update LogStream with no overlapping SN", func() {
			rctx, cancel := context.WithTimeout(context.TODO(), vtesting.TimeoutUnitTimesFactor(50))
			defer cancel()
			_, err = mr.Seal(rctx, lsID)
			So(err, ShouldBeNil)

			updatedls := makeLogStream(types.TopicID(1), lsID, snIDs[2:rep+2])
			err = mr.UpdateLogStream(context.TODO(), updatedls)

			Convey("Then it should be rejected", func() {
				So(err, ShouldNotBeNil)
			})
		})
	})
}

func TestMRUpdateLogStreamUnsafe(t *testing.T) {
	Convey("Given MR cluster", t, func(ctx C) {
		rep := 2
		nrStorageNode := rep + 2
		clus := newMetadataRepoCluster(1, rep, false)
		Reset(func() {
			clus.closeNoErrors(t)
		})

		So(clus.Start(), ShouldBeNil)
		So(testutil.CompareWaitN(10, func() bool {
			return clus.healthCheckAll()
		}), ShouldBeTrue)

		mr := clus.nodes[0]

		lsID := types.MinLogStreamID
		snIDs := make([]types.StorageNodeID, nrStorageNode)
		for i := range snIDs {
			snIDs[i] = types.MinStorageNodeID + types.StorageNodeID(i)

			clus.reporterClientFac.(*DummyStorageNodeClientFactory).registerLogStream(snIDs[i], []types.LogStreamID{lsID})

			sn := &varlogpb.StorageNodeDescriptor{
				StorageNode: varlogpb.StorageNode{
					StorageNodeID: snIDs[i],
				},
			}

			err := mr.RegisterStorageNode(context.TODO(), sn)
			So(err, ShouldBeNil)
		}

		err := mr.RegisterTopic(context.TODO(), types.TopicID(1))
		So(err, ShouldBeNil)

		ls := makeLogStream(types.TopicID(1), lsID, snIDs[0:rep])
		err = mr.RegisterLogStream(context.TODO(), ls)
		So(err, ShouldBeNil)
		So(testutil.CompareWaitN(1, func() bool {
			return mr.reportCollector.NumCommitter() == rep
		}), ShouldBeTrue)

		Convey("When Update LogStream with SN not reaching the last commit", func() {
			disableCommitSNID := snIDs[1]
			disableCommitSNCli := clus.reporterClientFac.(*DummyStorageNodeClientFactory).lookupClient(disableCommitSNID)
			disableCommitSNCli.DisableCommit()

			for _, snID := range snIDs[0:rep] {
				snCli := clus.reporterClientFac.(*DummyStorageNodeClientFactory).lookupClient(snID)
				snCli.increaseUncommitted(0)
			}

			So(testutil.CompareWaitN(10, func() bool {
				cr, _, _ := mr.GetLastCommitResults().LookupCommitResult(types.TopicID(1), lsID, -1)
				return cr.HighWatermark != types.InvalidGLSN
			}), ShouldBeTrue)

			rctx, cancel := context.WithTimeout(context.TODO(), vtesting.TimeoutUnitTimesFactor(50))
			defer cancel()
			_, err = mr.Seal(rctx, lsID)
			So(err, ShouldBeNil)

			updatedls := makeLogStream(types.TopicID(1), lsID, snIDs[1:rep+1])
			err = mr.UpdateLogStream(context.TODO(), updatedls)

			Convey("Then it should be rejected", func() {
				So(err, ShouldNotBeNil)
			})
		})
	})
}

func TestMRFailoverLeaderElection(t *testing.T) {
	Convey("Given MR cluster", t, func(ctx C) {
		nrRep := 1
		nrNode := 3

		clus := newMetadataRepoCluster(nrNode, nrRep, true)
		Reset(func() {
			clus.closeNoErrors(t)
		})
		So(clus.Start(), ShouldBeNil)
		So(testutil.CompareWaitN(10, func() bool {
			return clus.healthCheckAll()
		}), ShouldBeTrue)

		snIDs := make([]types.StorageNodeID, nrRep)
		for i := range snIDs {
			snIDs[i] = types.MinStorageNodeID + types.StorageNodeID(i)

			sn := &varlogpb.StorageNodeDescriptor{
				StorageNode: varlogpb.StorageNode{
					StorageNodeID: snIDs[i],
				},
			}

			rctx, cancel := context.WithTimeout(context.Background(), vtesting.TimeoutUnitTimesFactor(50))
			defer cancel()

			err := clus.nodes[0].RegisterStorageNode(rctx, sn)
			So(err, ShouldBeNil)
		}

		err := clus.nodes[0].RegisterTopic(context.TODO(), types.TopicID(1))
		So(err, ShouldBeNil)

		lsID := types.MinLogStreamID
		ls := makeLogStream(types.TopicID(1), lsID, snIDs)

		rctx, cancel := context.WithTimeout(context.Background(), vtesting.TimeoutUnitTimesFactor(50))
		defer cancel()

		err = clus.nodes[0].RegisterLogStream(rctx, ls)
		So(err, ShouldBeNil)

		reporterClient := clus.reporterClientFac.(*DummyStorageNodeClientFactory).lookupClient(snIDs[0])
		So(testutil.CompareWaitN(50, func() bool {
			return !reporterClient.getKnownVersion(0).Invalid()
		}), ShouldBeTrue)

		Convey("When node fail", func(ctx C) {
			leader := clus.leader()

			So(clus.leaderFail(), ShouldBeTrue)

			Convey("Then MR Cluster should elect", func(ctx C) {
				So(testutil.CompareWaitN(50, func() bool {
					return leader != clus.leader()
				}), ShouldBeTrue)

				prev := reporterClient.getKnownVersion(0)

				So(testutil.CompareWaitN(50, func() bool {
					return reporterClient.getKnownVersion(0) > prev
				}), ShouldBeTrue)
			})
		})
	})
}

func TestMRFailoverJoinNewNode(t *testing.T) {
	Convey("Given MR cluster", t, func(ctx C) {
		nrRep := 1
		nrNode := 3

		clus := newMetadataRepoCluster(nrNode, nrRep, false)
		Reset(func() {
			clus.closeNoErrors(t)
		})

		So(clus.Start(), ShouldBeNil)
		So(testutil.CompareWaitN(10, func() bool {
			return clus.healthCheckAll()
		}), ShouldBeTrue)

		snIDs := make([]types.StorageNodeID, nrRep)
		for i := range snIDs {
			snIDs[i] = types.MinStorageNodeID + types.StorageNodeID(i)

			sn := &varlogpb.StorageNodeDescriptor{
				StorageNode: varlogpb.StorageNode{
					StorageNodeID: snIDs[i],
				},
			}

			rctx, cancel := context.WithTimeout(context.Background(), vtesting.TimeoutUnitTimesFactor(200))
			defer cancel()

			err := clus.nodes[0].RegisterStorageNode(rctx, sn)
			So(err, ShouldBeNil)
		}

		err := clus.nodes[0].RegisterTopic(context.TODO(), types.TopicID(1))
		So(err, ShouldBeNil)

		lsID := types.MinLogStreamID
		ls := makeLogStream(types.TopicID(1), lsID, snIDs)

		rctx, cancel := context.WithTimeout(context.Background(), vtesting.TimeoutUnitTimesFactor(200))
		defer cancel()

		err = clus.nodes[0].RegisterLogStream(rctx, ls)
		So(err, ShouldBeNil)

		Convey("When new node join", func(ctx C) {
			newNode := nrNode
			nrNode += 1
			So(clus.appendMetadataRepo(), ShouldBeNil)
			So(clus.start(newNode), ShouldBeNil)

			info, err := clus.nodes[0].GetClusterInfo(context.Background(), types.ClusterID(0))
			So(err, ShouldBeNil)
			appliedIdx := info.AppliedIndex

			rctx, cancel := context.WithTimeout(context.Background(), vtesting.TimeoutUnitTimesFactor(200))
			defer cancel()

			So(clus.nodes[0].AddPeer(rctx,
				types.ClusterID(0),
				clus.nodes[newNode].nodeID,
				clus.peers[newNode]), ShouldBeNil)

			info, err = clus.nodes[0].GetClusterInfo(context.Background(), types.ClusterID(0))
			So(err, ShouldBeNil)
			So(info.AppliedIndex, ShouldBeGreaterThan, appliedIdx)

			Convey("Then new node should be member", func(ctx C) {
				So(testutil.CompareWaitN(200, func() bool {
					return clus.nodes[newNode].IsMember()
				}), ShouldBeTrue)

				Convey("Register to new node should be success", func(ctx C) {
					snID := snIDs[nrRep-1] + types.StorageNodeID(1)

					sn := &varlogpb.StorageNodeDescriptor{
						StorageNode: varlogpb.StorageNode{
							StorageNodeID: snID,
						},
					}

					rctx, cancel := context.WithTimeout(context.Background(), vtesting.TimeoutUnitTimesFactor(200))
					defer cancel()

					err := clus.nodes[newNode].RegisterStorageNode(rctx, sn)
					if rctx.Err() != nil {
						clus.logger.Info("complete with ctx error", zap.String("err", rctx.Err().Error()))
					}
					So(err, ShouldBeNil)

					meta, err := clus.nodes[newNode].GetMetadata(context.TODO())
					So(err, ShouldBeNil)
					So(meta.GetStorageNode(snID), ShouldNotBeNil)
				})
			})
		})

		Convey("When new nodes started but not yet joined", func(ctx C) {
			newNode := nrNode
			nrNode += 1
			So(clus.appendMetadataRepo(), ShouldBeNil)
			So(clus.start(newNode), ShouldBeNil)

			time.Sleep(10 * time.Second)

			Convey("Then it should not have member info", func(ctx C) {
				_, err := clus.nodes[newNode].GetClusterInfo(context.TODO(), types.ClusterID(0))
				So(status.Code(err), ShouldEqual, codes.Unavailable)

				cinfo, err := clus.nodes[0].GetClusterInfo(context.TODO(), types.ClusterID(0))
				So(err, ShouldBeNil)
				So(len(cinfo.Members), ShouldBeLessThan, nrNode)

				Convey("After join, it should have member info", func(ctx C) {
					info, err := clus.nodes[0].GetClusterInfo(context.Background(), types.ClusterID(0))
					So(err, ShouldBeNil)
					appliedIdx := info.AppliedIndex

					rctx, cancel := context.WithTimeout(context.Background(), vtesting.TimeoutUnitTimesFactor(200))
					defer cancel()

					So(clus.nodes[0].AddPeer(rctx,
						types.ClusterID(0),
						clus.nodes[newNode].nodeID,
						clus.peers[newNode]), ShouldBeNil)

					info, err = clus.nodes[0].GetClusterInfo(context.Background(), types.ClusterID(0))
					So(err, ShouldBeNil)
					So(info.AppliedIndex, ShouldBeGreaterThan, appliedIdx)

					So(testutil.CompareWaitN(200, func() bool {
						return clus.nodes[newNode].IsMember()
					}), ShouldBeTrue)

					Convey("Then proposal should be operated", func(ctx C) {
						snID := snIDs[nrRep-1] + types.StorageNodeID(1)

						sn := &varlogpb.StorageNodeDescriptor{
							StorageNode: varlogpb.StorageNode{
								StorageNodeID: snID,
							},
						}

						rctx, cancel := context.WithTimeout(context.Background(), vtesting.TimeoutUnitTimesFactor(200))
						defer cancel()

						err := clus.nodes[newNode].RegisterStorageNode(rctx, sn)
						if rctx.Err() != nil {
							clus.logger.Info("complete with ctx error", zap.String("err", rctx.Err().Error()))
						}
						So(err, ShouldBeNil)
					})
				})
			})
		})
	})
}

func TestMRFailoverLeaveNode(t *testing.T) {
	Convey("Given MR cluster", t, func(ctx C) {
		nrRep := 1
		nrNode := 3

		clus := newMetadataRepoCluster(nrNode, nrRep, false)
		Reset(func() {
			clus.closeNoErrors(t)
		})
		So(clus.Start(), ShouldBeNil)
		So(testutil.CompareWaitN(10, func() bool {
			return clus.healthCheckAll()
		}), ShouldBeTrue)

		leader := clus.leader()
		So(leader, ShouldBeGreaterThan, -1)

		Convey("When follower leave", func(ctx C) {
			leaveNode := (leader + 1) % nrNode
			checkNode := (leaveNode + 1) % nrNode

			So(testutil.CompareWaitN(50, func() bool {
				cinfo, _ := clus.nodes[checkNode].GetClusterInfo(context.TODO(), 0)
				return len(cinfo.Members) == nrNode
			}), ShouldBeTrue)

			clus.stop(leaveNode) //nolint:errcheck,revive // TODO:: Handle an error returned.

			info, err := clus.nodes[checkNode].GetClusterInfo(context.Background(), types.ClusterID(0))
			So(err, ShouldBeNil)
			appliedIdx := info.AppliedIndex

			rctx, cancel := context.WithTimeout(context.Background(), vtesting.TimeoutUnitTimesFactor(50))
			defer cancel()

			So(clus.nodes[checkNode].RemovePeer(rctx,
				types.ClusterID(0),
				clus.nodes[leaveNode].nodeID), ShouldBeNil)

			Convey("Then GetMembership should return 2 peers", func(ctx C) {
				cinfo, err := clus.nodes[checkNode].GetClusterInfo(context.TODO(), 0)
				So(err, ShouldBeNil)
				So(len(cinfo.Members), ShouldEqual, nrNode-1)
				So(cinfo.AppliedIndex, ShouldBeGreaterThan, appliedIdx)
			})
		})

		Convey("When leader leave", func(ctx C) {
			leaveNode := leader
			checkNode := (leaveNode + 1) % nrNode

			So(testutil.CompareWaitN(50, func() bool {
				cinfo, _ := clus.nodes[checkNode].GetClusterInfo(context.TODO(), 0)
				return len(cinfo.Members) == nrNode
			}), ShouldBeTrue)

			clus.stop(leaveNode) //nolint:errcheck,revive // TODO:: Handle an error returned.

			info, err := clus.nodes[checkNode].GetClusterInfo(context.Background(), types.ClusterID(0))
			So(err, ShouldBeNil)
			appliedIdx := info.AppliedIndex

			rctx, cancel := context.WithTimeout(context.Background(), vtesting.TimeoutUnitTimesFactor(50))
			defer cancel()

			So(clus.nodes[checkNode].RemovePeer(rctx,
				types.ClusterID(0),
				clus.nodes[leaveNode].nodeID), ShouldBeNil)

			Convey("Then GetMembership should return 2 peers", func(ctx C) {
				cinfo, err := clus.nodes[checkNode].GetClusterInfo(context.TODO(), 0)
				So(err, ShouldBeNil)
				So(len(cinfo.Members), ShouldEqual, nrNode-1)
				So(cinfo.AppliedIndex, ShouldBeGreaterThan, appliedIdx)
			})
		})
	})
}

func TestMRFailoverRestart(t *testing.T) {
	Convey("Given MR cluster with 5 peers", t, func(ctx C) {
		nrRep := 1
		nrNode := 5

		clus := newMetadataRepoCluster(nrNode, nrRep, false)
		Reset(func() {
			clus.closeNoErrors(t)
		})
		So(clus.Start(), ShouldBeNil)
		So(testutil.CompareWaitN(10, func() bool {
			return clus.healthCheckAll()
		}), ShouldBeTrue)

		leader := clus.leader()
		So(leader, ShouldBeGreaterThan, -1)

		Convey("When follower restart and new node joined", func(ctx C) {
			restartNode := (leader + 1) % nrNode

			So(testutil.CompareWaitN(50, func() bool {
				peers := clus.getMembersFromSnapshot(restartNode)
				return len(peers) > 0
			}), ShouldBeTrue)

			clus.restart(restartNode) //nolint:errcheck,revive // TODO:: Handle an error returned.

			newNode := nrNode
			nrNode += 1
			So(clus.appendMetadataRepo(), ShouldBeNil)
			So(clus.start(newNode), ShouldBeNil)

			rctx, cancel := context.WithTimeout(context.Background(), vtesting.TimeoutUnitTimesFactor(50))
			defer cancel()

			So(clus.nodes[leader].AddPeer(rctx,
				types.ClusterID(0),
				clus.nodes[newNode].nodeID,
				clus.peers[newNode]), ShouldBeNil)

			Convey("Then GetMembership should return 6 peers", func(ctx C) {
				So(testutil.CompareWaitN(100, func() bool {
					if !clus.healthCheck(restartNode) {
						return false
					}

					cinfo, err := clus.nodes[restartNode].GetClusterInfo(context.TODO(), 0)
					if err != nil {
						return false
					}
					return len(cinfo.Members) == nrNode
				}), ShouldBeTrue)
			})
		})

		Convey("When follower restart and some node leave", func(ctx C) {
			restartNode := (leader + 1) % nrNode
			leaveNode := (leader + 2) % nrNode

			So(testutil.CompareWaitN(100, func() bool {
				peers := clus.getMembersFromSnapshot(restartNode)
				return len(peers) > 0
			}), ShouldBeTrue)

			clus.stop(restartNode) //nolint:errcheck,revive // TODO:: Handle an error returned.
			clus.stop(leaveNode)   //nolint:errcheck,revive // TODO:: Handle an error returned.

			rctx, cancel := context.WithTimeout(context.Background(), vtesting.TimeoutUnitTimesFactor(50))
			defer cancel()

			So(clus.nodes[leader].RemovePeer(rctx,
				types.ClusterID(0),
				clus.nodes[leaveNode].nodeID), ShouldBeNil)

			nrNode -= 1

			clus.restart(restartNode) //nolint:errcheck,revive // TODO:: Handle an error returned.

			Convey("Then GetMembership should return 4 peers", func(ctx C) {
				So(testutil.CompareWaitN(100, func() bool {
					if !clus.healthCheck(restartNode) {
						return false
					}

					cinfo, err := clus.nodes[restartNode].GetClusterInfo(context.TODO(), 0)
					if err != nil {
						return false
					}
					return len(cinfo.Members) == nrNode
				}), ShouldBeTrue)
			})
		})
	})
}

func TestMRLoadSnapshot(t *testing.T) {
	Convey("Given MR cluster which have snapshot", t, func(ctx C) {
		testSnapCount = 10
		defer func() { testSnapCount = 0 }()
		nrRep := 1
		nrNode := 3

		clus := newMetadataRepoCluster(nrNode, nrRep, false)
		So(clus.Start(), ShouldBeNil)
		Reset(func() {
			clus.closeNoErrors(t)
		})
		So(testutil.CompareWaitN(10, func() bool {
			return clus.healthCheckAll()
		}), ShouldBeTrue)

		leader := clus.leader()
		So(leader, ShouldBeGreaterThan, -1)
		restartNode := (leader + 1) % nrNode

		snIDs := make([]types.StorageNodeID, nrRep)
		for i := range snIDs {
			snIDs[i] = types.StorageNodeID(i)

			sn := &varlogpb.StorageNodeDescriptor{
				StorageNode: varlogpb.StorageNode{
					StorageNodeID: snIDs[i],
				},
			}

			rctx, cancel := context.WithTimeout(context.Background(), vtesting.TimeoutUnitTimesFactor(50))
			defer cancel()

			err := clus.nodes[restartNode].RegisterStorageNode(rctx, sn)
			So(err, ShouldBeNil)
		}

		So(testutil.CompareWaitN(50, func() bool {
			metadata := clus.getMetadataFromSnapshot(restartNode)
			if metadata == nil {
				return false
			}

			for _, snID := range snIDs {
				if metadata.GetStorageNode(snID) == nil {
					return false
				}
			}

			return true
		}), ShouldBeTrue)

		Convey("When follower restart", func(ctx C) {
			So(clus.restart(restartNode), ShouldBeNil)
			Convey("Then GetMembership should recover metadata", func(ctx C) {
				So(testutil.CompareWaitN(100, func() bool {
					if !clus.healthCheck(restartNode) {
						return false
					}

					meta, err := clus.nodes[restartNode].GetMetadata(context.TODO())
					if err != nil {
						return false
					}

					for _, snID := range snIDs {
						if meta.GetStorageNode(snID) == nil {
							return false
						}
					}
					return true
				}), ShouldBeTrue)
			})
		})
	})
}

func TestMRRemoteSnapshot(t *testing.T) {
	Convey("Given MR cluster which have snapshot", t, func(ctx C) {
		testSnapCount = 10
		defer func() { testSnapCount = 0 }()
		nrRep := 1
		nrNode := 3

		clus := newMetadataRepoCluster(nrNode, nrRep, false)
		So(clus.Start(), ShouldBeNil)
		Reset(func() {
			clus.closeNoErrors(t)
		})
		So(testutil.CompareWaitN(10, func() bool {
			return clus.healthCheckAll()
		}), ShouldBeTrue)

		leader := clus.leader()
		So(leader, ShouldBeGreaterThan, -1)

		snIDs := make([]types.StorageNodeID, nrRep)
		for i := range snIDs {
			snIDs[i] = types.StorageNodeID(i)

			sn := &varlogpb.StorageNodeDescriptor{
				StorageNode: varlogpb.StorageNode{
					StorageNodeID: snIDs[i],
				},
			}

			rctx, cancel := context.WithTimeout(context.Background(), vtesting.TimeoutUnitTimesFactor(50))
			defer cancel()

			err := clus.nodes[leader].RegisterStorageNode(rctx, sn)
			So(err, ShouldBeNil)
		}

		So(testutil.CompareWaitN(500, func() bool {
			peers := clus.getMembersFromSnapshot(leader)
			return len(peers) == nrNode
		}), ShouldBeTrue)

		Convey("When new node join", func(ctx C) {
			newNode := nrNode
			nrNode += 1
			So(clus.appendMetadataRepo(), ShouldBeNil)
			So(clus.start(newNode), ShouldBeNil)

			rctx, cancel := context.WithTimeout(context.Background(), vtesting.TimeoutUnitTimesFactor(50))
			defer cancel()

			So(clus.nodes[leader].AddPeer(rctx,
				types.ClusterID(0),
				clus.nodes[newNode].nodeID,
				clus.peers[newNode]), ShouldBeNil)

			Convey("Then GetMembership should recover metadata", func(ctx C) {
				So(testutil.CompareWaitN(100, func() bool {
					cinfo, err := clus.nodes[newNode].GetClusterInfo(context.TODO(), 0)
					if err != nil {
						return false
					}
					return len(cinfo.Members) == nrNode
				}), ShouldBeTrue)

				Convey("Then replication should be operate", func(ctx C) {
					for i := range snIDs {
						rctx, cancel := context.WithTimeout(context.Background(), vtesting.TimeoutUnitTimesFactor(100))
						defer cancel()

						err := clus.nodes[leader].UnregisterStorageNode(rctx, snIDs[i])
						So(err, ShouldBeNil)
					}
				})
			})
		})
	})
}

func TestMRFailoverRestartWithSnapshot(t *testing.T) {
	Convey("Given MR cluster with 5 peers", t, func(ctx C) {
		nrRep := 1
		nrNode := 5
		testSnapCount = 10
		defer func() { testSnapCount = 0 }()

		clus := newMetadataRepoCluster(nrNode, nrRep, false)
		Reset(func() {
			clus.closeNoErrors(t)
		})
		So(clus.Start(), ShouldBeNil)
		So(testutil.CompareWaitN(10, func() bool {
			return clus.healthCheckAll()
		}), ShouldBeTrue)

		leader := clus.leader()
		So(leader, ShouldBeGreaterThan, -1)

		Convey("When follower restart with snapshot and some node leave", func(ctx C) {
			restartNode := (leader + 1) % nrNode
			leaveNode := (leader + 2) % nrNode

			So(testutil.CompareWaitN(500, func() bool {
				peers := clus.getMembersFromSnapshot(restartNode)
				return len(peers) == nrNode
			}), ShouldBeTrue)

			clus.stop(restartNode) //nolint:errcheck,revive // TODO:: Handle an error returned.
			clus.stop(leaveNode)   //nolint:errcheck,revive // TODO:: Handle an error returned.

			rctx, cancel := context.WithTimeout(context.Background(), vtesting.TimeoutUnitTimesFactor(50))
			defer cancel()

			So(clus.nodes[leader].RemovePeer(rctx,
				types.ClusterID(0),
				clus.nodes[leaveNode].nodeID), ShouldBeNil)

			nrNode -= 1

			clus.restart(restartNode) //nolint:errcheck,revive // TODO:: Handle an error returned.

			Convey("Then GetMembership should return 4 peers", func(ctx C) {
				So(testutil.CompareWaitN(100, func() bool {
					if !clus.healthCheck(restartNode) {
						return false
					}

					cinfo, err := clus.nodes[restartNode].GetClusterInfo(context.TODO(), 0)
					if err != nil {
						return false
					}
					return len(cinfo.Members) == nrNode
				}), ShouldBeTrue)
			})
		})
	})
}

func TestMRFailoverRestartWithOutdatedSnapshot(t *testing.T) {
	Convey("Given MR cluster with 3 peers", t, func(ctx C) {
		nrRep := 1
		nrNode := 3
		testSnapCount = 10
		defer func() { testSnapCount = 0 }()

		clus := newMetadataRepoCluster(nrNode, nrRep, false)
		Reset(func() {
			clus.closeNoErrors(t)
		})
		So(clus.Start(), ShouldBeNil)
		So(testutil.CompareWaitN(10, func() bool {
			return clus.healthCheckAll()
		}), ShouldBeTrue)

		leader := clus.leader()
		So(leader, ShouldBeGreaterThan, -1)

		Convey("When follower restart with outdated snapshot", func(ctx C) {
			restartNode := (leader + 1) % nrNode

			So(testutil.CompareWaitN(500, func() bool {
				peers := clus.getMembersFromSnapshot(restartNode)
				return len(peers) == nrNode
			}), ShouldBeTrue)

			clus.stop(restartNode) //nolint:errcheck,revive // TODO:: Handle an error returned.

			appliedIdx := clus.nodes[restartNode].raftNode.appliedIndex

			So(testutil.CompareWaitN(500, func() bool {
				snapshot := clus.getSnapshot(leader)
				return snapshot.Metadata.Index > appliedIdx+testSnapCount+1
			}), ShouldBeTrue)

			clus.restart(restartNode) //nolint:errcheck,revive // TODO:: Handle an error returned.

			Convey("Then node which is restarted should serve", func(ctx C) {
				So(testutil.CompareWaitN(100, func() bool {
					if !clus.healthCheck(restartNode) {
						return false
					}

					return clus.nodes[restartNode].GetServerAddr() != ""
				}), ShouldBeTrue)
			})
		})
	})
}

func TestMRFailoverRestartAlreadyLeavedNode(t *testing.T) {
	Convey("Given MR cluster with 3 peers", t, func(ctx C) {
		nrRep := 1
		nrNode := 3
		testSnapCount = 10
		defer func() { testSnapCount = 0 }()

		clus := newMetadataRepoCluster(nrNode, nrRep, false)
		Reset(func() {
			clus.closeNoErrors(t)
		})
		So(clus.Start(), ShouldBeNil)
		So(testutil.CompareWaitN(10, func() bool {
			return clus.healthCheckAll()
		}), ShouldBeTrue)

		leader := clus.leader()
		So(leader, ShouldBeGreaterThan, -1)

		Convey("When remove node during restart that", func(ctx C) {
			restartNode := (leader + 1) % nrNode

			So(testutil.CompareWaitN(500, func() bool {
				peers := clus.getMembersFromSnapshot(restartNode)
				return len(peers) == nrNode
			}), ShouldBeTrue)

			clus.stop(restartNode) //nolint:errcheck,revive // TODO:: Handle an error returned.

			rctx, cancel := context.WithTimeout(context.Background(), vtesting.TimeoutUnitTimesFactor(50))
			defer cancel()

			So(clus.nodes[leader].RemovePeer(rctx,
				types.ClusterID(0),
				clus.nodes[restartNode].nodeID), ShouldBeNil)

			nrNode -= 1

			So(testutil.CompareWaitN(500, func() bool {
				peers := clus.getMembersFromSnapshot(leader)
				return len(peers) == nrNode
			}), ShouldBeTrue)

			clus.restart(restartNode) //nolint:errcheck,revive // TODO:: Handle an error returned.

			Convey("Then the node should not serve", func(ctx C) {
				time.Sleep(10 * time.Second)
				So(clus.nodes[restartNode].GetServerAddr(), ShouldEqual, "")
			})
		})
	})
}

func TestMRFailoverRecoverReportCollector(t *testing.T) {
	Convey("Given MR cluster with 3 peers, 5 StorageNodes", t, func(ctx C) {
		nrRep := 1
		nrNode := 3
		nrStorageNode := 3
		nrLogStream := 2
		testSnapCount = 10

		clus := newMetadataRepoCluster(nrNode, nrRep, false)
		Reset(func() {
			clus.closeNoErrors(t)
			testSnapCount = 0
		})
		So(clus.Start(), ShouldBeNil)
		So(testutil.CompareWaitN(10, func() bool {
			return clus.healthCheckAll()
		}), ShouldBeTrue)

		leader := clus.leader()
		So(leader, ShouldBeGreaterThan, -1)

		restartNode := (leader + 1) % nrNode

		snIDs := make([][]types.StorageNodeID, nrStorageNode)
		for i := range snIDs {
			snIDs[i] = make([]types.StorageNodeID, nrRep)
			for j := range snIDs[i] {
				snIDs[i][j] = types.MinStorageNodeID + types.StorageNodeID(i*nrStorageNode+j)

				sn := &varlogpb.StorageNodeDescriptor{
					StorageNode: varlogpb.StorageNode{
						StorageNodeID: snIDs[i][j],
					},
				}

				rctx, cancel := context.WithTimeout(context.Background(), vtesting.TimeoutUnitTimesFactor(50))
				defer cancel()

				err := clus.nodes[leader].RegisterStorageNode(rctx, sn)
				So(err, ShouldBeNil)
			}
		}

		So(testutil.CompareWaitN(1, func() bool {
			return clus.nodes[leader].reportCollector.NumExecutors() == nrStorageNode*nrRep
		}), ShouldBeTrue)

		err := clus.nodes[0].RegisterTopic(context.TODO(), types.TopicID(1))
		So(err, ShouldBeNil)

		for i := 0; i < nrLogStream; i++ {
			lsID := types.MinLogStreamID + types.LogStreamID(i)
			ls := makeLogStream(types.TopicID(1), lsID, snIDs[i%nrStorageNode])

			rctx, cancel := context.WithTimeout(context.Background(), vtesting.TimeoutUnitTimesFactor(50))
			defer cancel()

			err := clus.nodes[0].RegisterLogStream(rctx, ls)
			So(err, ShouldBeNil)
		}

		So(testutil.CompareWaitN(1, func() bool {
			return clus.nodes[leader].reportCollector.NumCommitter() == nrLogStream*nrRep
		}), ShouldBeTrue)

		Convey("When follower restart with snapshot", func(ctx C) {
			So(testutil.CompareWaitN(500, func() bool {
				peers := clus.getMembersFromSnapshot(restartNode)
				return len(peers) == nrNode
			}), ShouldBeTrue)

			clus.restart(restartNode) //nolint:errcheck,revive // TODO:: Handle an error returned.

			So(testutil.CompareWaitN(50, func() bool {
				if !clus.healthCheck(restartNode) {
					return false
				}

				return clus.nodes[restartNode].GetServerAddr() != ""
			}), ShouldBeTrue)

			Convey("Then ReportCollector should recover", func(ctx C) {
				So(testutil.CompareWaitN(50, func() bool {
					return clus.nodes[restartNode].reportCollector.NumExecutors() == nrStorageNode*nrRep &&
						clus.nodes[restartNode].reportCollector.NumCommitter() == nrLogStream*nrRep
				}), ShouldBeTrue)
			})
		})
	})
}

func TestMRProposeTimeout(t *testing.T) {
	Convey("Given MR which is not running", t, func(ctx C) {
		clus := newMetadataRepoCluster(1, 1, false)
		Reset(func() {
			clus.closeNoErrors(t)
		})
		mr := clus.nodes[0]

		Convey("When cli register SN with timeout", func(ctx C) {
			snID := types.StorageNodeID(time.Now().UnixNano())
			sn := &varlogpb.StorageNodeDescriptor{
				StorageNode: varlogpb.StorageNode{
					StorageNodeID: snID,
				},
			}

			rctx, cancel := context.WithTimeout(context.Background(), vtesting.TimeoutUnitTimesFactor(50))
			defer cancel()
			err := mr.RegisterStorageNode(rctx, sn)
			Convey("Then it should be timed out", func(ctx C) {
				So(err, ShouldResemble, context.DeadlineExceeded)
			})
		})
	})
}

func TestMRProposeRetry(t *testing.T) {
	Convey("Given MR", t, func(ctx C) {
		clus := newMetadataRepoCluster(3, 1, false)
		So(clus.Start(), ShouldBeNil)
		Reset(func() {
			clus.closeNoErrors(t)
		})

		So(testutil.CompareWaitN(10, func() bool {
			return clus.healthCheckAll()
		}), ShouldBeTrue)

		Convey("When cli register SN & transfer leader for dropping propose", func(ctx C) {
			leader := clus.leader()
			clus.nodes[leader].raftNode.transferLeadership(false) //nolint:errcheck,revive // TODO:: Handle an error returned.

			snID := types.StorageNodeID(time.Now().UnixNano())
			sn := &varlogpb.StorageNodeDescriptor{
				StorageNode: varlogpb.StorageNode{
					StorageNodeID: snID,
				},
			}

			rctx, cancel := context.WithTimeout(context.Background(), vtesting.TimeoutUnitTimesFactor(50))
			defer cancel()
			err := clus.nodes[leader].RegisterStorageNode(rctx, sn)

			Convey("Then it should be success", func(ctx C) {
				So(err, ShouldBeNil)
				// So(atomic.LoadUint64(&clus.nodes[leader].requestNum), ShouldBeGreaterThan, 1)
			})
		})
	})
}

func TestMRScaleOutJoin(t *testing.T) {
	Convey("Given MR cluster", t, func(ctx C) {
		nrRep := 1
		nrNode := 1

		clus := newMetadataRepoCluster(nrNode, nrRep, false)
		Reset(func() {
			clus.closeNoErrors(t)
		})
		So(clus.Start(), ShouldBeNil)
		So(testutil.CompareWaitN(10, func() bool {
			return clus.healthCheckAll()
		}), ShouldBeTrue)

		So(testutil.CompareWaitN(1000, func() bool {
			peers := clus.getMembersFromSnapshot(0)
			return len(peers) == nrNode
		}), ShouldBeTrue)

		Convey("When new node join", func(ctx C) {
			for i := 0; i < 5; i++ {
				info, err := clus.nodes[0].GetClusterInfo(
					context.Background(),
					types.ClusterID(0),
				)
				So(err, ShouldBeNil)
				prevAppliedIndex := info.AppliedIndex

				newNode := nrNode
				So(clus.appendMetadataRepo(), ShouldBeNil)
				So(clus.start(newNode), ShouldBeNil)

				rctx, cancel := context.WithTimeout(context.Background(), vtesting.TimeoutUnitTimesFactor(50))
				defer cancel()

				So(clus.nodes[0].AddPeer(rctx,
					types.ClusterID(0),
					clus.nodes[newNode].nodeID,
					clus.peers[newNode]), ShouldBeNil)
				nrNode += 1

				info, err = clus.nodes[0].GetClusterInfo(
					context.Background(),
					types.ClusterID(0),
				)
				So(err, ShouldBeNil)
				So(info.AppliedIndex, ShouldBeGreaterThan, prevAppliedIndex)

				So(testutil.CompareWaitN(100, func() bool {
					return clus.nodes[newNode].IsMember()
				}), ShouldBeTrue)
			}

			Convey("Then it should be clustered", func(ctx C) {
				So(testutil.CompareWaitN(100, func() bool {
					cinfo, err := clus.nodes[0].GetClusterInfo(context.TODO(), types.ClusterID(0))
					if err != nil {
						return false
					}

					if len(cinfo.Members) != nrNode {
						return false
					}

					return true
				}), ShouldBeTrue)
			})
		})
	})
}

func TestMRUnregisterTopic(t *testing.T) {
	Convey("Given 1 topic & 5 log streams", t, func(ctx C) {
		rep := 1
		nrNodes := 1
		nrLS := 5
		topicID := types.TopicID(1)

		clus := newMetadataRepoCluster(nrNodes, rep, false)
		Reset(func() {
			clus.closeNoErrors(t)
		})

		So(clus.Start(), ShouldBeNil)
		So(testutil.CompareWaitN(10, func() bool {
			return clus.healthCheckAll()
		}), ShouldBeTrue)

		snIDs := make([]types.StorageNodeID, rep)
		for i := range snIDs {
			snIDs[i] = types.StorageNodeID(i)

			sn := &varlogpb.StorageNodeDescriptor{
				StorageNode: varlogpb.StorageNode{
					StorageNodeID: snIDs[i],
				},
			}

			err := clus.nodes[0].RegisterStorageNode(context.TODO(), sn)
			So(err, ShouldBeNil)
		}

		err := clus.nodes[0].RegisterTopic(context.TODO(), topicID)
		So(err, ShouldBeNil)

		lsIDs := make([]types.LogStreamID, nrLS)
		for i := range lsIDs {
			lsIDs[i] = types.MinLogStreamID + types.LogStreamID(i)
		}

		for _, lsID := range lsIDs {
			ls := makeLogStream(types.TopicID(1), lsID, snIDs)
			err := clus.nodes[0].RegisterLogStream(context.TODO(), ls)
			So(err, ShouldBeNil)
		}

		meta, _ := clus.nodes[0].GetMetadata(context.TODO())
		So(len(meta.GetLogStreams()), ShouldEqual, nrLS)

		err = clus.nodes[0].UnregisterTopic(context.TODO(), topicID)
		So(err, ShouldBeNil)

		meta, _ = clus.nodes[0].GetMetadata(context.TODO())
		So(len(meta.GetLogStreams()), ShouldEqual, 0)
	})
}

func TestMetadataRepository_MaxTopicsCount(t *testing.T) {
	const numNodes = 1
	const repFactor = 1
	const increaseUncommit = false

	Convey("MaxTopicsCount", t, func(C) {
		clus := newMetadataRepoCluster(numNodes, repFactor, increaseUncommit)
		Reset(func() {
			clus.closeNoErrors(t)
		})

		So(clus.Start(), ShouldBeNil)
		So(testutil.CompareWaitN(10, func() bool {
			return clus.healthCheckAll()
		}), ShouldBeTrue)

		mr := clus.nodes[0]

		Convey("Limit is zero", func(C) {
			mr.storage.limits.maxTopicsCount = 0
			err := mr.RegisterTopic(context.Background(), 1)
			So(err, ShouldNotBeNil)
		})

		Convey("Limit is one", func(C) {
			mr.storage.limits.maxTopicsCount = 1

			err := mr.RegisterTopic(context.Background(), 1)
			So(err, ShouldBeNil)

			err = mr.RegisterTopic(context.Background(), 2)
			So(err, ShouldNotBeNil)
			So(status.Code(err), ShouldEqual, codes.ResourceExhausted)

			err = mr.RegisterTopic(context.Background(), 1)
			So(err, ShouldBeNil)

			err = mr.UnregisterTopic(context.TODO(), 1)
			So(err, ShouldBeNil)

			err = mr.RegisterTopic(context.Background(), 2)
			So(err, ShouldBeNil)
		})
	})
}

func TestMetadataRepository_MaxLogStreamsCountPerTopic(t *testing.T) {
	const (
		numNodes         = 1
		repFactor        = 1
		increaseUncommit = false

		snid = types.StorageNodeID(1)
		tpid = types.TopicID(1)
	)

	Convey("MaxLogStreamsCountPerTopic", t, func(C) {
		clus := newMetadataRepoCluster(numNodes, repFactor, increaseUncommit)
		Reset(func() {
			clus.closeNoErrors(t)
		})

		So(clus.Start(), ShouldBeNil)
		So(testutil.CompareWaitN(10, func() bool {
			return clus.healthCheckAll()
		}), ShouldBeTrue)

		mr := clus.nodes[0]
		ctx := context.Background()

		err := mr.RegisterStorageNode(ctx, &varlogpb.StorageNodeDescriptor{
			StorageNode: varlogpb.StorageNode{
				StorageNodeID: snid,
			},
		})
		So(err, ShouldBeNil)

		err = mr.RegisterTopic(ctx, tpid)
		So(err, ShouldBeNil)

		Convey("Limit is zero", func(C) {
			mr.storage.limits.maxLogStreamsCountPerTopic = 0
			err := mr.RegisterLogStream(ctx, makeLogStream(tpid, types.LogStreamID(1), []types.StorageNodeID{snid}))
			So(err, ShouldNotBeNil)
			So(status.Code(err), ShouldEqual, codes.ResourceExhausted)
		})

		Convey("Limit is one", func(C) {
			mr.storage.limits.maxLogStreamsCountPerTopic = 1
			err := mr.RegisterLogStream(ctx, makeLogStream(tpid, types.LogStreamID(1), []types.StorageNodeID{snid}))
			So(err, ShouldBeNil)

			err = mr.RegisterLogStream(ctx, makeLogStream(tpid, types.LogStreamID(2), []types.StorageNodeID{snid}))
			So(err, ShouldNotBeNil)
			So(status.Code(err), ShouldEqual, codes.ResourceExhausted)
		})
	})
}

func TestMRTopicLastHighWatermark(t *testing.T) {
	Convey("given metadata repository with multiple topics", t, func(ctx C) {
		nrTopics := 3
		nrLS := 2
		rep := 2
		clus := newMetadataRepoCluster(1, rep, false)
		mr := clus.nodes[0]

		snIDs := make([]types.StorageNodeID, rep)
		for i := range snIDs {
			snIDs[i] = types.MinStorageNodeID + types.StorageNodeID(i)

			sn := &varlogpb.StorageNodeDescriptor{
				StorageNode: varlogpb.StorageNode{
					StorageNodeID: snIDs[i],
				},
			}

			err := mr.storage.registerStorageNode(sn)
			So(err, ShouldBeNil)
		}

		topicLogStreamID := make(map[types.TopicID][]types.LogStreamID)
		topicID := types.TopicID(1)
		lsID := types.MinLogStreamID
		for i := 0; i < nrTopics; i++ {
			err := mr.storage.registerTopic(&varlogpb.TopicDescriptor{TopicID: topicID})
			So(err, ShouldBeNil)

			lsIds := make([]types.LogStreamID, nrLS)
			for i := range lsIds {
				ls := makeLogStream(topicID, lsID, snIDs)
				err := mr.storage.registerLogStream(ls)
				So(err, ShouldBeNil)

				lsIds[i] = lsID
				lsID++
			}
			topicLogStreamID[topicID] = lsIds
			topicID++
		}

		Convey("getLastCommitted should return last committed Version", func(ctx C) {
			So(clus.Start(), ShouldBeNil)
			Reset(func() {
				clus.closeNoErrors(t)
			})

			So(testutil.CompareWaitN(10, func() bool {
				return clus.healthCheckAll()
			}), ShouldBeTrue)

			for topicID, lsIds := range topicLogStreamID {
				preVersion := mr.storage.GetLastCommitVersion()

				for _, lsID := range lsIds {
					for i := 0; i < rep; i++ {
						So(testutil.CompareWaitN(10, func() bool {
							report := makeUncommitReport(snIDs[i], types.InvalidVersion, types.InvalidGLSN, lsID, types.MinLLSN, uint64(2+i))
							return mr.proposeReport(report.StorageNodeID, report.UncommitReport) == nil
						}), ShouldBeTrue)
					}
				}

				// global commit (2, 2) highest glsn: 4
				So(testutil.CompareWaitN(10, func() bool {
					hwm, _ := mr.GetLastCommitResults().LastHighWatermark(topicID, -1)
					return hwm == types.GLSN(4)
				}), ShouldBeTrue)

				latest := mr.storage.getLastCommitResultsNoLock()
				base := mr.storage.lookupNextCommitResultsNoLock(preVersion)

				for _, lsID := range lsIds {
					So(mr.numCommitSince(topicID, lsID, base, latest, -1), ShouldEqual, 2)
				}
			}

			Convey("when topic added, glsn of new topic should start from 1", func(ctx C) {
				err := mr.storage.registerTopic(&varlogpb.TopicDescriptor{TopicID: topicID})
				So(err, ShouldBeNil)

				lsIds := make([]types.LogStreamID, nrLS)
				for i := range lsIds {
					ls := makeLogStream(topicID, lsID, snIDs)
					err := mr.storage.registerLogStream(ls)
					So(err, ShouldBeNil)

					lsIds[i] = lsID
					lsID++
				}
				topicLogStreamID[topicID] = lsIds

				preVersion := mr.storage.GetLastCommitVersion()

				for _, lsID := range lsIds {
					for i := 0; i < rep; i++ {
						So(testutil.CompareWaitN(10, func() bool {
							report := makeUncommitReport(snIDs[i], preVersion, types.InvalidGLSN, lsID, types.MinLLSN, uint64(2+i))
							return mr.proposeReport(report.StorageNodeID, report.UncommitReport) == nil
						}), ShouldBeTrue)
					}
				}

				// global commit (2, 2) highest glsn: 4
				So(testutil.CompareWaitN(10, func() bool {
					hwm, _ := mr.GetLastCommitResults().LastHighWatermark(topicID, -1)
					return hwm == types.GLSN(4)
				}), ShouldBeTrue)
			})
		})

		Convey("add logStream into topic", func(ctx C) {
			for topicID, lsIds := range topicLogStreamID {
				ls := makeLogStream(topicID, lsID, snIDs)
				err := mr.storage.registerLogStream(ls)
				So(err, ShouldBeNil)

				lsIds = append(lsIds, lsID)
				lsID++

				topicLogStreamID[topicID] = lsIds
			}

			So(clus.Start(), ShouldBeNil)
			Reset(func() {
				clus.closeNoErrors(t)
			})

			So(testutil.CompareWaitN(10, func() bool {
				return clus.healthCheckAll()
			}), ShouldBeTrue)

			for topicID, lsIds := range topicLogStreamID {
				preVersion := mr.storage.GetLastCommitVersion()

				for _, lsID := range lsIds {
					for i := 0; i < rep; i++ {
						So(testutil.CompareWaitN(10, func() bool {
							report := makeUncommitReport(snIDs[i], types.InvalidVersion, types.InvalidGLSN, lsID, types.MinLLSN, uint64(2+i))
							return mr.proposeReport(report.StorageNodeID, report.UncommitReport) == nil
						}), ShouldBeTrue)
					}
				}

				// global commit (2, 2, 2) highest glsn: 6
				So(testutil.CompareWaitN(10, func() bool {
					hwm, _ := mr.GetLastCommitResults().LastHighWatermark(topicID, -1)
					return hwm == types.GLSN(6)
				}), ShouldBeTrue)

				latest := mr.storage.getLastCommitResultsNoLock()
				base := mr.storage.lookupNextCommitResultsNoLock(preVersion)

				for _, lsID := range lsIds {
					So(mr.numCommitSince(topicID, lsID, base, latest, -1), ShouldEqual, 2)
				}
			}

			for topicID, lsIds := range topicLogStreamID {
				preVersion := mr.storage.GetLastCommitVersion()

				for _, lsID := range lsIds {
					for i := 0; i < rep; i++ {
						So(testutil.CompareWaitN(10, func() bool {
							report := makeUncommitReport(snIDs[i], types.InvalidVersion, types.InvalidGLSN, lsID, types.MinLLSN, uint64(5+i))
							return mr.proposeReport(report.StorageNodeID, report.UncommitReport) == nil
						}), ShouldBeTrue)
					}
				}

				// global commit (5, 5, 5) highest glsn: 15
				So(testutil.CompareWaitN(10, func() bool {
					hwm, _ := mr.GetLastCommitResults().LastHighWatermark(topicID, -1)
					return hwm == types.GLSN(15)
				}), ShouldBeTrue)

				latest := mr.storage.getLastCommitResultsNoLock()
				base := mr.storage.lookupNextCommitResultsNoLock(preVersion)

				for _, lsID := range lsIds {
					So(mr.numCommitSince(topicID, lsID, base, latest, -1), ShouldEqual, 3)
				}
			}
		})
	})
}

func TestMRTopicCatchup(t *testing.T) {
	Convey("Given MR cluster with multi topic", t, func(ctx C) {
		nrRep := 1
		nrNode := 1
		nrTopic := 2
		nrLSPerTopic := 2
		nrSN := nrTopic * nrLSPerTopic

		clus := newMetadataRepoCluster(nrNode, nrRep, false)
		clus.Start() //nolint:errcheck,revive // TODO:: Handle an error returned.
		Reset(func() {
			clus.closeNoErrors(t)
		})
		So(testutil.CompareWaitN(10, func() bool {
			return clus.healthCheckAll()
		}), ShouldBeTrue)

		leader := clus.leader()
		So(leader, ShouldBeGreaterThan, -1)

		// register SN & LS
		err := clus.initDummyStorageNode(nrSN, nrTopic)
		So(err, ShouldBeNil)

		So(testutil.CompareWaitN(50, func() bool {
			return len(clus.getSNIDs()) == nrSN
		}), ShouldBeTrue)

		// append to SN
		snIDs := clus.getSNIDs()
		for i := 0; i < 100; i++ {
			for _, snID := range snIDs {
				snCli := clus.reporterClientFac.(*DummyStorageNodeClientFactory).lookupClient(snID)
				snCli.increaseUncommitted(0)
			}

			for _, snID := range snIDs {
				snCli := clus.reporterClientFac.(*DummyStorageNodeClientFactory).lookupClient(snID)

				So(testutil.CompareWaitN(50, func() bool {
					return snCli.numUncommitted(0) == 0
				}), ShouldBeTrue)
			}
		}
	})
}

func TestMRCatchupUnsealedLogstream(t *testing.T) {
	Convey("Given MR cluster", t, func(ctx C) {
		nrRep := 1
		nrTopic := 1
		nrSN := 2
		clus := newMetadataRepoCluster(1, nrRep, false)
		Reset(func() {
			clus.closeNoErrors(t)
		})

		So(clus.Start(), ShouldBeNil)
		So(testutil.CompareWaitN(10, func() bool {
			return clus.healthCheckAll()
		}), ShouldBeTrue)

		mr := clus.nodes[0]

		// register SN & LS
		err := clus.initDummyStorageNode(nrSN, nrTopic)
		So(err, ShouldBeNil)

		err = mr.RegisterTopic(context.TODO(), types.TopicID(1))
		So(err, ShouldBeNil)

		So(testutil.CompareWaitN(50, func() bool {
			return len(clus.getSNIDs()) == nrSN
		}), ShouldBeTrue)

		Convey("After all ls belonging to the cluster are sealed, when a specific ls is unsealed", func(ctx C) {
			// append to LS[0]
			snIDs := clus.getSNIDs()
			snCli := clus.reporterClientFac.(*DummyStorageNodeClientFactory).lookupClient(snIDs[0])
			snCli.increaseUncommitted(0)

			So(testutil.CompareWaitN(50, func() bool {
				return snCli.numUncommitted(0) == 0
			}), ShouldBeTrue)

			// seal LS[0]
			_, err = mr.Seal(context.TODO(), types.LogStreamID(snIDs[0]))
			So(err, ShouldBeNil)

			So(testutil.CompareWaitN(10, func() bool {
				meta, err := mr.GetMetadata(context.TODO())
				if err != nil {
					return false
				}

				ls := meta.GetLogStream(types.LogStreamID(snIDs[0]))
				return ls.Status == varlogpb.LogStreamStatusSealed
			}), ShouldBeTrue)

			// append to LS[1]
			snCli = clus.reporterClientFac.(*DummyStorageNodeClientFactory).lookupClient(snIDs[1])
			snCli.increaseUncommitted(0)

			So(testutil.CompareWaitN(50, func() bool {
				return snCli.numUncommitted(0) == 0
			}), ShouldBeTrue)

			// seal LS[1]
			_, err = mr.Seal(context.TODO(), types.LogStreamID(snIDs[1]))
			So(err, ShouldBeNil)

			So(testutil.CompareWaitN(10, func() bool {
				meta, err := mr.GetMetadata(context.TODO())
				if err != nil {
					return false
				}

				ls := meta.GetLogStream(types.LogStreamID(snIDs[1]))
				return ls.Status == varlogpb.LogStreamStatusSealed
			}), ShouldBeTrue)

			// unseal LS[0]
			err = mr.Unseal(context.TODO(), types.LogStreamID(snIDs[0]))
			So(err, ShouldBeNil)

			Convey("Then the ls should appenable", func(ctx C) {
				// append to LS[0]
				snCli = clus.reporterClientFac.(*DummyStorageNodeClientFactory).lookupClient(snIDs[0])
				snCli.increaseUncommitted(0)

				So(testutil.CompareWaitN(50, func() bool {
					return snCli.numUncommitted(0) == 0
				}), ShouldBeTrue)
			})
		})
	})
}

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m,
		goleak.IgnoreTopFunction(
			"go.etcd.io/etcd/pkg/logutil.(*MergeLogger).outputLoop",
		),
		goleak.IgnoreTopFunction(
			"github.com/kakao/varlog/internal/metarepos.(*reportCollector).runPropagateCommit",
		),
	)
}

func TestMetadataRepository_AddPeer(t *testing.T) {
	const clusterID = types.ClusterID(1)

	tcs := []struct {
		name  string
		testf func(t *testing.T, server *RaftMetadataRepository, client mrpb.ManagementClient)
	}{
		{
			name: "InvalidNodeID",
			testf: func(t *testing.T, _ *RaftMetadataRepository, client mrpb.ManagementClient) {
				_, err := client.AddPeer(context.Background(), &mrpb.AddPeerRequest{
					PeerInfo: mrpb.PeerInfo{
						ClusterID: clusterID,
						NodeID:    types.InvalidNodeID,
						URL:       "http://127.0.0.1:11000",
					},
				})
				require.Error(t, err)
				require.Equal(t, codes.InvalidArgument, status.Code(err))
			},
		},
		{
			name: "AlreadyExists",
			testf: func(t *testing.T, server *RaftMetadataRepository, client mrpb.ManagementClient) {
				_, err := client.AddPeer(context.Background(), &mrpb.AddPeerRequest{
					PeerInfo: mrpb.PeerInfo{
						ClusterID: clusterID,
						NodeID:    server.nodeID,
						URL:       server.raftNode.url,
					},
				})
				require.Error(t, err)
				require.Equal(t, codes.AlreadyExists, status.Code(err))
			},
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			portLease, err := ports.ReserveWeaklyWithRetry(10000)
			require.NoError(t, err)

			peer := fmt.Sprintf("http://127.0.0.1:%d", portLease.Base())
			node := NewRaftMetadataRepository(
				WithClusterID(clusterID),
				WithRaftAddress(peer),
				WithRPCAddress("127.0.0.1:0"),
				WithRaftDirectory(t.TempDir()+"/raftdata"),
			)
			t.Cleanup(func() {
				err := node.Close()
				require.NoError(t, err)
			})

			node.Run()

			// Wait for initialization
			require.EventuallyWithT(t, func(collect *assert.CollectT) {
				addr := node.endpointAddr.Load()
				if !assert.NotNil(collect, addr) {
					return
				}
			}, 3*time.Second, 100*time.Millisecond)
			addr := node.endpointAddr.Load().(string)

			require.EventuallyWithT(t, func(collect *assert.CollectT) {
				conn, err := rpc.NewConn(context.Background(), addr)
				assert.NoError(collect, err)
				defer func() {
					err := conn.Close()
					assert.NoError(collect, err)
				}()

				healthClient := grpc_health_v1.NewHealthClient(conn.Conn)
				_, err = healthClient.Check(context.Background(), &grpc_health_v1.HealthCheckRequest{})
				assert.NoError(collect, err)
			}, 3*time.Second, 100*time.Millisecond)

			conn, err := rpc.NewConn(context.Background(), addr)
			require.NoError(t, err)
			t.Cleanup(func() {
				_ = conn.Close()
				require.NoError(t, err)
			})

			client := mrpb.NewManagementClient(conn.Conn)
			tc.testf(t, node, client)
		})
	}
}

func TestMetadataRepository_RemovePeer(t *testing.T) {
	const clusterID = types.ClusterID(1)

	tcs := []struct {
		name  string
		testf func(t *testing.T, server *RaftMetadataRepository, client mrpb.ManagementClient)
	}{
		{
			name: "InvalidNodeID",
			testf: func(t *testing.T, _ *RaftMetadataRepository, client mrpb.ManagementClient) {
				_, err := client.RemovePeer(context.Background(), &mrpb.RemovePeerRequest{
					ClusterID: clusterID,
					NodeID:    types.InvalidNodeID,
				})
				require.Error(t, err)
				require.Equal(t, codes.InvalidArgument, status.Code(err))
			},
		},
		{
			name: "NotFound",
			testf: func(t *testing.T, server *RaftMetadataRepository, client mrpb.ManagementClient) {
				_, err := client.RemovePeer(context.Background(), &mrpb.RemovePeerRequest{
					ClusterID: clusterID,
					NodeID:    server.nodeID + 1,
				})
				require.Error(t, err)
				require.Equal(t, codes.NotFound, status.Code(err))
			},
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			portLease, err := ports.ReserveWeaklyWithRetry(10000)
			require.NoError(t, err)

			peer := fmt.Sprintf("http://127.0.0.1:%d", portLease.Base())
			node := NewRaftMetadataRepository(
				WithClusterID(clusterID),
				WithRaftAddress(peer),
				WithRPCAddress("127.0.0.1:0"),
				WithRaftDirectory(t.TempDir()+"/raftdata"),
			)
			t.Cleanup(func() {
				err := node.Close()
				require.NoError(t, err)
			})

			node.Run()

			// Wait for initialization
			require.EventuallyWithT(t, func(collect *assert.CollectT) {
				addr := node.endpointAddr.Load()
				if !assert.NotNil(collect, addr) {
					return
				}
			}, 3*time.Second, 100*time.Millisecond)
			addr := node.endpointAddr.Load().(string)

			require.EventuallyWithT(t, func(collect *assert.CollectT) {
				conn, err := rpc.NewConn(context.Background(), addr)
				assert.NoError(collect, err)
				defer func() {
					err := conn.Close()
					assert.NoError(collect, err)
				}()

				healthClient := grpc_health_v1.NewHealthClient(conn.Conn)
				_, err = healthClient.Check(context.Background(), &grpc_health_v1.HealthCheckRequest{})
				assert.NoError(collect, err)
			}, 3*time.Second, 100*time.Millisecond)

			conn, err := rpc.NewConn(context.Background(), addr)
			require.NoError(t, err)
			t.Cleanup(func() {
				_ = conn.Close()
				require.NoError(t, err)
			})

			client := mrpb.NewManagementClient(conn.Conn)
			tc.testf(t, node, client)
		})
	}
}
