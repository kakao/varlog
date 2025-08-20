package metarepos

import (
	"context"
	"fmt"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.uber.org/goleak"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"

	"github.com/kakao/varlog/internal/vtesting"
	"github.com/kakao/varlog/pkg/rpc"
	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/pkg/util/testutil/ports"
	"github.com/kakao/varlog/proto/mrpb"
	"github.com/kakao/varlog/proto/snpb"
	"github.com/kakao/varlog/proto/varlogpb"
)

const (
	testRPCTimeout     = 3 * time.Second
	testContextTimeout = 5 * time.Second

	testDuration = 5 * time.Second
	testTick     = 100 * time.Millisecond
)

type testMetadataRepoCluster struct {
	nrRep             int
	peers             []string
	nodes             []*RaftMetadataRepository
	reporterClientFac ReporterClientFactory
	logger            *zap.Logger
	portLease         *ports.Lease
	rpcTimeout        time.Duration // conn + call
}

var testSnapCount uint64

func newTestMetadataRepoCluster(t *testing.T, n, nrRep int, increseUncommit bool) *testMetadataRepoCluster {
	portLease, err := ports.ReserveWeaklyWithRetry(10000)
	require.NoError(t, err)

	peers := make([]string, n)
	nodes := make([]*RaftMetadataRepository, n)

	for i := range peers {
		peers[i] = fmt.Sprintf("http://127.0.0.1:%d", portLease.Base()+i)
	}

	sncf := NewDummyStorageNodeClientFactory(1, !increseUncommit)
	clus := &testMetadataRepoCluster{
		nrRep:             nrRep,
		peers:             peers,
		nodes:             nodes,
		reporterClientFac: sncf,
		logger:            zap.L(),
		portLease:         portLease,
		rpcTimeout:        testRPCTimeout,
	}

	for i := range clus.peers {
		clus.clearNode(t, i)
		clus.createNode(t, i, false)
	}

	return clus
}

func (clus *testMetadataRepoCluster) clearNode(t *testing.T, idx int) {
	require.GreaterOrEqual(t, idx, 0)
	require.Less(t, idx, len(clus.nodes))

	nodeID := types.NewNodeIDFromURL(clus.peers[idx])
	err := os.RemoveAll(fmt.Sprintf("%s/wal/%d", vtesting.TestRaftDir(), nodeID))
	require.NoError(t, err)
	err = os.RemoveAll(fmt.Sprintf("%s/snap/%d", vtesting.TestRaftDir(), nodeID))
	require.NoError(t, err)
	err = os.RemoveAll(fmt.Sprintf("%s/sml/%d", vtesting.TestRaftDir(), nodeID))
	require.NoError(t, err)
}

func (clus *testMetadataRepoCluster) createNode(t *testing.T, idx int, join bool) {
	require.GreaterOrEqual(t, idx, 0)
	require.Less(t, idx, len(clus.nodes))

	peers := make([]string, len(clus.peers))
	copy(peers, clus.peers)

	opts := []Option{
		WithClusterID(1),
		WithReplicationFactor(clus.nrRep),
		WithRaftAddress(clus.peers[idx]),
		WithRPCAddress(":0"),
		WithRPCTimeout(clus.rpcTimeout),
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
}

func (clus *testMetadataRepoCluster) appendNewNode(t *testing.T) {
	idx := len(clus.nodes)
	clus.peers = append(clus.peers, fmt.Sprintf("http://127.0.0.1:%d", clus.portLease.Base()+idx))
	clus.nodes = append(clus.nodes, nil)
	clus.clearNode(t, idx)
	clus.createNode(t, idx, true)
}

func (clus *testMetadataRepoCluster) startNode(t *testing.T, idx int) {
	require.GreaterOrEqual(t, idx, 0)
	require.Less(t, idx, len(clus.nodes))

	clus.nodes[idx].Run()
}

func (clus *testMetadataRepoCluster) Start(t *testing.T) {
	clus.logger.Info("cluster start")
	for i := range clus.nodes {
		clus.startNode(t, i)
	}
	clus.logger.Info("cluster complete")
}

func (clus *testMetadataRepoCluster) stopNode(t *testing.T, idx int) {
	require.GreaterOrEqual(t, idx, 0)
	require.Less(t, idx, len(clus.nodes))

	err := clus.nodes[idx].Close()
	require.NoError(t, err)
}

func (clus *testMetadataRepoCluster) restartNode(t *testing.T, idx int) {
	require.GreaterOrEqual(t, idx, 0)
	require.Less(t, idx, len(clus.nodes))

	clus.stopNode(t, idx)
	clus.createNode(t, idx, false)
	clus.startNode(t, idx)
}

func (clus *testMetadataRepoCluster) closeNode(t *testing.T, idx int) {
	require.GreaterOrEqual(t, idx, 0)
	require.Less(t, idx, len(clus.nodes))

	clus.stopNode(t, idx)
	clus.clearNode(t, idx)
	clus.logger.Info("cluster node stop", zap.Int("idx", idx))
}

func (clus *testMetadataRepoCluster) getSnapshotFromNode(t *testing.T, idx int) *raftpb.Snapshot {
	require.GreaterOrEqual(t, idx, 0)
	require.Less(t, idx, len(clus.nodes))

	return clus.nodes[idx].raftNode.loadSnapshot()
}

func (clus *testMetadataRepoCluster) getMembersFromNodeSnapshot(t *testing.T, idx int) map[types.NodeID]*mrpb.MetadataRepositoryDescriptor_PeerDescriptor {
	snapshot := clus.getSnapshotFromNode(t, idx)
	if snapshot == nil {
		return nil
	}

	stateMachine := &mrpb.MetadataRepositoryDescriptor{}
	err := stateMachine.Unmarshal(snapshot.Data)
	require.NoError(t, err)

	members := make(map[types.NodeID]*mrpb.MetadataRepositoryDescriptor_PeerDescriptor)
	for nodeID, peer := range stateMachine.PeersMap.Peers {
		if !peer.IsLearner {
			members[nodeID] = peer
		}
	}

	return members
}

func (clus *testMetadataRepoCluster) getMetadataFromNodeSnapshot(t *testing.T, idx int) *varlogpb.MetadataDescriptor {
	snapshot := clus.getSnapshotFromNode(t, idx)
	if snapshot == nil {
		return nil
	}

	stateMachine := &mrpb.MetadataRepositoryDescriptor{}
	err := stateMachine.Unmarshal(snapshot.Data)
	require.NoError(t, err)

	return stateMachine.Metadata
}

// Close closes all cluster nodes.
func (clus *testMetadataRepoCluster) Close(t *testing.T) {
	clus.logger.Info("cluster stop")

	for i := range clus.peers {
		clus.closeNode(t, i)
	}
	err := os.RemoveAll(vtesting.TestRaftDir())
	require.NoError(t, err)
	err = clus.portLease.Release()
	require.NoError(t, err)

	clus.logger.Info("cluster stop complete")
}

func (clus *testMetadataRepoCluster) healthCheckNode(idx int) error {
	f := clus.nodes[idx].endpointAddr.Load()
	if f == nil {
		return fmt.Errorf("node %d: endpoint address is nil", idx)
	}
	_, port, _ := net.SplitHostPort(f.(string))
	endpoint := fmt.Sprintf("localhost:%s", port)

	ctx, cancel := context.WithTimeout(context.Background(), clus.rpcTimeout)
	defer cancel()

	conn, err := rpc.NewConn(ctx, endpoint)
	if err != nil {
		return fmt.Errorf("node %d: failed to connect to %s: %w", idx, endpoint, err)
	}
	defer func() {
		_ = conn.Close()
	}()

	healthClient := grpc_health_v1.NewHealthClient(conn.Conn)
	if _, err := healthClient.Check(ctx, &grpc_health_v1.HealthCheckRequest{}); err != nil {
		return fmt.Errorf("node %d: health check failed for %s: %w", idx, endpoint, err)
	}

	return nil
}

func (clus *testMetadataRepoCluster) healthCheckAll(t *testing.T) bool {
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		for i := range clus.nodes {
			err := clus.healthCheckNode(i)
			assert.NoError(c, err)
		}
	}, testDuration, testTick)

	return true
}

func (clus *testMetadataRepoCluster) leader() int {
	leader := -1
	for i, n := range clus.nodes {
		if n.isLeader() {
			leader = i
			break
		}
	}

	return leader
}

func (clus *testMetadataRepoCluster) leaderFail(t *testing.T) bool {
	leader := clus.leader()
	if leader < 0 {
		return false
	}

	clus.stopNode(t, leader)
	return true
}

func (clus *testMetadataRepoCluster) initDummyStorageNode(t *testing.T, nrSN, nrTopic int) {
	for i := 0; i < nrTopic; i++ {
		err := clus.nodes[0].RegisterTopic(t.Context(), types.TopicID(i%nrTopic))
		require.NoError(t, err)
	}

	for i := 0; i < nrSN; i++ {
		snID := types.MinStorageNodeID + types.StorageNodeID(i)

		sn := &varlogpb.StorageNodeDescriptor{
			StorageNode: varlogpb.StorageNode{
				StorageNodeID: snID,
			},
		}

		rctx, cancel := context.WithTimeout(t.Context(), testContextTimeout)
		defer cancel()

		err := clus.nodes[0].RegisterStorageNode(rctx, sn)
		require.NoError(t, err)

		lsID := types.LogStreamID(snID)
		ls := makeLogStream(types.TopicID(i%nrTopic), lsID, []types.StorageNodeID{snID})

		rctx, cancel = context.WithTimeout(t.Context(), testContextTimeout)
		defer cancel()

		err = clus.nodes[0].RegisterLogStream(rctx, ls)
		require.NoError(t, err)
	}
}

func (clus *testMetadataRepoCluster) getSNIDs() []types.StorageNodeID {
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
		const rep = 2
		clus := newTestMetadataRepoCluster(t, 1, rep, false)
		Reset(func() {
			clus.Close(t)
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
		const rep = 2
		clus := newTestMetadataRepoCluster(t, 1, rep, false)
		Reset(func() {
			clus.Close(t)
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
		const rep = 2
		clus := newTestMetadataRepoCluster(t, 1, rep, false)
		Reset(func() {
			clus.Close(t)
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
		clus := newTestMetadataRepoCluster(t, 1, 2, false)
		Reset(func() {
			clus.Close(t)
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
		const (
			topicID = types.TopicID(1)
			rep     = 2
		)
		clus := newTestMetadataRepoCluster(t, 1, rep, false)
		Reset(func() {
			clus.Close(t)
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

		clus.Start(t)
		clus.healthCheckAll(t)

		Convey("global commit", func(ctx C) {
			require.EventuallyWithT(t, func(c *assert.CollectT) {
				report := makeUncommitReport(snIDs[0][0], types.InvalidVersion, types.InvalidGLSN, lsIds[0], types.MinLLSN, 2)
				err := mr.proposeReport(report.StorageNodeID, report.UncommitReport)
				assert.NoError(c, err)
			}, testDuration, testTick)

			require.EventuallyWithT(t, func(c *assert.CollectT) {
				report := makeUncommitReport(snIDs[0][1], types.InvalidVersion, types.InvalidGLSN, lsIds[0], types.MinLLSN, 2)
				err := mr.proposeReport(report.StorageNodeID, report.UncommitReport)
				assert.NoError(c, err)
			}, testDuration, testTick)

			require.EventuallyWithT(t, func(c *assert.CollectT) {
				report := makeUncommitReport(snIDs[1][0], types.InvalidVersion, types.InvalidGLSN, lsIds[1], types.MinLLSN, 4)
				err := mr.proposeReport(report.StorageNodeID, report.UncommitReport)
				assert.NoError(c, err)
			}, testDuration, testTick)

			require.EventuallyWithT(t, func(c *assert.CollectT) {
				report := makeUncommitReport(snIDs[1][1], types.InvalidVersion, types.InvalidGLSN, lsIds[1], types.MinLLSN, 3)
				err := mr.proposeReport(report.StorageNodeID, report.UncommitReport)
				assert.NoError(c, err)
			}, testDuration, testTick)

			// global commit (2, 3) highest glsn: 5
			require.EventuallyWithT(t, func(c *assert.CollectT) {
				hwm, _ := mr.GetLastCommitResults().LastHighWatermark(topicID, -1)
				assert.Equal(c, types.GLSN(5), hwm)
			}, testDuration, testTick)

			Convey("LogStream should be dedup", func(ctx C) {
				require.EventuallyWithT(t, func(c *assert.CollectT) {
					report := makeUncommitReport(snIDs[0][0], types.InvalidVersion, types.InvalidGLSN, lsIds[0], types.MinLLSN, 3)
					err := mr.proposeReport(report.StorageNodeID, report.UncommitReport)
					assert.NoError(c, err)
				}, testDuration, testTick)

				require.EventuallyWithT(t, func(c *assert.CollectT) {
					report := makeUncommitReport(snIDs[0][1], types.InvalidVersion, types.InvalidGLSN, lsIds[0], types.MinLLSN, 2)
					err := mr.proposeReport(report.StorageNodeID, report.UncommitReport)
					assert.NoError(c, err)
				}, testDuration, testTick)

				require.EventuallyWithT(t, func(c *assert.CollectT) {
					hwm, _ := mr.GetLastCommitResults().LastHighWatermark(topicID, -1)
					assert.Equal(c, types.GLSN(5), hwm)
				}, testDuration, testTick)
			})

			Convey("LogStream which have wrong Version but have uncommitted should commit", func(ctx C) {
				require.EventuallyWithT(t, func(c *assert.CollectT) {
					report := makeUncommitReport(snIDs[0][0], types.InvalidVersion, types.InvalidGLSN, lsIds[0], types.MinLLSN, 6)
					err := mr.proposeReport(report.StorageNodeID, report.UncommitReport)
					assert.NoError(c, err)
				}, testDuration, testTick)

				require.EventuallyWithT(t, func(c *assert.CollectT) {
					report := makeUncommitReport(snIDs[0][1], types.InvalidVersion, types.InvalidGLSN, lsIds[0], types.MinLLSN, 6)
					err := mr.proposeReport(report.StorageNodeID, report.UncommitReport)
					assert.NoError(c, err)
				}, testDuration, testTick)

				require.EventuallyWithT(t, func(c *assert.CollectT) {
					hwm, _ := mr.GetLastCommitResults().LastHighWatermark(topicID, -1)
					assert.Equal(c, types.GLSN(9), hwm)
				}, testDuration, testTick)
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
		const (
			rep     = 1
			nrNodes = 2
			nrLS    = 5
			topicID = types.TopicID(1)
		)

		clus := newTestMetadataRepoCluster(t, nrNodes, rep, false)
		Reset(func() {
			clus.Close(t)
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

		clus.Start(t)
		clus.healthCheckAll(t)

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

				require.EventuallyWithT(t, func(c *assert.CollectT) {
					err := clus.nodes[0].proposeReport(report.StorageNodeID, report.UncommitReport)
					assert.NoError(c, err)
				}, testDuration, testTick)

				for j := 0; j < nrNodes; j++ {
					require.EventuallyWithT(t, func(c *assert.CollectT) {
						commitVersion := clus.nodes[j].storage.GetLastCommitVersion()
						assert.Equal(c, types.Version(i+1), commitVersion)
					}, testDuration, testTick)

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
		clus := newTestMetadataRepoCluster(t, 1, 1, false)
		Reset(func() {
			clus.Close(t)
		})
		clus.Start(t)
		clus.healthCheckAll(t)

		snID := types.MinStorageNodeID
		snIDs := make([]types.StorageNodeID, 0, 1)
		snIDs = append(snIDs, snID)

		lsID := types.LogStreamID(snID)

		sn := &varlogpb.StorageNodeDescriptor{
			StorageNode: varlogpb.StorageNode{
				StorageNodeID: snID,
			},
		}

		rctx, cancel := context.WithTimeout(t.Context(), testContextTimeout)
		defer cancel()

		err := clus.nodes[0].RegisterStorageNode(rctx, sn)
		So(err, ShouldBeNil)

		require.EventuallyWithT(t, func(c *assert.CollectT) {
			client := clus.reporterClientFac.(*DummyStorageNodeClientFactory).lookupClient(snID)
			assert.NotNil(c, client)
		}, testDuration, testTick)

		err = clus.nodes[0].RegisterTopic(t.Context(), types.TopicID(1))
		So(err, ShouldBeNil)

		ls := makeLogStream(types.TopicID(1), lsID, snIDs)
		rctx, cancel = context.WithTimeout(t.Context(), testContextTimeout)
		defer cancel()

		err = clus.nodes[0].RegisterLogStream(rctx, ls)
		So(err, ShouldBeNil)

		reporterClient := clus.reporterClientFac.(*DummyStorageNodeClientFactory).lookupClient(snID)
		reporterClient.increaseUncommitted(0)

		require.EventuallyWithT(t, func(c *assert.CollectT) {
			numUncommitted := reporterClient.numUncommitted(0)
			assert.Zero(c, numUncommitted)
		}, testDuration, testTick)
	})
}

func TestMRRequestMap(t *testing.T) {
	Convey("requestMap should have request when wait ack", t, func(ctx C) {
		clus := newTestMetadataRepoCluster(t, 1, 1, false)
		Reset(func() {
			clus.Close(t)
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
			rctx, cancel := context.WithTimeout(t.Context(), testContextTimeout)
			defer cancel()
			st.Done()
			mr.RegisterStorageNode(rctx, sn) //nolint:errcheck,revive // TODO:: Handle an error returned.
		}()

		st.Wait()
		require.EventuallyWithT(t, func(c *assert.CollectT) {
			_, ok := mr.requestMap.Load(requestNum + 1)
			assert.True(c, ok)
		}, testDuration, testTick)

		wg.Wait()
	})

	Convey("requestMap should ignore request that have different nodeIndex", t, func(ctx C) {
		clus := newTestMetadataRepoCluster(t, 1, 1, false)
		Reset(func() {
			clus.Close(t)
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

			require.Never(t, func() bool {
				_, ok := mr.requestMap.Load(uint64(1))
				return ok
			}, testDuration, testTick)

			dummy := &committedEntry{
				entry: &mrpb.RaftEntry{
					NodeIndex:    2,
					RequestIndex: uint64(1),
				},
			}
			mr.commitC <- dummy
		}()

		st.Wait()
		rctx, cancel := context.WithTimeout(t.Context(), testContextTimeout)
		defer cancel()
		err := mr.RegisterStorageNode(rctx, sn)

		wg.Wait()
		So(err, ShouldNotBeNil)
	})

	Convey("requestMap should delete request when context timeout", t, func(ctx C) {
		clus := newTestMetadataRepoCluster(t, 1, 1, false)
		Reset(func() {
			clus.Close(t)
		})
		mr := clus.nodes[0]

		sn := &varlogpb.StorageNodeDescriptor{
			StorageNode: varlogpb.StorageNode{
				StorageNodeID: types.StorageNodeID(0),
			},
		}

		rctx, cancel := context.WithTimeout(t.Context(), testContextTimeout)
		defer cancel()

		requestNum := mr.requestNum.Load()
		err := mr.RegisterStorageNode(rctx, sn)
		So(err, ShouldNotBeNil)

		_, ok := mr.requestMap.Load(requestNum + 1)
		So(ok, ShouldBeFalse)
	})

	Convey("requestMap should delete after ack", t, func(ctx C) {
		clus := newTestMetadataRepoCluster(t, 1, 1, false)
		Reset(func() {
			clus.Close(t)
		})
		clus.Start(t)
		clus.healthCheckAll(t)

		mr := clus.nodes[0]

		require.EventuallyWithT(t, func(c *assert.CollectT) {
			endpoint := mr.storage.LookupEndpoint(mr.nodeID)
			assert.NotEmpty(c, endpoint)
		}, testDuration, testTick)

		sn := &varlogpb.StorageNodeDescriptor{
			StorageNode: varlogpb.StorageNode{
				StorageNodeID: types.StorageNodeID(0),
			},
		}

		requestNum := mr.requestNum.Load()
		err := mr.RegisterStorageNode(t.Context(), sn)
		So(err, ShouldBeNil)

		_, ok := mr.requestMap.Load(requestNum + 1)
		So(ok, ShouldBeFalse)
	})
}

func TestMRGetLastCommitted(t *testing.T) {
	Convey("getLastCommitted", t, func(ctx C) {
		const (
			rep     = 2
			topicID = types.TopicID(1)
		)
		clus := newTestMetadataRepoCluster(t, 1, rep, false)
		Reset(func() {
			clus.Close(t)
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

		clus.Start(t)
		clus.healthCheckAll(t)

		Convey("getLastCommitted should return last committed Version", func(ctx C) {
			preVersion := mr.storage.GetLastCommitVersion()

			require.EventuallyWithT(t, func(c *assert.CollectT) {
				report := makeUncommitReport(snIDs[0][0], types.InvalidVersion, types.InvalidGLSN, lsIds[0], types.MinLLSN, 2)
				err := mr.proposeReport(report.StorageNodeID, report.UncommitReport)
				assert.NoError(c, err)
			}, testDuration, testTick)

			require.EventuallyWithT(t, func(c *assert.CollectT) {
				report := makeUncommitReport(snIDs[0][1], types.InvalidVersion, types.InvalidGLSN, lsIds[0], types.MinLLSN, 2)
				err := mr.proposeReport(report.StorageNodeID, report.UncommitReport)
				assert.NoError(c, err)
			}, testDuration, testTick)

			require.EventuallyWithT(t, func(c *assert.CollectT) {
				report := makeUncommitReport(snIDs[1][0], types.InvalidVersion, types.InvalidGLSN, lsIds[1], types.MinLLSN, 4)
				err := mr.proposeReport(report.StorageNodeID, report.UncommitReport)
				assert.NoError(c, err)
			}, testDuration, testTick)

			require.EventuallyWithT(t, func(c *assert.CollectT) {
				report := makeUncommitReport(snIDs[1][1], types.InvalidVersion, types.InvalidGLSN, lsIds[1], types.MinLLSN, 3)
				err := mr.proposeReport(report.StorageNodeID, report.UncommitReport)
				assert.NoError(c, err)
			}, testDuration, testTick)

			// global commit (2, 3) highest glsn: 5
			require.EventuallyWithT(t, func(c *assert.CollectT) {
				hwm, _ := mr.GetLastCommitResults().LastHighWatermark(topicID, -1)
				assert.Equal(c, types.GLSN(5), hwm)
			}, testDuration, testTick)

			latest := mr.storage.getLastCommitResultsNoLock()
			base := mr.storage.lookupNextCommitResultsNoLock(preVersion)

			So(mr.numCommitSince(topicID, lsIds[0], base, latest, -1), ShouldEqual, 2)
			So(mr.numCommitSince(topicID, lsIds[1], base, latest, -1), ShouldEqual, 3)

			Convey("getLastCommitted should return same if not committed", func(ctx C) {
				for i := 0; i < 10; i++ {
					preVersion := mr.storage.GetLastCommitVersion()

					require.EventuallyWithT(t, func(c *assert.CollectT) {
						report := makeUncommitReport(snIDs[1][0], types.InvalidVersion, types.InvalidGLSN, lsIds[1], types.MinLLSN, uint64(4+i))
						err := mr.proposeReport(report.StorageNodeID, report.UncommitReport)
						assert.NoError(c, err)
					}, testDuration, testTick)

					require.EventuallyWithT(t, func(c *assert.CollectT) {
						report := makeUncommitReport(snIDs[1][1], types.InvalidVersion, types.InvalidGLSN, lsIds[1], types.MinLLSN, uint64(4+i))
						err := mr.proposeReport(report.StorageNodeID, report.UncommitReport)
						assert.NoError(c, err)
					}, testDuration, testTick)

					require.EventuallyWithT(t, func(c *assert.CollectT) {
						hwm, _ := mr.GetLastCommitResults().LastHighWatermark(topicID, -1)
						assert.Equal(c, types.GLSN(6+i), hwm)
					}, testDuration, testTick)

					latest := mr.storage.getLastCommitResultsNoLock()
					base := mr.storage.lookupNextCommitResultsNoLock(preVersion)

					So(mr.numCommitSince(topicID, lsIds[0], base, latest, -1), ShouldEqual, 0)
					So(mr.numCommitSince(topicID, lsIds[1], base, latest, -1), ShouldEqual, 1)
				}
			})

			Convey("getLastCommitted should return same for sealed LS", func(ctx C) {
				rctx, cancel := context.WithTimeout(t.Context(), testContextTimeout)
				defer cancel()
				_, err := mr.Seal(rctx, lsIds[1])
				So(err, ShouldBeNil)

				for i := 0; i < 10; i++ {
					preVersion := mr.storage.GetLastCommitVersion()

					require.EventuallyWithT(t, func(c *assert.CollectT) {
						report := makeUncommitReport(snIDs[0][0], types.InvalidVersion, types.InvalidGLSN, lsIds[0], types.MinLLSN, uint64(3+i))
						err := mr.proposeReport(report.StorageNodeID, report.UncommitReport)
						assert.NoError(c, err)
					}, testDuration, testTick)

					require.EventuallyWithT(t, func(c *assert.CollectT) {
						report := makeUncommitReport(snIDs[0][1], types.InvalidVersion, types.InvalidGLSN, lsIds[0], types.MinLLSN, uint64(3+i))
						err := mr.proposeReport(report.StorageNodeID, report.UncommitReport)
						assert.NoError(c, err)
					}, testDuration, testTick)

					require.EventuallyWithT(t, func(c *assert.CollectT) {
						report := makeUncommitReport(snIDs[1][0], types.InvalidVersion, types.InvalidGLSN, lsIds[1], types.MinLLSN, uint64(4+i))
						err := mr.proposeReport(report.StorageNodeID, report.UncommitReport)
						assert.NoError(c, err)
					}, testDuration, testTick)

					require.EventuallyWithT(t, func(c *assert.CollectT) {
						report := makeUncommitReport(snIDs[1][1], types.InvalidVersion, types.InvalidGLSN, lsIds[1], types.MinLLSN, uint64(4+i))
						err := mr.proposeReport(report.StorageNodeID, report.UncommitReport)
						assert.NoError(c, err)
					}, testDuration, testTick)

					require.EventuallyWithT(t, func(c *assert.CollectT) {
						hwm, _ := mr.GetLastCommitResults().LastHighWatermark(topicID, -1)
						assert.Equal(c, types.GLSN(6+i), hwm)
					}, testDuration, testTick)

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
		const (
			rep     = 2
			topicID = types.TopicID(1)
		)

		clus := newTestMetadataRepoCluster(t, 1, rep, false)
		Reset(func() {
			clus.Close(t)
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

		clus.Start(t)
		clus.healthCheckAll(t)

		Convey("Seal should commit and return last committed", func(ctx C) {
			require.EventuallyWithT(t, func(c *assert.CollectT) {
				report := makeUncommitReport(snIDs[0][0], types.InvalidVersion, types.InvalidGLSN, lsIDs[0], types.MinLLSN, 2)
				err := mr.proposeReport(report.StorageNodeID, report.UncommitReport)
				assert.NoError(c, err)
			}, testDuration, testTick)

			require.EventuallyWithT(t, func(c *assert.CollectT) {
				report := makeUncommitReport(snIDs[0][1], types.InvalidVersion, types.InvalidGLSN, lsIDs[0], types.MinLLSN, 2)
				err := mr.proposeReport(report.StorageNodeID, report.UncommitReport)
				assert.NoError(c, err)
			}, testDuration, testTick)

			require.EventuallyWithT(t, func(c *assert.CollectT) {
				report := makeUncommitReport(snIDs[1][0], types.InvalidVersion, types.InvalidGLSN, lsIDs[1], types.MinLLSN, 4)
				err := mr.proposeReport(report.StorageNodeID, report.UncommitReport)
				assert.NoError(c, err)
			}, testDuration, testTick)

			require.EventuallyWithT(t, func(c *assert.CollectT) {
				report := makeUncommitReport(snIDs[1][1], types.InvalidVersion, types.InvalidGLSN, lsIDs[1], types.MinLLSN, 3)
				err := mr.proposeReport(report.StorageNodeID, report.UncommitReport)
				assert.NoError(c, err)
			}, testDuration, testTick)

			rctx, cancel := context.WithTimeout(t.Context(), testContextTimeout)
			defer cancel()
			lc, err := mr.Seal(rctx, lsIDs[1])
			So(err, ShouldBeNil)
			So(lc, ShouldEqual, mr.getLastCommitted(topicID, lsIDs[1], -1))

			Convey("Seal should return same last committed", func(ctx C) {
				for i := 0; i < 10; i++ {
					rctx, cancel := context.WithTimeout(t.Context(), testContextTimeout)
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
		const (
			rep     = 2
			topicID = types.TopicID(1)
		)

		clus := newTestMetadataRepoCluster(t, 1, rep, false)
		Reset(func() {
			clus.Close(t)
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

		clus.Start(t)
		clus.healthCheckAll(t)

		require.EventuallyWithT(t, func(c *assert.CollectT) {
			report := makeUncommitReport(snIDs[0][0], types.InvalidVersion, types.InvalidGLSN, lsIDs[0], types.MinLLSN, 2)
			err := mr.proposeReport(report.StorageNodeID, report.UncommitReport)
			assert.NoError(c, err)
		}, testDuration, testTick)

		require.EventuallyWithT(t, func(c *assert.CollectT) {
			report := makeUncommitReport(snIDs[0][1], types.InvalidVersion, types.InvalidGLSN, lsIDs[0], types.MinLLSN, 2)
			err := mr.proposeReport(report.StorageNodeID, report.UncommitReport)
			assert.NoError(c, err)
		}, testDuration, testTick)

		require.EventuallyWithT(t, func(c *assert.CollectT) {
			report := makeUncommitReport(snIDs[1][0], types.InvalidVersion, types.InvalidGLSN, lsIDs[1], types.MinLLSN, 4)
			err := mr.proposeReport(report.StorageNodeID, report.UncommitReport)
			assert.NoError(c, err)
		}, testDuration, testTick)

		require.EventuallyWithT(t, func(c *assert.CollectT) {
			report := makeUncommitReport(snIDs[1][1], types.InvalidVersion, types.InvalidGLSN, lsIDs[1], types.MinLLSN, 3)
			err := mr.proposeReport(report.StorageNodeID, report.UncommitReport)
			assert.NoError(c, err)
		}, testDuration, testTick)

		require.EventuallyWithT(t, func(c *assert.CollectT) {
			hwm, _ := mr.GetLastCommitResults().LastHighWatermark(topicID, -1)
			assert.Equal(c, types.GLSN(5), hwm)
		}, testDuration, testTick)

		rctx, cancel := context.WithTimeout(t.Context(), testContextTimeout)
		defer cancel()
		sealedHWM, err := mr.Seal(rctx, lsIDs[1])

		So(err, ShouldBeNil)
		So(sealedHWM, ShouldEqual, mr.getLastCommitted(topicID, lsIDs[1], -1))

		require.EventuallyWithT(t, func(c *assert.CollectT) {
			sealedVer := mr.getLastCommitVersion(topicID, lsIDs[1])

			for _, snID := range snIDs[1] {
				report := makeUncommitReport(snID, sealedVer, sealedHWM, lsIDs[1], types.LLSN(4), 0)
				err := mr.proposeReport(report.StorageNodeID, report.UncommitReport)
				assert.NoError(c, err)
			}

			meta, err := mr.GetMetadata(t.Context())
			if !assert.NoError(c, err) {
				return
			}

			ls := meta.GetLogStream(lsIDs[1])
			assert.Equal(c, varlogpb.LogStreamStatusSealed, ls.Status)
		}, testDuration, testTick)

		sealedVer := mr.getLastCommitVersion(topicID, lsIDs[1])

		Convey("Unealed LS should update report", func(ctx C) {
			rctx, cancel := context.WithTimeout(t.Context(), testContextTimeout)
			defer cancel()
			err := mr.Unseal(rctx, lsIDs[1])
			So(err, ShouldBeNil)

			for i := 1; i < 10; i++ {
				preVersion := mr.storage.GetLastCommitVersion()

				require.EventuallyWithT(t, func(c *assert.CollectT) {
					report := makeUncommitReport(snIDs[1][0], sealedVer, sealedHWM, lsIDs[1], types.LLSN(4), uint64(i))
					err := mr.proposeReport(report.StorageNodeID, report.UncommitReport)
					assert.NoError(c, err)
				}, testDuration, testTick)

				require.EventuallyWithT(t, func(c *assert.CollectT) {
					report := makeUncommitReport(snIDs[1][1], sealedVer, sealedHWM, lsIDs[1], types.LLSN(4), uint64(i))
					err := mr.proposeReport(report.StorageNodeID, report.UncommitReport)
					assert.NoError(c, err)
				}, testDuration, testTick)

				require.EventuallyWithT(t, func(c *assert.CollectT) {
					hwm, _ := mr.GetLastCommitResults().LastHighWatermark(topicID, -1)
					assert.Equal(c, types.GLSN(5+i), hwm)
				}, testDuration, testTick)

				latest := mr.storage.getLastCommitResultsNoLock()
				base := mr.storage.lookupNextCommitResultsNoLock(preVersion)

				So(mr.numCommitSince(topicID, lsIDs[1], base, latest, -1), ShouldEqual, 1)
			}
		})
	})
}

func TestMRUpdateLogStream(t *testing.T) {
	Convey("Given MR cluster", t, func(ctx C) {
		const (
			rep           = 2
			nrStorageNode = rep + 1
		)
		clus := newTestMetadataRepoCluster(t, 1, rep, false)
		Reset(func() {
			clus.Close(t)
		})

		clus.Start(t)
		clus.healthCheckAll(t)

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

			err := mr.RegisterStorageNode(t.Context(), sn)
			So(err, ShouldBeNil)
		}

		err := mr.RegisterTopic(t.Context(), types.TopicID(1))
		So(err, ShouldBeNil)

		ls := makeLogStream(types.TopicID(1), lsID, snIDs[0:rep])
		err = mr.RegisterLogStream(t.Context(), ls)
		So(err, ShouldBeNil)
		require.EventuallyWithT(t, func(c *assert.CollectT) {
			numCommitters := mr.reportCollector.NumCommitter()
			assert.Equal(c, rep, numCommitters)
		}, testDuration, testTick)

		Convey("When Update LogStream", func() {
			rctx, cancel := context.WithTimeout(t.Context(), testContextTimeout)
			defer cancel()
			_, err = mr.Seal(rctx, lsID)
			So(err, ShouldBeNil)

			updatedls := makeLogStream(types.TopicID(1), lsID, snIDs[1:rep+1])
			err = mr.UpdateLogStream(t.Context(), updatedls)
			So(err, ShouldBeNil)

			Convey("Then unused report collector should be closed", func() {
				require.EventuallyWithT(t, func(c *assert.CollectT) {
					numCommitters := mr.reportCollector.NumCommitter()
					assert.Equal(c, rep, numCommitters)
				}, testDuration, testTick)

				err = mr.UnregisterStorageNode(t.Context(), snIDs[0])
				So(err, ShouldBeNil)

				require.EventuallyWithT(t, func(c *assert.CollectT) {
					reporterClient := clus.reporterClientFac.(*DummyStorageNodeClientFactory).lookupClient(snIDs[0])
					assert.Nil(c, reporterClient)
				}, testDuration, testTick)

				require.EventuallyWithT(t, func(c *assert.CollectT) {
					numCommitters := mr.reportCollector.NumCommitter()
					assert.Equal(c, rep, numCommitters)
				}, testDuration, testTick)

				So(clus.reporterClientFac.(*DummyStorageNodeClientFactory).lookupClient(snIDs[1]) == nil, ShouldBeFalse)
				So(clus.reporterClientFac.(*DummyStorageNodeClientFactory).lookupClient(snIDs[2]) == nil, ShouldBeFalse)
			})
		})
	})
}

func TestMRUpdateLogStreamExclusive(t *testing.T) {
	Convey("Given MR cluster", t, func(ctx C) {
		const (
			rep           = 2
			nrStorageNode = rep + 2
		)
		clus := newTestMetadataRepoCluster(t, 1, rep, false)
		Reset(func() {
			clus.Close(t)
		})

		clus.Start(t)
		clus.healthCheckAll(t)

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

			err := mr.RegisterStorageNode(t.Context(), sn)
			So(err, ShouldBeNil)
		}

		err := mr.RegisterTopic(t.Context(), types.TopicID(1))
		So(err, ShouldBeNil)

		ls := makeLogStream(types.TopicID(1), lsID, snIDs[0:rep])
		err = mr.RegisterLogStream(t.Context(), ls)
		So(err, ShouldBeNil)
		require.EventuallyWithT(t, func(c *assert.CollectT) {
			numCommitters := mr.reportCollector.NumCommitter()
			assert.Equal(c, rep, numCommitters)
		}, testDuration, testTick)

		Convey("When Update LogStream with no overlapping SN", func() {
			rctx, cancel := context.WithTimeout(t.Context(), testContextTimeout)
			defer cancel()
			_, err = mr.Seal(rctx, lsID)
			So(err, ShouldBeNil)

			updatedls := makeLogStream(types.TopicID(1), lsID, snIDs[2:rep+2])
			err = mr.UpdateLogStream(t.Context(), updatedls)

			Convey("Then it should be rejected", func() {
				So(err, ShouldNotBeNil)
			})
		})
	})
}

func TestMRUpdateLogStreamUnsafe(t *testing.T) {
	Convey("Given MR cluster", t, func(ctx C) {
		const (
			rep           = 2
			nrStorageNode = rep + 2
		)
		clus := newTestMetadataRepoCluster(t, 1, rep, false)
		Reset(func() {
			clus.Close(t)
		})

		clus.Start(t)
		clus.healthCheckAll(t)

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

			err := mr.RegisterStorageNode(t.Context(), sn)
			So(err, ShouldBeNil)
		}

		err := mr.RegisterTopic(t.Context(), types.TopicID(1))
		So(err, ShouldBeNil)

		ls := makeLogStream(types.TopicID(1), lsID, snIDs[0:rep])
		err = mr.RegisterLogStream(t.Context(), ls)
		So(err, ShouldBeNil)
		require.EventuallyWithT(t, func(c *assert.CollectT) {
			numCommitters := mr.reportCollector.NumCommitter()
			assert.Equal(c, rep, numCommitters)
		}, testDuration, testTick)

		Convey("When Update LogStream with SN not reaching the last commit", func() {
			disableCommitSNID := snIDs[1]
			disableCommitSNCli := clus.reporterClientFac.(*DummyStorageNodeClientFactory).lookupClient(disableCommitSNID)
			disableCommitSNCli.DisableCommit()

			for _, snID := range snIDs[0:rep] {
				snCli := clus.reporterClientFac.(*DummyStorageNodeClientFactory).lookupClient(snID)
				snCli.increaseUncommitted(0)
			}

			require.EventuallyWithT(t, func(c *assert.CollectT) {
				cr, _, _ := mr.GetLastCommitResults().LookupCommitResult(types.TopicID(1), lsID, -1)
				assert.NotEqual(c, types.InvalidGLSN, cr.HighWatermark)
			}, testDuration, testTick)

			rctx, cancel := context.WithTimeout(t.Context(), testContextTimeout)
			defer cancel()
			_, err = mr.Seal(rctx, lsID)
			So(err, ShouldBeNil)

			updatedls := makeLogStream(types.TopicID(1), lsID, snIDs[1:rep+1])
			err = mr.UpdateLogStream(t.Context(), updatedls)

			Convey("Then it should be rejected", func() {
				So(err, ShouldNotBeNil)
			})
		})
	})
}

func TestMRFailoverLeaderElection(t *testing.T) {
	Convey("Given MR cluster", t, func(ctx C) {
		const (
			nrRep  = 1
			nrNode = 3
		)

		clus := newTestMetadataRepoCluster(t, nrNode, nrRep, true)
		Reset(func() {
			clus.Close(t)
		})
		clus.Start(t)
		clus.healthCheckAll(t)

		snIDs := make([]types.StorageNodeID, nrRep)
		for i := range snIDs {
			snIDs[i] = types.MinStorageNodeID + types.StorageNodeID(i)

			sn := &varlogpb.StorageNodeDescriptor{
				StorageNode: varlogpb.StorageNode{
					StorageNodeID: snIDs[i],
				},
			}

			rctx, cancel := context.WithTimeout(t.Context(), testContextTimeout)
			defer cancel()

			err := clus.nodes[0].RegisterStorageNode(rctx, sn)
			So(err, ShouldBeNil)
		}

		err := clus.nodes[0].RegisterTopic(t.Context(), types.TopicID(1))
		So(err, ShouldBeNil)

		lsID := types.MinLogStreamID
		ls := makeLogStream(types.TopicID(1), lsID, snIDs)

		rctx, cancel := context.WithTimeout(t.Context(), testContextTimeout)
		defer cancel()

		err = clus.nodes[0].RegisterLogStream(rctx, ls)
		So(err, ShouldBeNil)

		reporterClient := clus.reporterClientFac.(*DummyStorageNodeClientFactory).lookupClient(snIDs[0])
		require.EventuallyWithT(t, func(c *assert.CollectT) {
			knownVersion := reporterClient.getKnownVersion(0)
			assert.False(c, knownVersion.Invalid())
		}, testDuration, testTick)

		Convey("When node fail", func(ctx C) {
			leader := clus.leader()

			So(clus.leaderFail(t), ShouldBeTrue)

			Convey("Then MR Cluster should elect", func(ctx C) {
				require.EventuallyWithT(t, func(c *assert.CollectT) {
					currentLeader := clus.leader()
					assert.NotEqual(c, leader, currentLeader)
				}, testDuration, testTick)

				prev := reporterClient.getKnownVersion(0)

				require.EventuallyWithT(t, func(c *assert.CollectT) {
					currentVersion := reporterClient.getKnownVersion(0)
					assert.Greater(c, currentVersion, prev)
				}, testDuration, testTick)
			})
		})
	})
}

func TestMRFailoverJoinNewNode(t *testing.T) {
	Convey("Given MR cluster", t, func(ctx C) {
		const nrRep = 1
		nrNode := 3

		clus := newTestMetadataRepoCluster(t, nrNode, nrRep, false)
		Reset(func() {
			clus.Close(t)
		})

		clus.Start(t)
		clus.healthCheckAll(t)

		snIDs := make([]types.StorageNodeID, nrRep)
		for i := range snIDs {
			snIDs[i] = types.MinStorageNodeID + types.StorageNodeID(i)

			sn := &varlogpb.StorageNodeDescriptor{
				StorageNode: varlogpb.StorageNode{
					StorageNodeID: snIDs[i],
				},
			}

			rctx, cancel := context.WithTimeout(t.Context(), testContextTimeout)
			defer cancel()

			err := clus.nodes[0].RegisterStorageNode(rctx, sn)
			So(err, ShouldBeNil)
		}

		err := clus.nodes[0].RegisterTopic(t.Context(), types.TopicID(1))
		So(err, ShouldBeNil)

		lsID := types.MinLogStreamID
		ls := makeLogStream(types.TopicID(1), lsID, snIDs)

		rctx, cancel := context.WithTimeout(t.Context(), testContextTimeout)
		defer cancel()

		err = clus.nodes[0].RegisterLogStream(rctx, ls)
		So(err, ShouldBeNil)

		Convey("When new node join", func(ctx C) {
			newNode := nrNode
			nrNode += 1
			clus.appendNewNode(t)
			clus.startNode(t, newNode)

			info, err := clus.nodes[0].GetClusterInfo(t.Context(), types.ClusterID(0))
			So(err, ShouldBeNil)
			appliedIdx := info.AppliedIndex

			rctx, cancel := context.WithTimeout(t.Context(), testContextTimeout)
			defer cancel()

			So(clus.nodes[0].AddPeer(rctx,
				types.ClusterID(0),
				clus.nodes[newNode].nodeID,
				clus.peers[newNode]), ShouldBeNil)

			info, err = clus.nodes[0].GetClusterInfo(t.Context(), types.ClusterID(0))
			So(err, ShouldBeNil)
			So(info.AppliedIndex, ShouldBeGreaterThan, appliedIdx)

			Convey("Then new node should be member", func(ctx C) {
				require.EventuallyWithT(t, func(c *assert.CollectT) {
					isMember := clus.nodes[newNode].IsMember()
					assert.True(c, isMember)
				}, testDuration, testTick)

				Convey("Register to new node should be success", func(ctx C) {
					snID := snIDs[nrRep-1] + types.StorageNodeID(1)

					sn := &varlogpb.StorageNodeDescriptor{
						StorageNode: varlogpb.StorageNode{
							StorageNodeID: snID,
						},
					}

					rctx, cancel := context.WithTimeout(t.Context(), testContextTimeout)
					defer cancel()

					err := clus.nodes[newNode].RegisterStorageNode(rctx, sn)
					if rctx.Err() != nil {
						clus.logger.Info("complete with ctx error", zap.String("err", rctx.Err().Error()))
					}
					So(err, ShouldBeNil)

					meta, err := clus.nodes[newNode].GetMetadata(t.Context())
					So(err, ShouldBeNil)
					So(meta.GetStorageNode(snID), ShouldNotBeNil)
				})
			})
		})

		Convey("When new nodes started but not yet joined", func(ctx C) {
			newNode := nrNode
			nrNode += 1
			clus.appendNewNode(t)
			clus.startNode(t, newNode)

			time.Sleep(10 * time.Second)

			Convey("Then it should not have member info", func(ctx C) {
				_, err := clus.nodes[newNode].GetClusterInfo(t.Context(), types.ClusterID(0))
				So(status.Code(err), ShouldEqual, codes.Unavailable)

				cinfo, err := clus.nodes[0].GetClusterInfo(t.Context(), types.ClusterID(0))
				So(err, ShouldBeNil)
				So(len(cinfo.Members), ShouldBeLessThan, nrNode)

				Convey("After join, it should have member info", func(ctx C) {
					info, err := clus.nodes[0].GetClusterInfo(t.Context(), types.ClusterID(0))
					So(err, ShouldBeNil)
					appliedIdx := info.AppliedIndex

					rctx, cancel := context.WithTimeout(t.Context(), testContextTimeout)
					defer cancel()

					So(clus.nodes[0].AddPeer(rctx,
						types.ClusterID(0),
						clus.nodes[newNode].nodeID,
						clus.peers[newNode]), ShouldBeNil)

					info, err = clus.nodes[0].GetClusterInfo(t.Context(), types.ClusterID(0))
					So(err, ShouldBeNil)
					So(info.AppliedIndex, ShouldBeGreaterThan, appliedIdx)

					require.EventuallyWithT(t, func(c *assert.CollectT) {
						isMember := clus.nodes[newNode].IsMember()
						assert.True(c, isMember)
					}, testDuration, testTick)

					Convey("Then proposal should be operated", func(ctx C) {
						snID := snIDs[nrRep-1] + types.StorageNodeID(1)

						sn := &varlogpb.StorageNodeDescriptor{
							StorageNode: varlogpb.StorageNode{
								StorageNodeID: snID,
							},
						}

						rctx, cancel := context.WithTimeout(t.Context(), testContextTimeout)
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
		const (
			nrRep  = 1
			nrNode = 3
		)

		clus := newTestMetadataRepoCluster(t, nrNode, nrRep, false)
		Reset(func() {
			clus.Close(t)
		})
		clus.Start(t)
		clus.healthCheckAll(t)

		leader := clus.leader()
		So(leader, ShouldBeGreaterThan, -1)

		Convey("When follower leave", func(ctx C) {
			leaveNode := (leader + 1) % nrNode
			checkNode := (leaveNode + 1) % nrNode

			require.EventuallyWithT(t, func(c *assert.CollectT) {
				cinfo, _ := clus.nodes[checkNode].GetClusterInfo(t.Context(), 0)
				assert.Len(c, cinfo.Members, nrNode)
			}, testDuration, testTick)

			clus.stopNode(t, leaveNode)

			info, err := clus.nodes[checkNode].GetClusterInfo(t.Context(), types.ClusterID(0))
			So(err, ShouldBeNil)
			appliedIdx := info.AppliedIndex

			rctx, cancel := context.WithTimeout(t.Context(), testContextTimeout)
			defer cancel()

			So(clus.nodes[checkNode].RemovePeer(rctx,
				types.ClusterID(0),
				clus.nodes[leaveNode].nodeID), ShouldBeNil)

			Convey("Then GetMembership should return 2 peers", func(ctx C) {
				cinfo, err := clus.nodes[checkNode].GetClusterInfo(t.Context(), 0)
				So(err, ShouldBeNil)
				So(len(cinfo.Members), ShouldEqual, nrNode-1)
				So(cinfo.AppliedIndex, ShouldBeGreaterThan, appliedIdx)
			})
		})

		Convey("When leader leave", func(ctx C) {
			leaveNode := leader
			checkNode := (leaveNode + 1) % nrNode

			require.EventuallyWithT(t, func(c *assert.CollectT) {
				cinfo, _ := clus.nodes[checkNode].GetClusterInfo(t.Context(), 0)
				assert.Len(c, cinfo.Members, nrNode)
			}, testDuration, testTick)

			clus.stopNode(t, leaveNode)

			info, err := clus.nodes[checkNode].GetClusterInfo(t.Context(), types.ClusterID(0))
			So(err, ShouldBeNil)
			appliedIdx := info.AppliedIndex

			rctx, cancel := context.WithTimeout(t.Context(), testContextTimeout)
			defer cancel()

			So(clus.nodes[checkNode].RemovePeer(rctx,
				types.ClusterID(0),
				clus.nodes[leaveNode].nodeID), ShouldBeNil)

			Convey("Then GetMembership should return 2 peers", func(ctx C) {
				cinfo, err := clus.nodes[checkNode].GetClusterInfo(t.Context(), 0)
				So(err, ShouldBeNil)
				So(len(cinfo.Members), ShouldEqual, nrNode-1)
				So(cinfo.AppliedIndex, ShouldBeGreaterThan, appliedIdx)
			})
		})
	})
}

func TestMRFailoverRestart(t *testing.T) {
	Convey("Given MR cluster with 5 peers", t, func(ctx C) {
		const nrRep = 1
		nrNode := 5

		clus := newTestMetadataRepoCluster(t, nrNode, nrRep, false)
		Reset(func() {
			clus.Close(t)
		})
		clus.Start(t)
		clus.healthCheckAll(t)

		leader := clus.leader()
		So(leader, ShouldBeGreaterThan, -1)

		Convey("When follower restart and new node joined", func(ctx C) {
			restartNode := (leader + 1) % nrNode

			require.EventuallyWithT(t, func(c *assert.CollectT) {
				peers := clus.getMembersFromNodeSnapshot(t, restartNode)
				assert.NotEmpty(c, peers)
			}, testDuration, testTick)

			clus.restartNode(t, restartNode)

			newNode := nrNode
			nrNode += 1
			clus.appendNewNode(t)
			clus.startNode(t, newNode)

			rctx, cancel := context.WithTimeout(t.Context(), testContextTimeout)
			defer cancel()

			So(clus.nodes[leader].AddPeer(rctx,
				types.ClusterID(0),
				clus.nodes[newNode].nodeID,
				clus.peers[newNode]), ShouldBeNil)

			Convey("Then GetMembership should return 6 peers", func(ctx C) {
				require.EventuallyWithT(t, func(c *assert.CollectT) {
					err := clus.healthCheckNode(restartNode)
					if !assert.NoError(c, err) {
						return
					}

					cinfo, err := clus.nodes[restartNode].GetClusterInfo(t.Context(), 0)
					if !assert.NoError(c, err) {
						return
					}
					assert.Len(c, cinfo.Members, nrNode)
				}, testDuration, testTick)
			})
		})

		Convey("When follower restart and some node leave", func(ctx C) {
			restartNode := (leader + 1) % nrNode
			leaveNode := (leader + 2) % nrNode

			require.EventuallyWithT(t, func(c *assert.CollectT) {
				peers := clus.getMembersFromNodeSnapshot(t, restartNode)
				assert.NotEmpty(c, peers)
			}, testDuration, testTick)

			clus.stopNode(t, restartNode)
			clus.stopNode(t, leaveNode)

			rctx, cancel := context.WithTimeout(t.Context(), testContextTimeout)
			defer cancel()

			So(clus.nodes[leader].RemovePeer(rctx,
				types.ClusterID(0),
				clus.nodes[leaveNode].nodeID), ShouldBeNil)

			nrNode -= 1

			clus.restartNode(t, restartNode)

			Convey("Then GetMembership should return 4 peers", func(ctx C) {
				require.EventuallyWithT(t, func(c *assert.CollectT) {
					err := clus.healthCheckNode(restartNode)
					if !assert.NoError(c, err) {
						return
					}

					cinfo, err := clus.nodes[restartNode].GetClusterInfo(t.Context(), 0)
					if !assert.NoError(c, err) {
						return
					}
					assert.Len(c, cinfo.Members, nrNode)
				}, testDuration, testTick)
			})
		})
	})
}

func TestMRLoadSnapshot(t *testing.T) {
	Convey("Given MR cluster which have snapshot", t, func(ctx C) {
		testSnapCount = 10
		defer func() { testSnapCount = 0 }()

		const (
			nrRep  = 1
			nrNode = 3
		)

		clus := newTestMetadataRepoCluster(t, nrNode, nrRep, false)
		clus.Start(t)
		Reset(func() {
			clus.Close(t)
		})
		clus.healthCheckAll(t)

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

			rctx, cancel := context.WithTimeout(t.Context(), testContextTimeout)
			defer cancel()

			err := clus.nodes[restartNode].RegisterStorageNode(rctx, sn)
			So(err, ShouldBeNil)
		}

		require.EventuallyWithT(t, func(c *assert.CollectT) {
			metadata := clus.getMetadataFromNodeSnapshot(t, restartNode)
			if !assert.NotNil(c, metadata) {
				return
			}

			for _, snID := range snIDs {
				sn := metadata.GetStorageNode(snID)
				assert.NotNil(c, sn)
			}
		}, testDuration, testTick)

		Convey("When follower restart", func(ctx C) {
			clus.restartNode(t, restartNode)
			Convey("Then GetMembership should recover metadata", func(ctx C) {
				require.EventuallyWithT(t, func(c *assert.CollectT) {
					err := clus.healthCheckNode(restartNode)
					if !assert.NoError(c, err) {
						return
					}

					meta, err := clus.nodes[restartNode].GetMetadata(t.Context())
					if !assert.NoError(c, err) {
						return
					}

					for _, snID := range snIDs {
						sn := meta.GetStorageNode(snID)
						assert.NotNil(c, sn)
					}
				}, testDuration, testTick)
			})
		})
	})
}

func TestMRRemoteSnapshot(t *testing.T) {
	Convey("Given MR cluster which have snapshot", t, func(ctx C) {
		testSnapCount = 10
		defer func() { testSnapCount = 0 }()

		const nrRep = 1
		nrNode := 3

		clus := newTestMetadataRepoCluster(t, nrNode, nrRep, false)
		clus.Start(t)
		Reset(func() {
			clus.Close(t)
		})
		clus.healthCheckAll(t)

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

			rctx, cancel := context.WithTimeout(t.Context(), testContextTimeout)
			defer cancel()

			err := clus.nodes[leader].RegisterStorageNode(rctx, sn)
			So(err, ShouldBeNil)
		}

		require.EventuallyWithT(t, func(c *assert.CollectT) {
			peers := clus.getMembersFromNodeSnapshot(t, leader)
			assert.Len(c, peers, nrNode)
		}, testDuration, testTick)

		Convey("When new node join", func(ctx C) {
			newNode := nrNode
			nrNode += 1
			clus.appendNewNode(t)
			clus.startNode(t, newNode)

			rctx, cancel := context.WithTimeout(t.Context(), testContextTimeout)
			defer cancel()

			So(clus.nodes[leader].AddPeer(rctx,
				types.ClusterID(0),
				clus.nodes[newNode].nodeID,
				clus.peers[newNode]), ShouldBeNil)

			Convey("Then GetMembership should recover metadata", func(ctx C) {
				require.EventuallyWithT(t, func(c *assert.CollectT) {
					cinfo, err := clus.nodes[newNode].GetClusterInfo(t.Context(), 0)
					if !assert.NoError(c, err) {
						return
					}
					assert.Len(c, cinfo.Members, nrNode)
				}, testDuration, testTick)

				Convey("Then replication should be operate", func(ctx C) {
					for i := range snIDs {
						rctx, cancel := context.WithTimeout(t.Context(), testContextTimeout)
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
		const nrRep = 1
		nrNode := 5

		testSnapCount = 10
		defer func() { testSnapCount = 0 }()

		clus := newTestMetadataRepoCluster(t, nrNode, nrRep, false)
		Reset(func() {
			clus.Close(t)
		})
		clus.Start(t)
		clus.healthCheckAll(t)

		leader := clus.leader()
		So(leader, ShouldBeGreaterThan, -1)

		Convey("When follower restart with snapshot and some node leave", func(ctx C) {
			restartNode := (leader + 1) % nrNode
			leaveNode := (leader + 2) % nrNode

			require.EventuallyWithT(t, func(c *assert.CollectT) {
				peers := clus.getMembersFromNodeSnapshot(t, restartNode)
				assert.Len(c, peers, nrNode)
			}, testDuration, testTick)

			clus.stopNode(t, restartNode)
			clus.stopNode(t, leaveNode)

			rctx, cancel := context.WithTimeout(t.Context(), testContextTimeout)
			defer cancel()

			So(clus.nodes[leader].RemovePeer(rctx,
				types.ClusterID(0),
				clus.nodes[leaveNode].nodeID), ShouldBeNil)

			nrNode -= 1

			clus.restartNode(t, restartNode)

			Convey("Then GetMembership should return 4 peers", func(ctx C) {
				require.EventuallyWithT(t, func(c *assert.CollectT) {
					err := clus.healthCheckNode(restartNode)
					if !assert.NoError(c, err) {
						return
					}

					cinfo, err := clus.nodes[restartNode].GetClusterInfo(t.Context(), 0)
					if !assert.NoError(c, err) {
						return
					}
					assert.Len(c, cinfo.Members, nrNode)
				}, testDuration, testTick)
			})
		})
	})
}

func TestMRFailoverRestartWithOutdatedSnapshot(t *testing.T) {
	Convey("Given MR cluster with 3 peers", t, func(ctx C) {
		const (
			nrRep  = 1
			nrNode = 3
		)

		testSnapCount = 10
		defer func() { testSnapCount = 0 }()

		clus := newTestMetadataRepoCluster(t, nrNode, nrRep, false)
		Reset(func() {
			clus.Close(t)
		})
		clus.Start(t)
		clus.healthCheckAll(t)

		leader := clus.leader()
		So(leader, ShouldBeGreaterThan, -1)

		Convey("When follower restart with outdated snapshot", func(ctx C) {
			restartNode := (leader + 1) % nrNode

			require.EventuallyWithT(t, func(c *assert.CollectT) {
				peers := clus.getMembersFromNodeSnapshot(t, restartNode)
				assert.Len(c, peers, nrNode)
			}, testDuration, testTick)

			clus.stopNode(t, restartNode)

			appliedIdx := clus.nodes[restartNode].raftNode.appliedIndex

			require.EventuallyWithT(t, func(c *assert.CollectT) {
				snapshot := clus.getSnapshotFromNode(t, leader)
				assert.Greater(c, snapshot.Metadata.Index, appliedIdx+testSnapCount+1)
			}, testDuration, testTick)

			clus.restartNode(t, restartNode)

			Convey("Then node which is restarted should serve", func(ctx C) {
				require.EventuallyWithT(t, func(c *assert.CollectT) {
					err := clus.healthCheckNode(restartNode)
					if !assert.NoError(c, err) {
						return
					}

					serverAddr := clus.nodes[restartNode].GetServerAddr()
					assert.NotEmpty(c, serverAddr)
				}, testDuration, testTick)
			})
		})
	})
}

func TestMRFailoverRestartAlreadyLeavedNode(t *testing.T) {
	Convey("Given MR cluster with 3 peers", t, func(ctx C) {
		const nrRep = 1
		nrNode := 3
		testSnapCount = 10
		defer func() { testSnapCount = 0 }()

		clus := newTestMetadataRepoCluster(t, nrNode, nrRep, false)
		Reset(func() {
			clus.Close(t)
		})
		clus.Start(t)
		clus.healthCheckAll(t)

		leader := clus.leader()
		So(leader, ShouldBeGreaterThan, -1)

		Convey("When remove node during restart that", func(ctx C) {
			restartNode := (leader + 1) % nrNode

			require.EventuallyWithT(t, func(c *assert.CollectT) {
				peers := clus.getMembersFromNodeSnapshot(t, restartNode)
				assert.Len(c, peers, nrNode)
			}, testDuration, testTick)

			clus.stopNode(t, restartNode)

			rctx, cancel := context.WithTimeout(t.Context(), testContextTimeout)
			defer cancel()

			So(clus.nodes[leader].RemovePeer(rctx,
				types.ClusterID(0),
				clus.nodes[restartNode].nodeID), ShouldBeNil)

			nrNode -= 1

			require.EventuallyWithT(t, func(c *assert.CollectT) {
				peers := clus.getMembersFromNodeSnapshot(t, leader)
				assert.Len(c, peers, nrNode)
			}, testDuration, testTick)

			clus.restartNode(t, restartNode)

			Convey("Then the node should not serve", func(ctx C) {
				time.Sleep(10 * time.Second)
				So(clus.nodes[restartNode].GetServerAddr(), ShouldEqual, "")
			})
		})
	})
}

func TestMRFailoverRecoverReportCollector(t *testing.T) {
	Convey("Given MR cluster with 3 peers, 5 StorageNodes", t, func(ctx C) {
		const (
			nrRep         = 1
			nrNode        = 3
			nrStorageNode = 3
			nrLogStream   = 2
		)
		testSnapCount = 10

		clus := newTestMetadataRepoCluster(t, nrNode, nrRep, false)
		Reset(func() {
			clus.Close(t)
			testSnapCount = 0
		})
		clus.Start(t)
		clus.healthCheckAll(t)

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

				rctx, cancel := context.WithTimeout(t.Context(), testContextTimeout)
				defer cancel()

				err := clus.nodes[leader].RegisterStorageNode(rctx, sn)
				So(err, ShouldBeNil)
			}
		}

		require.EventuallyWithT(t, func(c *assert.CollectT) {
			numExecutors := clus.nodes[leader].reportCollector.NumExecutors()
			assert.Equal(c, nrStorageNode*nrRep, numExecutors)
		}, testDuration, testTick)

		err := clus.nodes[0].RegisterTopic(t.Context(), types.TopicID(1))
		So(err, ShouldBeNil)

		for i := 0; i < nrLogStream; i++ {
			lsID := types.MinLogStreamID + types.LogStreamID(i)
			ls := makeLogStream(types.TopicID(1), lsID, snIDs[i%nrStorageNode])

			rctx, cancel := context.WithTimeout(t.Context(), testContextTimeout)
			defer cancel()

			err := clus.nodes[0].RegisterLogStream(rctx, ls)
			So(err, ShouldBeNil)
		}

		require.EventuallyWithT(t, func(c *assert.CollectT) {
			numCommitters := clus.nodes[leader].reportCollector.NumCommitter()
			assert.Equal(c, nrLogStream*nrRep, numCommitters)
		}, testDuration, testTick)

		Convey("When follower restart with snapshot", func(ctx C) {
			require.EventuallyWithT(t, func(c *assert.CollectT) {
				peers := clus.getMembersFromNodeSnapshot(t, restartNode)
				assert.Len(c, peers, nrNode)
			}, testDuration, testTick)

			clus.restartNode(t, restartNode)

			require.EventuallyWithT(t, func(c *assert.CollectT) {
				err := clus.healthCheckNode(restartNode)
				if !assert.NoError(c, err) {
					return
				}

				serverAddr := clus.nodes[restartNode].GetServerAddr()
				assert.NotEmpty(c, serverAddr)
			}, testDuration, testTick)

			Convey("Then ReportCollector should recover", func(ctx C) {
				require.EventuallyWithT(t, func(c *assert.CollectT) {
					numExecutors := clus.nodes[restartNode].reportCollector.NumExecutors()
					numCommitters := clus.nodes[restartNode].reportCollector.NumCommitter()
					assert.Equal(c, nrStorageNode*nrRep, numExecutors)
					assert.Equal(c, nrLogStream*nrRep, numCommitters)
				}, testDuration, testTick)
			})
		})
	})
}

func TestMRProposeTimeout(t *testing.T) {
	Convey("Given MR which is not running", t, func(ctx C) {
		clus := newTestMetadataRepoCluster(t, 1, 1, false)
		Reset(func() {
			clus.Close(t)
		})
		mr := clus.nodes[0]

		Convey("When cli register SN with timeout", func(ctx C) {
			snID := types.StorageNodeID(time.Now().UnixNano())
			sn := &varlogpb.StorageNodeDescriptor{
				StorageNode: varlogpb.StorageNode{
					StorageNodeID: snID,
				},
			}

			rctx, cancel := context.WithTimeout(t.Context(), testContextTimeout)
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
		clus := newTestMetadataRepoCluster(t, 3, 1, false)
		clus.Start(t)
		Reset(func() {
			clus.Close(t)
		})

		clus.healthCheckAll(t)

		Convey("When cli register SN & transfer leader for dropping propose", func(ctx C) {
			leader := clus.leader()
			clus.nodes[leader].raftNode.transferLeadership(false) //nolint:errcheck,revive // TODO:: Handle an error returned.

			snID := types.StorageNodeID(time.Now().UnixNano())
			sn := &varlogpb.StorageNodeDescriptor{
				StorageNode: varlogpb.StorageNode{
					StorageNodeID: snID,
				},
			}

			rctx, cancel := context.WithTimeout(t.Context(), testContextTimeout)
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
		const nrRep = 1
		nrNode := 1

		clus := newTestMetadataRepoCluster(t, nrNode, nrRep, false)
		Reset(func() {
			clus.Close(t)
		})
		clus.Start(t)
		clus.healthCheckAll(t)

		require.EventuallyWithT(t, func(c *assert.CollectT) {
			peers := clus.getMembersFromNodeSnapshot(t, 0)
			assert.Len(c, peers, nrNode)
		}, testDuration, testTick)

		Convey("When new node join", func(ctx C) {
			for i := 0; i < 5; i++ {
				info, err := clus.nodes[0].GetClusterInfo(
					t.Context(),
					types.ClusterID(0),
				)
				So(err, ShouldBeNil)
				prevAppliedIndex := info.AppliedIndex

				newNode := nrNode
				clus.appendNewNode(t)
				clus.startNode(t, newNode)

				rctx, cancel := context.WithTimeout(t.Context(), testContextTimeout)
				defer cancel()

				So(clus.nodes[0].AddPeer(rctx,
					types.ClusterID(0),
					clus.nodes[newNode].nodeID,
					clus.peers[newNode]), ShouldBeNil)
				nrNode += 1

				info, err = clus.nodes[0].GetClusterInfo(
					t.Context(),
					types.ClusterID(0),
				)
				So(err, ShouldBeNil)
				So(info.AppliedIndex, ShouldBeGreaterThan, prevAppliedIndex)

				require.EventuallyWithT(t, func(c *assert.CollectT) {
					isMember := clus.nodes[newNode].IsMember()
					assert.True(c, isMember)
				}, testDuration, testTick)
			}

			Convey("Then it should be clustered", func(ctx C) {
				require.EventuallyWithT(t, func(c *assert.CollectT) {
					cinfo, err := clus.nodes[0].GetClusterInfo(t.Context(), types.ClusterID(0))
					if !assert.NoError(c, err) {
						return
					}

					assert.Len(c, cinfo.Members, nrNode)
				}, testDuration, testTick)
			})
		})
	})
}

func TestMRUnregisterTopic(t *testing.T) {
	Convey("Given 1 topic & 5 log streams", t, func(ctx C) {
		const (
			rep     = 1
			nrNodes = 1
			nrLS    = 5
			topicID = types.TopicID(1)
		)

		clus := newTestMetadataRepoCluster(t, nrNodes, rep, false)
		Reset(func() {
			clus.Close(t)
		})

		clus.Start(t)
		clus.healthCheckAll(t)

		snIDs := make([]types.StorageNodeID, rep)
		for i := range snIDs {
			snIDs[i] = types.StorageNodeID(i)

			sn := &varlogpb.StorageNodeDescriptor{
				StorageNode: varlogpb.StorageNode{
					StorageNodeID: snIDs[i],
				},
			}

			err := clus.nodes[0].RegisterStorageNode(t.Context(), sn)
			So(err, ShouldBeNil)
		}

		err := clus.nodes[0].RegisterTopic(t.Context(), topicID)
		So(err, ShouldBeNil)

		lsIDs := make([]types.LogStreamID, nrLS)
		for i := range lsIDs {
			lsIDs[i] = types.MinLogStreamID + types.LogStreamID(i)
		}

		for _, lsID := range lsIDs {
			ls := makeLogStream(types.TopicID(1), lsID, snIDs)
			err := clus.nodes[0].RegisterLogStream(t.Context(), ls)
			So(err, ShouldBeNil)
		}

		meta, _ := clus.nodes[0].GetMetadata(t.Context())
		So(len(meta.GetLogStreams()), ShouldEqual, nrLS)

		err = clus.nodes[0].UnregisterTopic(t.Context(), topicID)
		So(err, ShouldBeNil)

		meta, _ = clus.nodes[0].GetMetadata(t.Context())
		So(len(meta.GetLogStreams()), ShouldEqual, 0)
	})

	Convey("Given 1 topic & 1000 log streams", t, func(ctx C) {
		const (
			rep     = 1
			nrNodes = 1
			nrLS    = 1000
			topicID = types.TopicID(1)
		)

		clus := newTestMetadataRepoCluster(t, nrNodes, rep, false)
		Reset(func() {
			clus.Close(t)
		})

		mr := clus.nodes[0]

		clus.Start(t)
		clus.healthCheckAll(t)

		snIDs := make([]types.StorageNodeID, rep)
		for i := range snIDs {
			snIDs[i] = types.StorageNodeID(i)

			sn := &varlogpb.StorageNodeDescriptor{
				StorageNode: varlogpb.StorageNode{
					StorageNodeID: snIDs[i],
				},
			}

			err := mr.RegisterStorageNode(t.Context(), sn)
			So(err, ShouldBeNil)
		}

		err := mr.RegisterTopic(t.Context(), topicID)
		So(err, ShouldBeNil)

		lsIDs := make([]types.LogStreamID, nrLS)
		for i := range lsIDs {
			lsIDs[i] = types.MinLogStreamID + types.LogStreamID(i)
		}

		for _, lsID := range lsIDs {
			ls := makeLogStream(types.TopicID(1), lsID, snIDs)
			err := mr.RegisterLogStream(t.Context(), ls)
			So(err, ShouldBeNil)
		}

		meta, _ := mr.GetMetadata(t.Context())
		So(len(meta.GetLogStreams()), ShouldEqual, nrLS)

		// unregistering topic requires one raft consensus and pure in-memory
		// state machine operations, so it should be done in less than 500ms.
		require.EventuallyWithT(t, func(c *assert.CollectT) {
			err = mr.UnregisterTopic(t.Context(), topicID)
			require.NoError(c, err)
		}, 500*time.Millisecond, 10*time.Millisecond)
		So(err, ShouldBeNil)

		meta, _ = mr.GetMetadata(t.Context())
		So(len(meta.GetLogStreams()), ShouldEqual, 0)
	})
}

func TestMetadataRepository_MaxTopicsCount(t *testing.T) {
	const numNodes = 1
	const repFactor = 1
	const increaseUncommit = false

	Convey("MaxTopicsCount", t, func(C) {
		clus := newTestMetadataRepoCluster(t, numNodes, repFactor, increaseUncommit)
		Reset(func() {
			clus.Close(t)
		})

		clus.Start(t)
		clus.healthCheckAll(t)

		mr := clus.nodes[0]

		Convey("Limit is zero", func(C) {
			mr.storage.limits.maxTopicsCount = 0
			err := mr.RegisterTopic(t.Context(), 1)
			So(err, ShouldNotBeNil)
		})

		Convey("Limit is one", func(C) {
			mr.storage.limits.maxTopicsCount = 1

			err := mr.RegisterTopic(t.Context(), 1)
			So(err, ShouldBeNil)

			err = mr.RegisterTopic(t.Context(), 2)
			So(err, ShouldNotBeNil)
			So(status.Code(err), ShouldEqual, codes.ResourceExhausted)

			err = mr.RegisterTopic(t.Context(), 1)
			So(err, ShouldBeNil)

			err = mr.UnregisterTopic(t.Context(), 1)
			So(err, ShouldBeNil)

			err = mr.RegisterTopic(t.Context(), 2)
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
		clus := newTestMetadataRepoCluster(t, numNodes, repFactor, increaseUncommit)
		Reset(func() {
			clus.Close(t)
		})

		clus.Start(t)
		clus.healthCheckAll(t)

		mr := clus.nodes[0]
		ctx := t.Context()

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
		const (
			nrTopics = 3
			nrLS     = 2
			rep      = 2
		)

		clus := newTestMetadataRepoCluster(t, 1, rep, false)
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
			clus.Start(t)
			Reset(func() {
				clus.Close(t)
			})

			clus.healthCheckAll(t)

			for topicID, lsIds := range topicLogStreamID {
				preVersion := mr.storage.GetLastCommitVersion()

				for _, lsID := range lsIds {
					for i := 0; i < rep; i++ {
						require.EventuallyWithT(t, func(c *assert.CollectT) {
							report := makeUncommitReport(snIDs[i], types.InvalidVersion, types.InvalidGLSN, lsID, types.MinLLSN, uint64(2+i))
							err := mr.proposeReport(report.StorageNodeID, report.UncommitReport)
							assert.NoError(c, err)
						}, testDuration, testTick)
					}
				}

				// global commit (2, 2) highest glsn: 4
				require.EventuallyWithT(t, func(c *assert.CollectT) {
					hwm, _ := mr.GetLastCommitResults().LastHighWatermark(topicID, -1)
					assert.Equal(c, types.GLSN(4), hwm)
				}, testDuration, testTick)

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
						require.EventuallyWithT(t, func(c *assert.CollectT) {
							report := makeUncommitReport(snIDs[i], preVersion, types.InvalidGLSN, lsID, types.MinLLSN, uint64(2+i))
							err := mr.proposeReport(report.StorageNodeID, report.UncommitReport)
							assert.NoError(c, err)
						}, testDuration, testTick)
					}
				}

				// global commit (2, 2) highest glsn: 4
				require.EventuallyWithT(t, func(c *assert.CollectT) {
					hwm, _ := mr.GetLastCommitResults().LastHighWatermark(topicID, -1)
					assert.Equal(c, types.GLSN(4), hwm)
				}, testDuration, testTick)
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

			clus.Start(t)
			Reset(func() {
				clus.Close(t)
			})

			clus.healthCheckAll(t)

			for topicID, lsIds := range topicLogStreamID {
				preVersion := mr.storage.GetLastCommitVersion()

				for _, lsID := range lsIds {
					for i := 0; i < rep; i++ {
						require.EventuallyWithT(t, func(c *assert.CollectT) {
							report := makeUncommitReport(snIDs[i], types.InvalidVersion, types.InvalidGLSN, lsID, types.MinLLSN, uint64(2+i))
							err := mr.proposeReport(report.StorageNodeID, report.UncommitReport)
							assert.NoError(c, err)
						}, testDuration, testTick)
					}
				}

				// global commit (2, 2, 2) highest glsn: 6
				require.EventuallyWithT(t, func(c *assert.CollectT) {
					hwm, _ := mr.GetLastCommitResults().LastHighWatermark(topicID, -1)
					assert.Equal(c, types.GLSN(6), hwm)
				}, testDuration, testTick)

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
						require.EventuallyWithT(t, func(c *assert.CollectT) {
							report := makeUncommitReport(snIDs[i], types.InvalidVersion, types.InvalidGLSN, lsID, types.MinLLSN, uint64(5+i))
							err := mr.proposeReport(report.StorageNodeID, report.UncommitReport)
							assert.NoError(c, err)
						}, testDuration, testTick)
					}
				}

				// global commit (5, 5, 5) highest glsn: 15
				require.EventuallyWithT(t, func(c *assert.CollectT) {
					hwm, _ := mr.GetLastCommitResults().LastHighWatermark(topicID, -1)
					assert.Equal(c, types.GLSN(15), hwm)
				}, testDuration, testTick)

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
		const (
			nrRep        = 1
			nrNode       = 1
			nrTopic      = 2
			nrLSPerTopic = 2
			nrSN         = nrTopic * nrLSPerTopic
		)

		clus := newTestMetadataRepoCluster(t, nrNode, nrRep, false)
		clus.Start(t)
		Reset(func() {
			clus.Close(t)
		})
		clus.healthCheckAll(t)

		leader := clus.leader()
		So(leader, ShouldBeGreaterThan, -1)

		// register SN & LS
		clus.initDummyStorageNode(t, nrSN, nrTopic)

		require.EventuallyWithT(t, func(c *assert.CollectT) {
			snIDs := clus.getSNIDs()
			assert.Len(c, snIDs, nrSN)
		}, testDuration, testTick)

		// append to SN
		snIDs := clus.getSNIDs()
		for i := 0; i < 100; i++ {
			for _, snID := range snIDs {
				snCli := clus.reporterClientFac.(*DummyStorageNodeClientFactory).lookupClient(snID)
				snCli.increaseUncommitted(0)
			}

			for _, snID := range snIDs {
				snCli := clus.reporterClientFac.(*DummyStorageNodeClientFactory).lookupClient(snID)

				require.EventuallyWithT(t, func(c *assert.CollectT) {
					numUncommitted := snCli.numUncommitted(0)
					assert.Zero(c, numUncommitted)
				}, testDuration, testTick)
			}
		}
	})
}

func TestMRCatchupUnsealedLogstream(t *testing.T) {
	Convey("Given MR cluster", t, func(ctx C) {
		const (
			nrRep   = 1
			nrTopic = 1
			nrSN    = 2
		)
		clus := newTestMetadataRepoCluster(t, 1, nrRep, false)
		Reset(func() {
			clus.Close(t)
		})

		clus.Start(t)
		clus.healthCheckAll(t)

		mr := clus.nodes[0]

		// register SN & LS
		clus.initDummyStorageNode(t, nrSN, nrTopic)

		err := mr.RegisterTopic(t.Context(), types.TopicID(1))
		So(err, ShouldBeNil)

		require.EventuallyWithT(t, func(c *assert.CollectT) {
			snIDs := clus.getSNIDs()
			assert.Len(c, snIDs, nrSN)
		}, testDuration, testTick)

		Convey("After all ls belonging to the cluster are sealed, when a specific ls is unsealed", func(ctx C) {
			// append to LS[0]
			snIDs := clus.getSNIDs()
			snCli := clus.reporterClientFac.(*DummyStorageNodeClientFactory).lookupClient(snIDs[0])
			snCli.increaseUncommitted(0)

			require.EventuallyWithT(t, func(c *assert.CollectT) {
				numUncommitted := snCli.numUncommitted(0)
				assert.Zero(c, numUncommitted)
			}, testDuration, testTick)

			// seal LS[0]
			_, err = mr.Seal(t.Context(), types.LogStreamID(snIDs[0]))
			So(err, ShouldBeNil)

			require.EventuallyWithT(t, func(c *assert.CollectT) {
				meta, err := mr.GetMetadata(t.Context())
				if !assert.NoError(c, err) {
					return
				}

				ls := meta.GetLogStream(types.LogStreamID(snIDs[0]))
				assert.Equal(c, varlogpb.LogStreamStatusSealed, ls.Status)
			}, testDuration, testTick)

			// append to LS[1]
			snCli = clus.reporterClientFac.(*DummyStorageNodeClientFactory).lookupClient(snIDs[1])
			snCli.increaseUncommitted(0)

			require.EventuallyWithT(t, func(c *assert.CollectT) {
				numUncommitted := snCli.numUncommitted(0)
				assert.Zero(c, numUncommitted)
			}, testDuration, testTick)

			// seal LS[1]
			_, err = mr.Seal(t.Context(), types.LogStreamID(snIDs[1]))
			So(err, ShouldBeNil)

			require.EventuallyWithT(t, func(c *assert.CollectT) {
				meta, err := mr.GetMetadata(t.Context())
				if !assert.NoError(c, err) {
					return
				}

				ls := meta.GetLogStream(types.LogStreamID(snIDs[1]))
				assert.Equal(c, varlogpb.LogStreamStatusSealed, ls.Status)
			}, testDuration, testTick)

			// unseal LS[0]
			err = mr.Unseal(t.Context(), types.LogStreamID(snIDs[0]))
			So(err, ShouldBeNil)

			Convey("Then the ls should appenable", func(ctx C) {
				// append to LS[0]
				snCli = clus.reporterClientFac.(*DummyStorageNodeClientFactory).lookupClient(snIDs[0])
				snCli.increaseUncommitted(0)

				require.EventuallyWithT(t, func(c *assert.CollectT) {
					numUncommitted := snCli.numUncommitted(0)
					assert.Zero(c, numUncommitted)
				}, testDuration, testTick)
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
				_, err := client.AddPeer(t.Context(), &mrpb.AddPeerRequest{
					Peer: mrpb.PeerInfo{
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
				_, err := client.AddPeer(t.Context(), &mrpb.AddPeerRequest{
					Peer: mrpb.PeerInfo{
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
			require.EventuallyWithT(t, func(c *assert.CollectT) {
				addr := node.endpointAddr.Load()
				if !assert.NotNil(c, addr) {
					return
				}
			}, 3*time.Second, 100*time.Millisecond)
			addr := node.endpointAddr.Load().(string)

			require.EventuallyWithT(t, func(c *assert.CollectT) {
				conn, err := rpc.NewConn(t.Context(), addr)
				assert.NoError(c, err)
				defer func() {
					err := conn.Close()
					assert.NoError(c, err)
				}()

				healthClient := grpc_health_v1.NewHealthClient(conn.Conn)
				_, err = healthClient.Check(t.Context(), &grpc_health_v1.HealthCheckRequest{})
				assert.NoError(c, err)
			}, 3*time.Second, 100*time.Millisecond)

			conn, err := rpc.NewConn(t.Context(), addr)
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
				_, err := client.RemovePeer(t.Context(), &mrpb.RemovePeerRequest{
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
				_, err := client.RemovePeer(t.Context(), &mrpb.RemovePeerRequest{
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
			require.EventuallyWithT(t, func(c *assert.CollectT) {
				addr := node.endpointAddr.Load()
				if !assert.NotNil(c, addr) {
					return
				}
			}, 3*time.Second, 100*time.Millisecond)
			addr := node.endpointAddr.Load().(string)

			require.EventuallyWithT(t, func(c *assert.CollectT) {
				conn, err := rpc.NewConn(t.Context(), addr)
				assert.NoError(c, err)
				defer func() {
					err := conn.Close()
					assert.NoError(c, err)
				}()

				healthClient := grpc_health_v1.NewHealthClient(conn.Conn)
				_, err = healthClient.Check(t.Context(), &grpc_health_v1.HealthCheckRequest{})
				assert.NoError(c, err)
			}, 3*time.Second, 100*time.Millisecond)

			conn, err := rpc.NewConn(t.Context(), addr)
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
