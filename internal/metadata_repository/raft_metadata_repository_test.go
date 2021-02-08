package metadata_repository

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
	"go.etcd.io/etcd/raft/raftpb"
	"go.uber.org/multierr"
	"go.uber.org/zap"
	"google.golang.org/grpc/health/grpc_health_v1"

	"github.daumkakao.com/varlog/varlog/pkg/rpc"
	"github.daumkakao.com/varlog/varlog/pkg/types"
	"github.daumkakao.com/varlog/varlog/pkg/util/testutil"
	"github.daumkakao.com/varlog/varlog/pkg/util/testutil/ports"
	"github.daumkakao.com/varlog/varlog/pkg/verrors"
	"github.daumkakao.com/varlog/varlog/proto/mrpb"
	"github.daumkakao.com/varlog/varlog/proto/snpb"
	"github.daumkakao.com/varlog/varlog/proto/varlogpb"
	"github.daumkakao.com/varlog/varlog/vtesting"
)

type metadataRepoCluster struct {
	nrRep             int
	peers             []string
	nodes             []*RaftMetadataRepository
	reporterClientFac ReporterClientFactory
	logger            *zap.Logger
	portLease         *ports.Lease
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

	clus := &metadataRepoCluster{
		nrRep:             nrRep,
		peers:             peers,
		nodes:             nodes,
		reporterClientFac: NewDummyReporterClientFactory(1, !increseUncommit),
		logger:            zap.L(),
		portLease:         portLease,
	}

	for i := range clus.peers {
		clus.clear(i)
		clus.createMetadataRepo(i, false)
	}

	return clus
}

func (clus *metadataRepoCluster) clear(idx int) {
	if idx < 0 || idx >= len(clus.nodes) {
		return
	}
	nodeID := types.NewNodeIDFromURL(clus.peers[idx])
	os.RemoveAll(fmt.Sprintf("%s/wal/%d", vtesting.TestRaftDir(), nodeID))
	os.RemoveAll(fmt.Sprintf("%s/snap/%d", vtesting.TestRaftDir(), nodeID))
}

func (clus *metadataRepoCluster) createMetadataRepo(idx int, join bool) error {
	if idx < 0 || idx >= len(clus.nodes) {
		return errors.New("out of range")
	}

	peers := make([]string, len(clus.peers))
	copy(peers, clus.peers)
	options := &MetadataRepositoryOptions{
		ClusterID:         types.ClusterID(1),
		RaftAddress:       clus.peers[idx],
		Join:              join,
		SnapCount:         testSnapCount,
		SnapCatchUpCount:  testSnapCount,
		RaftTick:          vtesting.TestRaftTick(),
		RaftDir:           vtesting.TestRaftDir(),
		CommitTick:        vtesting.TestCommitTick(),
		RPCTimeout:        vtesting.TimeoutAccordingToProcCnt(DefaultRPCTimeout),
		NumRep:            clus.nrRep,
		Peers:             peers,
		RPCBindAddress:    ":0",
		ReporterClientFac: clus.reporterClientFac,
		Logger:            clus.logger,
	}

	options.CollectorName = "nop"
	options.CollectorEndpoint = "localhost:55680"

	clus.nodes[idx] = NewRaftMetadataRepository(options)
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

func (clus *metadataRepoCluster) Start() {
	clus.logger.Info("cluster start")
	for i := range clus.nodes {
		clus.start(i)
	}
	clus.logger.Info("cluster complete")
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

	clus.stop(idx)
	clus.createMetadataRepo(idx, false)
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

func (clus *metadataRepoCluster) hasSnapshot(idx int) (bool, error) {
	if idx < 0 || idx >= len(clus.nodes) {
		return false, errors.New("out or range")
	}

	return clus.nodes[idx].raftNode.loadSnapshot() != nil, nil
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
	for nodeID, peer := range stateMachine.Peers {
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

// Close closes all cluster nodes
func (clus *metadataRepoCluster) Close() error {
	var err error
	for i := range clus.peers {
		err = multierr.Append(err, clus.close(i))
	}
	err = multierr.Append(err, os.RemoveAll(vtesting.TestRaftDir()))
	err = multierr.Append(err, clus.portLease.Release())
	testutil.GC()
	return err
}

func (clus *metadataRepoCluster) healthCheck(idx int) bool {
	f := clus.nodes[idx].endpointAddr.Load()
	if f == nil {
		return false
	}
	_, port, _ := net.SplitHostPort(f.(string))
	endpoint := fmt.Sprintf("localhost:%s", port)

	conn, err := rpc.NewBlockingConn(endpoint)
	if err != nil {
		return false
	}
	defer conn.Close()

	healthClient := grpc_health_v1.NewHealthClient(conn.Conn)
	if _, err := healthClient.Check(context.Background(), &grpc_health_v1.HealthCheckRequest{}); err != nil {
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

func (clus *metadataRepoCluster) leaderElected() bool {
	//return clus.leader() >= 0

	for _, n := range clus.nodes {
		if !n.hasLeader() {
			return false
		}
	}

	return true
}

func (clus *metadataRepoCluster) leaderFail() bool {
	leader := clus.leader()
	if leader < 0 {
		return false
	}

	clus.stop(leader)
	return true
}

func (clus *metadataRepoCluster) closeNoErrors(t *testing.T) {
	clus.logger.Info("cluster stop")
	if err := clus.Close(); err != nil {
		t.Log(err)
	}
	clus.logger.Info("cluster stop complete")
}

func makeUncommitReport(snID types.StorageNodeID, knownHighWatermark types.GLSN, lsID types.LogStreamID, offset types.LLSN, length uint64) *mrpb.Report {
	report := &mrpb.Report{
		StorageNodeID: snID,
	}
	u := &snpb.LogStreamUncommitReport{
		LogStreamID:           lsID,
		HighWatermark:         knownHighWatermark,
		UncommittedLLSNOffset: offset,
		UncommittedLLSNLength: length,
	}
	report.UncommitReport = append(report.UncommitReport, u)

	return report
}

func appendUncommitReport(report *mrpb.Report, knownHighWatermark types.GLSN, lsID types.LogStreamID, offset types.LLSN, length uint64) *mrpb.Report {
	u := &snpb.LogStreamUncommitReport{
		LogStreamID:           lsID,
		HighWatermark:         knownHighWatermark,
		UncommittedLLSNOffset: offset,
		UncommittedLLSNLength: length,
	}
	report.UncommitReport = append(report.UncommitReport, u)

	return report
}

func makeLogStream(lsID types.LogStreamID, snIDs []types.StorageNodeID) *varlogpb.LogStreamDescriptor {
	ls := &varlogpb.LogStreamDescriptor{
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

		snIDs := make([]types.StorageNodeID, rep)
		for i := range snIDs {
			snIDs[i] = types.StorageNodeID(i)

			sn := &varlogpb.StorageNodeDescriptor{
				StorageNodeID: snIDs[i],
			}

			err := mr.storage.registerStorageNode(sn)
			So(err, ShouldBeNil)
		}
		lsId := types.LogStreamID(0)
		notExistSnID := types.StorageNodeID(rep)

		report := makeUncommitReport(snIDs[0], types.InvalidGLSN, lsId, types.MinLLSN, 2)
		mr.applyReport(report)

		for _, snId := range snIDs {
			r := mr.storage.LookupUncommitReport(lsId, snId)
			So(r, ShouldBeNil)
		}

		Convey("UncommitReport should register when register LogStream", func(ctx C) {
			ls := makeLogStream(lsId, snIDs)
			err := mr.storage.registerLogStream(ls)
			So(err, ShouldBeNil)

			for _, snId := range snIDs {
				r := mr.storage.LookupUncommitReport(lsId, snId)
				So(r, ShouldNotBeNil)
			}

			Convey("Report should not apply if snID is not exist in UncommitReport", func(ctx C) {
				report := makeUncommitReport(notExistSnID, types.InvalidGLSN, lsId, types.MinLLSN, 2)
				mr.applyReport(report)

				r := mr.storage.LookupUncommitReport(lsId, notExistSnID)
				So(r, ShouldBeNil)
			})

			Convey("Report should apply if snID is exist in UncommitReport", func(ctx C) {
				snId := snIDs[0]
				report := makeUncommitReport(snId, types.InvalidGLSN, lsId, types.MinLLSN, 2)
				mr.applyReport(report)

				r := mr.storage.LookupUncommitReport(lsId, snId)
				So(r, ShouldNotBeNil)
				So(r.UncommittedLLSNEnd(), ShouldEqual, types.MinLLSN+types.LLSN(2))

				Convey("Report which have bigger END LLSN Should be applied", func(ctx C) {
					report := makeUncommitReport(snId, types.InvalidGLSN, lsId, types.MinLLSN, 3)
					mr.applyReport(report)

					r := mr.storage.LookupUncommitReport(lsId, snId)
					So(r, ShouldNotBeNil)
					So(r.UncommittedLLSNEnd(), ShouldEqual, types.MinLLSN+types.LLSN(3))
				})

				Convey("Report which have smaller END LLSN Should Not be applied", func(ctx C) {
					report := makeUncommitReport(snId, types.InvalidGLSN, lsId, types.MinLLSN, 1)
					mr.applyReport(report)

					r := mr.storage.LookupUncommitReport(lsId, snId)
					So(r, ShouldNotBeNil)
					So(r.UncommittedLLSNEnd(), ShouldNotEqual, types.MinLLSN+types.LLSN(1))
				})
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
			snIDs[i] = types.StorageNodeID(i)
			sn := &varlogpb.StorageNodeDescriptor{
				StorageNodeID: snIDs[i],
			}

			err := mr.storage.registerStorageNode(sn)
			So(err, ShouldBeNil)
		}
		lsId := types.LogStreamID(0)
		ls := makeLogStream(lsId, snIDs)
		err := mr.storage.registerLogStream(ls)
		So(err, ShouldBeNil)

		Convey("LogStream which all reports have not arrived cannot be commit", func(ctx C) {
			report := makeUncommitReport(snIDs[0], types.InvalidGLSN, lsId, types.MinLLSN, 2)
			mr.applyReport(report)

			replicas := mr.storage.LookupUncommitReports(lsId)
			_, minHWM, nrCommit := mr.calculateCommit(replicas)
			So(nrCommit, ShouldEqual, 0)
			So(minHWM, ShouldEqual, types.InvalidGLSN)
		})

		Convey("LogStream which all reports are disjoint cannot be commit", func(ctx C) {
			report := makeUncommitReport(snIDs[0], types.GLSN(10), lsId, types.MinLLSN+types.LLSN(5), 1)
			mr.applyReport(report)

			report = makeUncommitReport(snIDs[1], types.GLSN(7), lsId, types.MinLLSN+types.LLSN(3), 2)
			mr.applyReport(report)

			replicas := mr.storage.LookupUncommitReports(lsId)
			knownHWM, minHWM, nrCommit := mr.calculateCommit(replicas)
			So(nrCommit, ShouldEqual, 0)
			So(knownHWM, ShouldEqual, types.GLSN(10))
			So(minHWM, ShouldEqual, types.GLSN(7))
		})

		Convey("LogStream Should be commit where replication is completed", func(ctx C) {
			report := makeUncommitReport(snIDs[0], types.GLSN(10), lsId, types.MinLLSN+types.LLSN(3), 3)
			mr.applyReport(report)

			report = makeUncommitReport(snIDs[1], types.GLSN(9), lsId, types.MinLLSN+types.LLSN(3), 2)
			mr.applyReport(report)

			replicas := mr.storage.LookupUncommitReports(lsId)
			knownHWM, minHWM, nrCommit := mr.calculateCommit(replicas)
			So(nrCommit, ShouldEqual, 2)
			So(minHWM, ShouldEqual, types.GLSN(9))
			So(knownHWM, ShouldEqual, types.GLSN(10))
		})
	})
}

func TestMRGlobalCommit(t *testing.T) {
	Convey("Calculate commit", t, func(ctx C) {
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
				snIDs[i][j] = types.StorageNodeID(i*2 + j)

				sn := &varlogpb.StorageNodeDescriptor{
					StorageNodeID: snIDs[i][j],
				}

				err := mr.storage.registerStorageNode(sn)
				So(err, ShouldBeNil)
			}
		}

		lsIds := make([]types.LogStreamID, 2)
		for i := range lsIds {
			lsIds[i] = types.LogStreamID(i)
		}

		for i, lsId := range lsIds {
			ls := makeLogStream(lsId, snIDs[i])
			err := mr.storage.registerLogStream(ls)
			So(err, ShouldBeNil)
		}

		clus.Start()
		So(testutil.CompareWaitN(10, func() bool {
			return clus.healthCheckAll()
		}), ShouldBeTrue)

		Convey("global commit", func(ctx C) {
			So(testutil.CompareWaitN(10, func() bool {
				report := makeUncommitReport(snIDs[0][0], types.InvalidGLSN, lsIds[0], types.MinLLSN, 2)
				return mr.proposeReport(report.StorageNodeID, report.UncommitReport) == nil
			}), ShouldBeTrue)

			So(testutil.CompareWaitN(10, func() bool {
				report := makeUncommitReport(snIDs[0][1], types.InvalidGLSN, lsIds[0], types.MinLLSN, 2)
				return mr.proposeReport(report.StorageNodeID, report.UncommitReport) == nil
			}), ShouldBeTrue)

			So(testutil.CompareWaitN(10, func() bool {
				report := makeUncommitReport(snIDs[1][0], types.InvalidGLSN, lsIds[1], types.MinLLSN, 4)
				return mr.proposeReport(report.StorageNodeID, report.UncommitReport) == nil
			}), ShouldBeTrue)

			So(testutil.CompareWaitN(10, func() bool {
				report := makeUncommitReport(snIDs[1][1], types.InvalidGLSN, lsIds[1], types.MinLLSN, 3)
				return mr.proposeReport(report.StorageNodeID, report.UncommitReport) == nil
			}), ShouldBeTrue)

			// global commit (2, 3) highest glsn: 5
			So(testutil.CompareWaitN(10, func() bool {
				return mr.storage.GetHighWatermark() == types.GLSN(5)
			}), ShouldBeTrue)

			Convey("LogStream should be dedup", func(ctx C) {
				So(testutil.CompareWaitN(10, func() bool {
					report := makeUncommitReport(snIDs[0][0], types.InvalidGLSN, lsIds[0], types.MinLLSN, 3)
					return mr.proposeReport(report.StorageNodeID, report.UncommitReport) == nil
				}), ShouldBeTrue)

				So(testutil.CompareWaitN(10, func() bool {
					report := makeUncommitReport(snIDs[0][1], types.InvalidGLSN, lsIds[0], types.MinLLSN, 2)
					return mr.proposeReport(report.StorageNodeID, report.UncommitReport) == nil
				}), ShouldBeTrue)

				time.Sleep(vtesting.TimeoutUnitTimesFactor(1))

				So(testutil.CompareWaitN(50, func() bool {
					return mr.storage.GetHighWatermark() == types.GLSN(5)
				}), ShouldBeTrue)
			})

			Convey("LogStream which have wrong GLSN but have uncommitted should commit", func(ctx C) {
				So(testutil.CompareWaitN(10, func() bool {
					report := makeUncommitReport(snIDs[0][0], types.InvalidGLSN, lsIds[0], types.MinLLSN, 6)
					return mr.proposeReport(report.StorageNodeID, report.UncommitReport) == nil
				}), ShouldBeTrue)

				So(testutil.CompareWaitN(10, func() bool {
					report := makeUncommitReport(snIDs[0][1], types.InvalidGLSN, lsIds[0], types.MinLLSN, 6)
					return mr.proposeReport(report.StorageNodeID, report.UncommitReport) == nil
				}), ShouldBeTrue)

				So(testutil.CompareWaitN(10, func() bool {
					return mr.storage.GetHighWatermark() == types.GLSN(9)
				}), ShouldBeTrue)
			})
		})
	})
}

func TestMRGlobalCommitConsistency(t *testing.T) {
	Convey("Given 2 mr nodes & 5 log streams", t, func(ctx C) {
		rep := 1
		nrNodes := 2
		nrLS := 5

		clus := newMetadataRepoCluster(nrNodes, rep, false)
		Reset(func() {
			clus.closeNoErrors(t)
		})

		snIDs := make([]types.StorageNodeID, rep)
		for i := range snIDs {
			snIDs[i] = types.StorageNodeID(i)

			sn := &varlogpb.StorageNodeDescriptor{
				StorageNodeID: snIDs[i],
			}

			for j := 0; j < nrNodes; j++ {
				err := clus.nodes[j].storage.registerStorageNode(sn)
				So(err, ShouldBeNil)
			}
		}

		lsIDs := make([]types.LogStreamID, nrLS)
		for i := range lsIDs {
			lsIDs[i] = types.LogStreamID(i)
		}

		for _, lsID := range lsIDs {
			ls := makeLogStream(lsID, snIDs)
			for i := 0; i < nrNodes; i++ {
				err := clus.nodes[i].storage.registerLogStream(ls)
				So(err, ShouldBeNil)
			}
		}

		clus.Start()
		So(testutil.CompareWaitN(10, func() bool {
			return clus.healthCheckAll()
		}), ShouldBeTrue)

		Convey("Then, it should calulate same glsn for each log streams", func(ctx C) {
			for i := 0; i < 100; i++ {
				var report *mrpb.Report
				for j, lsID := range lsIDs {
					if j == 0 {
						report = makeUncommitReport(snIDs[0], types.GLSN(i*nrLS), lsID, types.LLSN(i+1), 1)
					} else {
						report = appendUncommitReport(report, types.GLSN(i*nrLS), lsID, types.LLSN(i+1), 1)
					}
				}

				So(testutil.CompareWaitN(10, func() bool {
					return clus.nodes[0].proposeReport(report.StorageNodeID, report.UncommitReport) == nil
				}), ShouldBeTrue)

				for j := 0; j < nrNodes; j++ {
					So(testutil.CompareWaitN(10, func() bool {
						return clus.nodes[j].storage.GetHighWatermark() == types.GLSN(nrLS*(i+1))
					}), ShouldBeTrue)

					for k, lsID := range lsIDs {
						So(clus.nodes[j].getLastCommitted(lsID), ShouldEqual, types.GLSN((nrLS*i)+k+1))
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
		clus.Start()
		So(testutil.CompareWaitN(10, func() bool {
			return clus.healthCheckAll()
		}), ShouldBeTrue)

		snID := types.StorageNodeID(0)
		snIDs := make([]types.StorageNodeID, 1)
		snIDs = append(snIDs, snID)

		lsID := types.LogStreamID(snID)

		sn := &varlogpb.StorageNodeDescriptor{
			StorageNodeID: snID,
		}

		rctx, cancel := context.WithTimeout(context.Background(), vtesting.TimeoutUnitTimesFactor(50))
		defer cancel()

		err := clus.nodes[0].RegisterStorageNode(rctx, sn)
		So(err, ShouldBeNil)

		So(testutil.CompareWaitN(50, func() bool {
			return clus.reporterClientFac.(*DummyReporterClientFactory).lookupClient(snID) != nil
		}), ShouldBeTrue)

		ls := makeLogStream(lsID, snIDs)
		rctx, cancel = context.WithTimeout(context.Background(), vtesting.TimeoutUnitTimesFactor(50))
		defer cancel()
		err = clus.nodes[0].RegisterLogStream(rctx, ls)
		So(err, ShouldBeNil)

		reporterClient := clus.reporterClientFac.(*DummyReporterClientFactory).lookupClient(snID)
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
			StorageNodeID: types.StorageNodeID(0),
		}

		requestNum := atomic.LoadUint64(&mr.requestNum)

		var wg sync.WaitGroup
		var st sync.WaitGroup

		st.Add(1)
		wg.Add(1)
		go func() {
			defer wg.Done()
			rctx, cancel := context.WithTimeout(context.Background(), vtesting.TimeoutUnitTimesFactor(1))
			defer cancel()
			st.Done()
			mr.RegisterStorageNode(rctx, sn)
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
			StorageNodeID: types.StorageNodeID(0),
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
			StorageNodeID: types.StorageNodeID(0),
		}

		rctx, cancel := context.WithTimeout(context.Background(), vtesting.TimeoutUnitTimesFactor(1))
		defer cancel()

		requestNum := atomic.LoadUint64(&mr.requestNum)
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
		clus.Start()
		So(testutil.CompareWaitN(50, func() bool {
			return clus.healthCheckAll()
		}), ShouldBeTrue)

		mr := clus.nodes[0]

		So(testutil.CompareWaitN(50, func() bool {
			return mr.storage.LookupEndpoint(mr.nodeID) != ""
		}), ShouldBeTrue)

		sn := &varlogpb.StorageNodeDescriptor{
			StorageNodeID: types.StorageNodeID(0),
		}

		requestNum := atomic.LoadUint64(&mr.requestNum)
		err := mr.RegisterStorageNode(context.TODO(), sn)
		So(err, ShouldBeNil)

		_, ok := mr.requestMap.Load(requestNum + 1)
		So(ok, ShouldBeFalse)
	})
}

func TestMRGetLastCommitted(t *testing.T) {
	Convey("getLastCommitted", t, func(ctx C) {
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
				snIDs[i][j] = types.StorageNodeID(i*2 + j)

				sn := &varlogpb.StorageNodeDescriptor{
					StorageNodeID: snIDs[i][j],
				}

				err := mr.storage.registerStorageNode(sn)
				So(err, ShouldBeNil)
			}
		}

		lsIds := make([]types.LogStreamID, 2)
		for i := range lsIds {
			lsIds[i] = types.LogStreamID(i)
		}

		for i, lsId := range lsIds {
			ls := makeLogStream(lsId, snIDs[i])
			err := mr.storage.registerLogStream(ls)
			So(err, ShouldBeNil)
		}

		clus.Start()
		So(testutil.CompareWaitN(10, func() bool {
			return clus.healthCheckAll()
		}), ShouldBeTrue)

		Convey("getLastCommitted should return last committed GLSN", func(ctx C) {
			preHighWatermark := mr.storage.GetHighWatermark()

			So(testutil.CompareWaitN(10, func() bool {
				report := makeUncommitReport(snIDs[0][0], types.InvalidGLSN, lsIds[0], types.MinLLSN, 2)
				return mr.proposeReport(report.StorageNodeID, report.UncommitReport) == nil
			}), ShouldBeTrue)

			So(testutil.CompareWaitN(10, func() bool {
				report := makeUncommitReport(snIDs[0][1], types.InvalidGLSN, lsIds[0], types.MinLLSN, 2)
				return mr.proposeReport(report.StorageNodeID, report.UncommitReport) == nil
			}), ShouldBeTrue)

			So(testutil.CompareWaitN(10, func() bool {
				report := makeUncommitReport(snIDs[1][0], types.InvalidGLSN, lsIds[1], types.MinLLSN, 4)
				return mr.proposeReport(report.StorageNodeID, report.UncommitReport) == nil
			}), ShouldBeTrue)

			So(testutil.CompareWaitN(10, func() bool {
				report := makeUncommitReport(snIDs[1][1], types.InvalidGLSN, lsIds[1], types.MinLLSN, 3)
				return mr.proposeReport(report.StorageNodeID, report.UncommitReport) == nil
			}), ShouldBeTrue)

			// global commit (2, 3) highest glsn: 5
			So(testutil.CompareWaitN(10, func() bool {
				return mr.storage.GetHighWatermark() == types.GLSN(5)
			}), ShouldBeTrue)

			So(mr.numCommitSince(lsIds[0], preHighWatermark), ShouldEqual, 2)
			So(mr.numCommitSince(lsIds[1], preHighWatermark), ShouldEqual, 3)

			Convey("getLastCommitted should return same if not committed", func(ctx C) {
				for i := 0; i < 10; i++ {
					preHighWatermark := mr.storage.GetHighWatermark()

					So(testutil.CompareWaitN(10, func() bool {
						report := makeUncommitReport(snIDs[1][0], types.InvalidGLSN, lsIds[1], types.MinLLSN, uint64(4+i))
						return mr.proposeReport(report.StorageNodeID, report.UncommitReport) == nil
					}), ShouldBeTrue)

					So(testutil.CompareWaitN(10, func() bool {
						report := makeUncommitReport(snIDs[1][1], types.InvalidGLSN, lsIds[1], types.MinLLSN, uint64(4+i))
						return mr.proposeReport(report.StorageNodeID, report.UncommitReport) == nil
					}), ShouldBeTrue)

					So(testutil.CompareWaitN(50, func() bool {
						return mr.storage.GetHighWatermark() == types.GLSN(6+i)
					}), ShouldBeTrue)

					So(mr.numCommitSince(lsIds[0], preHighWatermark), ShouldEqual, 0)
					So(mr.numCommitSince(lsIds[1], preHighWatermark), ShouldEqual, 1)
				}
			})

			Convey("getLastCommitted should return same for sealed LS", func(ctx C) {
				rctx, cancel := context.WithTimeout(context.Background(), vtesting.TimeoutUnitTimesFactor(50))
				defer cancel()
				_, err := mr.Seal(rctx, lsIds[1])
				So(err, ShouldBeNil)

				for i := 0; i < 10; i++ {
					preHighWatermark := mr.storage.GetHighWatermark()

					So(testutil.CompareWaitN(10, func() bool {
						report := makeUncommitReport(snIDs[0][0], types.InvalidGLSN, lsIds[0], types.MinLLSN, uint64(3+i))
						return mr.proposeReport(report.StorageNodeID, report.UncommitReport) == nil
					}), ShouldBeTrue)

					So(testutil.CompareWaitN(10, func() bool {
						report := makeUncommitReport(snIDs[0][1], types.InvalidGLSN, lsIds[0], types.MinLLSN, uint64(3+i))
						return mr.proposeReport(report.StorageNodeID, report.UncommitReport) == nil
					}), ShouldBeTrue)

					So(testutil.CompareWaitN(10, func() bool {
						report := makeUncommitReport(snIDs[1][0], types.InvalidGLSN, lsIds[1], types.MinLLSN, uint64(4+i))
						return mr.proposeReport(report.StorageNodeID, report.UncommitReport) == nil
					}), ShouldBeTrue)

					So(testutil.CompareWaitN(10, func() bool {
						report := makeUncommitReport(snIDs[1][1], types.InvalidGLSN, lsIds[1], types.MinLLSN, uint64(4+i))
						return mr.proposeReport(report.StorageNodeID, report.UncommitReport) == nil
					}), ShouldBeTrue)

					So(testutil.CompareWaitN(10, func() bool {
						return mr.storage.GetHighWatermark() == types.GLSN(6+i)
					}), ShouldBeTrue)

					So(mr.numCommitSince(lsIds[0], preHighWatermark), ShouldEqual, 1)
					So(mr.numCommitSince(lsIds[1], preHighWatermark), ShouldEqual, 0)
				}
			})
		})
	})
}

func TestMRSeal(t *testing.T) {
	Convey("seal", t, func(ctx C) {
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
				snIDs[i][j] = types.StorageNodeID(i*2 + j)

				sn := &varlogpb.StorageNodeDescriptor{
					StorageNodeID: snIDs[i][j],
				}

				err := mr.storage.registerStorageNode(sn)
				So(err, ShouldBeNil)
			}
		}

		lsIds := make([]types.LogStreamID, 2)
		for i := range lsIds {
			lsIds[i] = types.LogStreamID(i)
		}

		for i, lsId := range lsIds {
			ls := makeLogStream(lsId, snIDs[i])
			err := mr.storage.registerLogStream(ls)
			So(err, ShouldBeNil)
		}

		clus.Start()
		So(testutil.CompareWaitN(10, func() bool {
			return clus.healthCheckAll()
		}), ShouldBeTrue)

		Convey("Seal should commit and return last committed", func(ctx C) {
			So(testutil.CompareWaitN(10, func() bool {
				report := makeUncommitReport(snIDs[0][0], types.InvalidGLSN, lsIds[0], types.MinLLSN, 2)
				return mr.proposeReport(report.StorageNodeID, report.UncommitReport) == nil
			}), ShouldBeTrue)

			So(testutil.CompareWaitN(10, func() bool {
				report := makeUncommitReport(snIDs[0][1], types.InvalidGLSN, lsIds[0], types.MinLLSN, 2)
				return mr.proposeReport(report.StorageNodeID, report.UncommitReport) == nil
			}), ShouldBeTrue)

			So(testutil.CompareWaitN(10, func() bool {
				report := makeUncommitReport(snIDs[1][0], types.InvalidGLSN, lsIds[1], types.MinLLSN, 4)
				return mr.proposeReport(report.StorageNodeID, report.UncommitReport) == nil
			}), ShouldBeTrue)

			So(testutil.CompareWaitN(10, func() bool {
				report := makeUncommitReport(snIDs[1][1], types.InvalidGLSN, lsIds[1], types.MinLLSN, 3)
				return mr.proposeReport(report.StorageNodeID, report.UncommitReport) == nil
			}), ShouldBeTrue)

			rctx, cancel := context.WithTimeout(context.Background(), vtesting.TimeoutUnitTimesFactor(50))
			defer cancel()
			lc, err := mr.Seal(rctx, lsIds[1])
			So(err, ShouldBeNil)
			So(lc, ShouldEqual, mr.getLastCommitted(lsIds[1]))

			Convey("Seal should return same last committed", func(ctx C) {
				for i := 0; i < 10; i++ {
					rctx, cancel := context.WithTimeout(context.Background(), vtesting.TimeoutUnitTimesFactor(50))
					defer cancel()
					lc2, err := mr.Seal(rctx, lsIds[1])
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
		clus := newMetadataRepoCluster(1, rep, false)
		Reset(func() {
			clus.closeNoErrors(t)
		})

		mr := clus.nodes[0]

		snIDs := make([][]types.StorageNodeID, 2)
		for i := range snIDs {
			snIDs[i] = make([]types.StorageNodeID, rep)
			for j := range snIDs[i] {
				snIDs[i][j] = types.StorageNodeID(i*2 + j)

				sn := &varlogpb.StorageNodeDescriptor{
					StorageNodeID: snIDs[i][j],
				}

				err := mr.storage.registerStorageNode(sn)
				So(err, ShouldBeNil)
			}
		}

		lsIds := make([]types.LogStreamID, 2)
		for i := range lsIds {
			lsIds[i] = types.LogStreamID(i)
		}

		for i, lsId := range lsIds {
			ls := makeLogStream(lsId, snIDs[i])
			err := mr.storage.registerLogStream(ls)
			So(err, ShouldBeNil)
		}

		clus.Start()
		So(testutil.CompareWaitN(10, func() bool {
			return clus.healthCheckAll()
		}), ShouldBeTrue)

		So(testutil.CompareWaitN(10, func() bool {
			report := makeUncommitReport(snIDs[0][0], types.InvalidGLSN, lsIds[0], types.MinLLSN, 2)
			return mr.proposeReport(report.StorageNodeID, report.UncommitReport) == nil
		}), ShouldBeTrue)

		So(testutil.CompareWaitN(10, func() bool {
			report := makeUncommitReport(snIDs[0][1], types.InvalidGLSN, lsIds[0], types.MinLLSN, 2)
			return mr.proposeReport(report.StorageNodeID, report.UncommitReport) == nil
		}), ShouldBeTrue)

		So(testutil.CompareWaitN(10, func() bool {
			report := makeUncommitReport(snIDs[1][0], types.InvalidGLSN, lsIds[1], types.MinLLSN, 4)
			return mr.proposeReport(report.StorageNodeID, report.UncommitReport) == nil
		}), ShouldBeTrue)

		So(testutil.CompareWaitN(10, func() bool {
			report := makeUncommitReport(snIDs[1][1], types.InvalidGLSN, lsIds[1], types.MinLLSN, 3)
			return mr.proposeReport(report.StorageNodeID, report.UncommitReport) == nil
		}), ShouldBeTrue)

		rctx, cancel := context.WithTimeout(context.Background(), vtesting.TimeoutUnitTimesFactor(50))
		defer cancel()
		lc, err := mr.Seal(rctx, lsIds[1])
		So(err, ShouldBeNil)
		So(lc, ShouldEqual, mr.getLastCommitted(lsIds[1]))

		Convey("Unealed LS should update report", func(ctx C) {
			rctx, cancel := context.WithTimeout(context.Background(), vtesting.TimeoutUnitTimesFactor(50))
			defer cancel()
			err := mr.Unseal(rctx, lsIds[1])
			So(err, ShouldBeNil)

			for i := 0; i < 10; i++ {
				preHighWatermark := mr.storage.GetHighWatermark()

				So(testutil.CompareWaitN(10, func() bool {
					report := makeUncommitReport(snIDs[1][0], types.InvalidGLSN, lsIds[1], types.MinLLSN, uint64(4+i))
					return mr.proposeReport(report.StorageNodeID, report.UncommitReport) == nil
				}), ShouldBeTrue)

				So(testutil.CompareWaitN(10, func() bool {
					report := makeUncommitReport(snIDs[1][1], types.InvalidGLSN, lsIds[1], types.MinLLSN, uint64(4+i))
					return mr.proposeReport(report.StorageNodeID, report.UncommitReport) == nil
				}), ShouldBeTrue)

				So(testutil.CompareWaitN(50, func() bool {
					return mr.storage.GetHighWatermark() == types.GLSN(6+i)
				}), ShouldBeTrue)

				So(mr.numCommitSince(lsIds[1], preHighWatermark), ShouldEqual, 1)
			}
		})
	})
}

func TestMRUpdateLogStream(t *testing.T) {
	Convey("Given MR cluster", t, func(ctx C) {
		nrStorageNode := 2
		rep := 1
		clus := newMetadataRepoCluster(1, rep, false)
		Reset(func() {
			clus.closeNoErrors(t)
		})

		clus.Start()
		So(testutil.CompareWaitN(10, func() bool {
			return clus.healthCheckAll()
		}), ShouldBeTrue)

		mr := clus.nodes[0]

		snIDs := make([]types.StorageNodeID, nrStorageNode)
		for i := range snIDs {
			snIDs[i] = types.StorageNodeID(i)

			sn := &varlogpb.StorageNodeDescriptor{
				StorageNodeID: snIDs[i],
			}

			err := mr.RegisterStorageNode(context.TODO(), sn)
			So(err, ShouldBeNil)
		}

		lsID := types.LogStreamID(0)
		ls := makeLogStream(lsID, snIDs[0:1])
		err := mr.RegisterLogStream(context.TODO(), ls)
		So(err, ShouldBeNil)
		So(testutil.CompareWaitN(1, func() bool {
			return mr.reportCollector.NumCommitter() == 1
		}), ShouldBeTrue)

		Convey("When Update LogStream", func() {
			rctx, cancel := context.WithTimeout(context.TODO(), vtesting.TimeoutUnitTimesFactor(50))
			defer cancel()
			_, err = mr.Seal(rctx, lsID)
			So(err, ShouldBeNil)

			updatedls := makeLogStream(lsID, snIDs[1:2])
			err = mr.UpdateLogStream(context.TODO(), updatedls)
			So(err, ShouldBeNil)

			Convey("Then unused report collector should be closed", func() {
				So(testutil.CompareWaitN(1, func() bool {
					return mr.reportCollector.NumCommitter() == 1
				}), ShouldBeTrue)

				err = mr.UnregisterStorageNode(context.TODO(), snIDs[0])
				So(err, ShouldBeNil)

				So(testutil.CompareWaitN(10, func() bool {
					reporterClient := clus.reporterClientFac.(*DummyReporterClientFactory).lookupClient(snIDs[0])
					return reporterClient == nil
				}), ShouldBeTrue)

				So(testutil.CompareWaitN(1, func() bool {
					return mr.reportCollector.NumCommitter() == 1
				}), ShouldBeTrue)

				reporterClient := clus.reporterClientFac.(*DummyReporterClientFactory).lookupClient(snIDs[1])
				So(reporterClient == nil, ShouldBeFalse)
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
		clus.Start()
		So(testutil.CompareWaitN(10, func() bool {
			return clus.healthCheckAll()
		}), ShouldBeTrue)

		snIDs := make([]types.StorageNodeID, nrRep)
		for i := range snIDs {
			snIDs[i] = types.StorageNodeID(i)

			sn := &varlogpb.StorageNodeDescriptor{
				StorageNodeID: snIDs[i],
			}

			rctx, cancel := context.WithTimeout(context.Background(), vtesting.TimeoutUnitTimesFactor(50))
			defer cancel()

			err := clus.nodes[0].RegisterStorageNode(rctx, sn)
			So(err, ShouldBeNil)
		}

		lsID := types.LogStreamID(0)

		ls := makeLogStream(lsID, snIDs)
		rctx, cancel := context.WithTimeout(context.Background(), vtesting.TimeoutUnitTimesFactor(50))
		defer cancel()
		err := clus.nodes[0].RegisterLogStream(rctx, ls)
		So(err, ShouldBeNil)

		reporterClient := clus.reporterClientFac.(*DummyReporterClientFactory).lookupClient(snIDs[0])
		So(testutil.CompareWaitN(50, func() bool {
			return !reporterClient.getKnownHighWatermark(0).Invalid()
		}), ShouldBeTrue)

		Convey("When node fail", func(ctx C) {
			leader := clus.leader()

			So(clus.leaderFail(), ShouldBeTrue)

			Convey("Then MR Cluster should elect", func(ctx C) {
				So(testutil.CompareWaitN(50, func() bool {
					return leader != clus.leader()
				}), ShouldBeTrue)

				prev := reporterClient.getKnownHighWatermark(0)

				So(testutil.CompareWaitN(50, func() bool {
					return reporterClient.getKnownHighWatermark(0) > prev
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

		clus.Start()
		So(testutil.CompareWaitN(10, func() bool {
			return clus.healthCheckAll()
		}), ShouldBeTrue)

		snIDs := make([]types.StorageNodeID, nrRep)
		for i := range snIDs {
			snIDs[i] = types.StorageNodeID(i)

			sn := &varlogpb.StorageNodeDescriptor{
				StorageNodeID: snIDs[i],
			}

			rctx, cancel := context.WithTimeout(context.Background(), vtesting.TimeoutUnitTimesFactor(200))
			defer cancel()

			err := clus.nodes[0].RegisterStorageNode(rctx, sn)
			So(err, ShouldBeNil)
		}

		lsID := types.LogStreamID(0)

		ls := makeLogStream(lsID, snIDs)
		rctx, cancel := context.WithTimeout(context.Background(), vtesting.TimeoutUnitTimesFactor(200))
		defer cancel()
		err := clus.nodes[0].RegisterLogStream(rctx, ls)
		So(err, ShouldBeNil)

		Convey("When new node join", func(ctx C) {
			newNode := nrNode
			nrNode += 1
			So(clus.appendMetadataRepo(), ShouldBeNil)
			So(clus.start(newNode), ShouldBeNil)

			rctx, cancel := context.WithTimeout(context.Background(), vtesting.TimeoutUnitTimesFactor(200))
			defer cancel()

			So(clus.nodes[0].AddPeer(rctx,
				types.ClusterID(0),
				clus.nodes[newNode].nodeID,
				clus.peers[newNode]), ShouldBeNil)

			Convey("Then new node should be member", func(ctx C) {
				So(testutil.CompareWaitN(200, func() bool {
					return clus.nodes[newNode].IsMember()
				}), ShouldBeTrue)

				Convey("Register to new node should be success", func(ctx C) {
					snID := snIDs[nrRep-1] + types.StorageNodeID(1)

					sn := &varlogpb.StorageNodeDescriptor{
						StorageNodeID: snID,
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
				cinfo, err := clus.nodes[newNode].GetClusterInfo(context.TODO(), types.ClusterID(0))
				So(err, ShouldResemble, verrors.ErrNotMember)

				cinfo, err = clus.nodes[0].GetClusterInfo(context.TODO(), types.ClusterID(0))
				So(err, ShouldBeNil)
				So(len(cinfo.Members), ShouldBeLessThan, nrNode)

				Convey("After join, it should have member info", func(ctx C) {
					rctx, cancel := context.WithTimeout(context.Background(), vtesting.TimeoutUnitTimesFactor(200))
					defer cancel()

					So(clus.nodes[0].AddPeer(rctx,
						types.ClusterID(0),
						clus.nodes[newNode].nodeID,
						clus.peers[newNode]), ShouldBeNil)

					So(testutil.CompareWaitN(200, func() bool {
						return clus.nodes[newNode].IsMember()
					}), ShouldBeTrue)

					Convey("Then proposal should be operated", func(ctx C) {
						snID := snIDs[nrRep-1] + types.StorageNodeID(1)

						sn := &varlogpb.StorageNodeDescriptor{
							StorageNodeID: snID,
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
		clus.Start()
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

			clus.stop(leaveNode)

			rctx, cancel := context.WithTimeout(context.Background(), vtesting.TimeoutUnitTimesFactor(50))
			defer cancel()

			So(clus.nodes[checkNode].RemovePeer(rctx,
				types.ClusterID(0),
				clus.nodes[leaveNode].nodeID), ShouldBeNil)

			Convey("Then GetMembership should return 2 peers", func(ctx C) {
				cinfo, err := clus.nodes[checkNode].GetClusterInfo(context.TODO(), 0)
				So(err, ShouldBeNil)
				So(len(cinfo.Members), ShouldEqual, nrNode-1)
			})
		})

		Convey("When leader leave", func(ctx C) {
			leaveNode := leader
			checkNode := (leaveNode + 1) % nrNode

			So(testutil.CompareWaitN(50, func() bool {
				cinfo, _ := clus.nodes[checkNode].GetClusterInfo(context.TODO(), 0)
				return len(cinfo.Members) == nrNode
			}), ShouldBeTrue)

			clus.stop(leaveNode)

			rctx, cancel := context.WithTimeout(context.Background(), vtesting.TimeoutUnitTimesFactor(50))
			defer cancel()

			So(clus.nodes[checkNode].RemovePeer(rctx,
				types.ClusterID(0),
				clus.nodes[leaveNode].nodeID), ShouldBeNil)

			Convey("Then GetMembership should return 2 peers", func(ctx C) {
				cinfo, err := clus.nodes[checkNode].GetClusterInfo(context.TODO(), 0)
				So(err, ShouldBeNil)
				So(len(cinfo.Members), ShouldEqual, nrNode-1)
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
		clus.Start()
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

			clus.restart(restartNode)

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

			clus.stop(restartNode)
			clus.stop(leaveNode)

			rctx, cancel := context.WithTimeout(context.Background(), vtesting.TimeoutUnitTimesFactor(50))
			defer cancel()

			So(clus.nodes[leader].RemovePeer(rctx,
				types.ClusterID(0),
				clus.nodes[leaveNode].nodeID), ShouldBeNil)

			nrNode -= 1

			clus.restart(restartNode)

			Convey("Then GetMembership should return 4 peers", func(ctx C) {
				So(testutil.CompareWaitN(100, func() bool {
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
		clus.Start()
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
				StorageNodeID: snIDs[i],
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
		clus.Start()
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
				StorageNodeID: snIDs[i],
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
		clus.Start()
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

			clus.stop(restartNode)
			clus.stop(leaveNode)

			rctx, cancel := context.WithTimeout(context.Background(), vtesting.TimeoutUnitTimesFactor(50))
			defer cancel()

			So(clus.nodes[leader].RemovePeer(rctx,
				types.ClusterID(0),
				clus.nodes[leaveNode].nodeID), ShouldBeNil)

			nrNode -= 1

			clus.restart(restartNode)

			Convey("Then GetMembership should return 4 peers", func(ctx C) {
				So(testutil.CompareWaitN(100, func() bool {
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
		clus.Start()
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

			clus.stop(restartNode)

			appliedIdx := clus.nodes[restartNode].raftNode.appliedIndex

			So(testutil.CompareWaitN(500, func() bool {
				snapshot := clus.getSnapshot(leader)
				return snapshot.Metadata.Index > appliedIdx+testSnapCount+1
			}), ShouldBeTrue)

			clus.restart(restartNode)

			Convey("Then node which is restarted should serve", func(ctx C) {
				So(testutil.CompareWaitN(100, func() bool {
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
		clus.Start()
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

			clus.stop(restartNode)

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

			clus.restart(restartNode)

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
		clus.Start()
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
				snIDs[i][j] = types.StorageNodeID(i*nrStorageNode + j)

				sn := &varlogpb.StorageNodeDescriptor{
					StorageNodeID: snIDs[i][j],
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

		for i := 0; i < nrLogStream; i++ {
			lsID := types.LogStreamID(i)
			ls := makeLogStream(lsID, snIDs[i%nrStorageNode])

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

			clus.restart(restartNode)

			So(testutil.CompareWaitN(50, func() bool {
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
				StorageNodeID: types.StorageNodeID(snID),
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
		clus.Start()
		Reset(func() {
			clus.closeNoErrors(t)
		})

		So(testutil.CompareWaitN(10, func() bool {
			return clus.healthCheckAll()
		}), ShouldBeTrue)

		Convey("When cli register SN & transfer leader for dropping propose", func(ctx C) {
			leader := clus.leader()
			clus.nodes[leader].raftNode.transferLeadership(false)

			snID := types.StorageNodeID(time.Now().UnixNano())
			sn := &varlogpb.StorageNodeDescriptor{
				StorageNodeID: types.StorageNodeID(snID),
			}

			rctx, cancel := context.WithTimeout(context.Background(), vtesting.TimeoutUnitTimesFactor(50))
			defer cancel()
			err := clus.nodes[leader].RegisterStorageNode(rctx, sn)

			Convey("Then it should be success", func(ctx C) {
				So(err, ShouldBeNil)
				//So(atomic.LoadUint64(&clus.nodes[leader].requestNum), ShouldBeGreaterThan, 1)
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
		clus.Start()
		So(testutil.CompareWaitN(10, func() bool {
			return clus.healthCheckAll()
		}), ShouldBeTrue)

		So(testutil.CompareWaitN(500, func() bool {
			peers := clus.getMembersFromSnapshot(0)
			return len(peers) == nrNode
		}), ShouldBeTrue)

		Convey("When new node join", func(ctx C) {
			for i := 0; i < 5; i++ {
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
