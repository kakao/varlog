package metadata_repository

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"sync"
	"testing"
	"time"

	types "github.daumkakao.com/varlog/varlog/pkg/varlog/types"
	"github.daumkakao.com/varlog/varlog/pkg/varlog/util/testutil"
	pb "github.daumkakao.com/varlog/varlog/proto/metadata_repository"
	snpb "github.daumkakao.com/varlog/varlog/proto/storage_node"
	varlogpb "github.daumkakao.com/varlog/varlog/proto/varlog"
	"go.uber.org/zap"

	. "github.com/smartystreets/goconvey/convey"
)

type metadataRepoCluster struct {
	nrRep             int
	peers             []string
	nodes             []*RaftMetadataRepository
	reporterClientFac ReporterClientFactory
	logger            *zap.Logger
}

func newMetadataRepoCluster(n, nrRep int, increseUncommit bool) *metadataRepoCluster {
	peers := make([]string, n)
	nodes := make([]*RaftMetadataRepository, n)

	for i := range peers {
		peers[i] = fmt.Sprintf("http://127.0.0.1:%d", 10000+i)
	}

	//logger, _ := zap.NewDevelopment()
	logger := zap.NewNop()
	clus := &metadataRepoCluster{
		nrRep:             nrRep,
		peers:             peers,
		nodes:             nodes,
		reporterClientFac: NewDummyReporterClientFactory(!increseUncommit),
		logger:            logger,
	}

	for i := range clus.peers {
		clus.createMetadataRepo(i, false)
	}

	return clus
}

func (clus *metadataRepoCluster) createMetadataRepo(idx int, join bool) error {
	if idx < 0 || idx > len(clus.nodes) {
		return errors.New("out or range")
	}

	url, _ := url.Parse(clus.peers[idx])
	nodeID := types.NewNodeID(url.Host)

	os.RemoveAll(fmt.Sprintf("raft-%d", nodeID))
	os.RemoveAll(fmt.Sprintf("raft-%d-snap", nodeID))

	config := &Config{
		Index:             nodeID,
		Join:              join,
		NumRep:            clus.nrRep,
		PeerList:          clus.peers,
		ReporterClientFac: clus.reporterClientFac,
		Logger:            clus.logger,
	}

	clus.nodes[idx] = NewRaftMetadataRepository(config)
	return nil
}

func (clus *metadataRepoCluster) appendMetadataRepo() error {
	idx := len(clus.nodes)
	clus.peers = append(clus.peers, fmt.Sprintf("http://127.0.0.1:%d", 10000+idx))
	clus.nodes = append(clus.nodes, nil)

	return clus.createMetadataRepo(idx, true)
}

func (clus *metadataRepoCluster) start(idx int) error {
	if idx < 0 || idx > len(clus.nodes) {
		return errors.New("out or range")
	}

	clus.nodes[idx].Run()

	return nil
}

func (clus *metadataRepoCluster) Start() {
	for i := range clus.nodes {
		clus.start(i)
	}
}

func (clus *metadataRepoCluster) stop(idx int) error {
	if idx < 0 || idx > len(clus.nodes) {
		return errors.New("out or range")
	}

	return clus.nodes[idx].Close()
}

func (clus *metadataRepoCluster) close(idx int) error {
	if idx < 0 || idx > len(clus.nodes) {
		return errors.New("out or range")
	}

	err := clus.stop(idx)

	nodeID := clus.nodes[idx].index
	os.RemoveAll(fmt.Sprintf("raft-%d", nodeID))
	os.RemoveAll(fmt.Sprintf("raft-%d-snap", nodeID))

	return err
}

// Close closes all cluster nodes
func (clus *metadataRepoCluster) Close() error {
	var err error
	for i := range clus.peers {
		if erri := clus.close(i); erri != nil {
			err = erri
		}
	}
	return err
}

func (clus *metadataRepoCluster) leader() int {
	leader := -1
	for i, n := range clus.nodes {
		if n.isLeader() {
			leader = i
		}
	}

	return leader
}

func (clus *metadataRepoCluster) leaderElected() bool {
	return clus.leader() >= 0
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
	if err := clus.Close(); err != nil {
		t.Log(err)
	}
}

func makeLocalLogStream(snID types.StorageNodeID, knownHighWatermark types.GLSN, lsID types.LogStreamID, offset types.LLSN, length uint64) *snpb.LocalLogStreamDescriptor {
	lls := &snpb.LocalLogStreamDescriptor{
		StorageNodeID: snID,
		HighWatermark: knownHighWatermark,
	}
	ls := &snpb.LocalLogStreamDescriptor_LogStreamUncommitReport{
		LogStreamID:           lsID,
		UncommittedLLSNOffset: offset,
		UncommittedLLSNLength: length,
	}
	lls.Uncommit = append(lls.Uncommit, ls)

	return lls
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

		snIds := make([]types.StorageNodeID, rep)
		for i := range snIds {
			snIds[i] = types.StorageNodeID(i)

			sn := &varlogpb.StorageNodeDescriptor{
				StorageNodeID: snIds[i],
			}

			err := mr.storage.registerStorageNode(sn)
			So(err, ShouldBeNil)
		}
		lsId := types.LogStreamID(0)
		notExistSnID := types.StorageNodeID(rep)

		lls := makeLocalLogStream(snIds[0], types.InvalidGLSN, lsId, types.MinLLSN, 2)
		mr.applyReport(&pb.Report{LogStream: lls})

		for _, snId := range snIds {
			r := mr.storage.LookupLocalLogStreamReplica(lsId, snId)
			So(r, ShouldBeNil)
		}

		Convey("LocalLogStream should register when register LogStream", func(ctx C) {
			ls := makeLogStream(lsId, snIds)
			err := mr.storage.registerLogStream(ls)
			So(err, ShouldBeNil)

			for _, snId := range snIds {
				r := mr.storage.LookupLocalLogStreamReplica(lsId, snId)
				So(r, ShouldNotBeNil)
			}

			Convey("Report should not apply if snID is not exist in LocalLogStream", func(ctx C) {
				lls := makeLocalLogStream(notExistSnID, types.InvalidGLSN, lsId, types.MinLLSN, 2)
				mr.applyReport(&pb.Report{LogStream: lls})

				r := mr.storage.LookupLocalLogStreamReplica(lsId, notExistSnID)
				So(r, ShouldBeNil)
			})

			Convey("Report should apply if snID is exist in LocalLogStream", func(ctx C) {
				snId := snIds[0]
				lls := makeLocalLogStream(snId, types.InvalidGLSN, lsId, types.MinLLSN, 2)
				mr.applyReport(&pb.Report{LogStream: lls})

				r := mr.storage.LookupLocalLogStreamReplica(lsId, snId)
				So(r, ShouldNotBeNil)
				So(r.UncommittedLLSNEnd(), ShouldEqual, types.MinLLSN+types.LLSN(2))

				Convey("Report which have bigger END LLSN Should be applied", func(ctx C) {
					lls := makeLocalLogStream(snId, types.InvalidGLSN, lsId, types.MinLLSN, 3)
					mr.applyReport(&pb.Report{LogStream: lls})

					r := mr.storage.LookupLocalLogStreamReplica(lsId, snId)
					So(r, ShouldNotBeNil)
					So(r.UncommittedLLSNEnd(), ShouldEqual, types.MinLLSN+types.LLSN(3))
				})

				Convey("Report which have smaller END LLSN Should Not be applied", func(ctx C) {
					lls := makeLocalLogStream(snId, types.InvalidGLSN, lsId, types.MinLLSN, 1)
					mr.applyReport(&pb.Report{LogStream: lls})

					r := mr.storage.LookupLocalLogStreamReplica(lsId, snId)
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

		snIds := make([]types.StorageNodeID, 2)
		for i := range snIds {
			snIds[i] = types.StorageNodeID(i)
			sn := &varlogpb.StorageNodeDescriptor{
				StorageNodeID: snIds[i],
			}

			err := mr.storage.registerStorageNode(sn)
			So(err, ShouldBeNil)
		}
		lsId := types.LogStreamID(0)
		ls := makeLogStream(lsId, snIds)
		err := mr.storage.registerLogStream(ls)
		So(err, ShouldBeNil)

		Convey("LogStream which all reports have not arrived cannot be commit", func(ctx C) {
			lls := makeLocalLogStream(snIds[0], types.InvalidGLSN, lsId, types.MinLLSN, 2)
			mr.applyReport(&pb.Report{LogStream: lls})

			replicas := mr.storage.LookupLocalLogStream(lsId)
			_, minHWM, nrCommit := mr.calculateCommit(replicas)
			So(nrCommit, ShouldEqual, 0)
			So(minHWM, ShouldEqual, types.InvalidGLSN)
		})

		Convey("LogStream which all reports are disjoint cannot be commit", func(ctx C) {
			lls := makeLocalLogStream(snIds[0], types.GLSN(10), lsId, types.MinLLSN+types.LLSN(5), 1)
			mr.applyReport(&pb.Report{LogStream: lls})

			lls = makeLocalLogStream(snIds[1], types.GLSN(7), lsId, types.MinLLSN+types.LLSN(3), 2)
			mr.applyReport(&pb.Report{LogStream: lls})

			replicas := mr.storage.LookupLocalLogStream(lsId)
			knownHWM, minHWM, nrCommit := mr.calculateCommit(replicas)
			So(nrCommit, ShouldEqual, 0)
			So(knownHWM, ShouldEqual, types.GLSN(10))
			So(minHWM, ShouldEqual, types.GLSN(7))
		})

		Convey("LogStream Should be commit where replication is completed", func(ctx C) {
			lls := makeLocalLogStream(snIds[0], types.GLSN(10), lsId, types.MinLLSN+types.LLSN(3), 3)
			mr.applyReport(&pb.Report{LogStream: lls})

			lls = makeLocalLogStream(snIds[1], types.GLSN(9), lsId, types.MinLLSN+types.LLSN(3), 2)
			mr.applyReport(&pb.Report{LogStream: lls})

			replicas := mr.storage.LookupLocalLogStream(lsId)
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

		clus.Start()
		So(testutil.CompareWait(func() bool {
			return clus.leaderElected()
		}, time.Second), ShouldBeTrue)

		mr := clus.nodes[0]

		snIds := make([][]types.StorageNodeID, 2)
		for i := range snIds {
			snIds[i] = make([]types.StorageNodeID, rep)
			for j := range snIds[i] {
				snIds[i][j] = types.StorageNodeID(i*2 + j)

				sn := &varlogpb.StorageNodeDescriptor{
					StorageNodeID: snIds[i][j],
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
			ls := makeLogStream(lsId, snIds[i])
			err := mr.storage.registerLogStream(ls)
			So(err, ShouldBeNil)
		}

		Convey("global commit", func(ctx C) {
			So(testutil.CompareWait(func() bool {
				lls := makeLocalLogStream(snIds[0][0], types.InvalidGLSN, lsIds[0], types.MinLLSN, 2)
				return mr.proposeReport(lls) == nil
			}, time.Second), ShouldBeTrue)

			So(testutil.CompareWait(func() bool {
				lls := makeLocalLogStream(snIds[0][1], types.InvalidGLSN, lsIds[0], types.MinLLSN, 2)
				return mr.proposeReport(lls) == nil
			}, time.Second), ShouldBeTrue)

			So(testutil.CompareWait(func() bool {
				lls := makeLocalLogStream(snIds[1][0], types.InvalidGLSN, lsIds[1], types.MinLLSN, 4)
				return mr.proposeReport(lls) == nil
			}, time.Second), ShouldBeTrue)

			So(testutil.CompareWait(func() bool {
				lls := makeLocalLogStream(snIds[1][1], types.InvalidGLSN, lsIds[1], types.MinLLSN, 3)
				return mr.proposeReport(lls) == nil
			}, time.Second), ShouldBeTrue)

			// global commit (2, 3) highest glsn: 5
			So(testutil.CompareWait(func() bool {
				return mr.storage.GetHighWatermark() == types.GLSN(5)
			}, time.Second), ShouldBeTrue)

			Convey("LogStream should be dedup", func(ctx C) {
				So(testutil.CompareWait(func() bool {
					lls := makeLocalLogStream(snIds[0][0], types.InvalidGLSN, lsIds[0], types.MinLLSN, 3)
					return mr.proposeReport(lls) == nil
				}, time.Second), ShouldBeTrue)

				So(testutil.CompareWait(func() bool {
					lls := makeLocalLogStream(snIds[0][1], types.InvalidGLSN, lsIds[0], types.MinLLSN, 2)
					return mr.proposeReport(lls) == nil
				}, time.Second), ShouldBeTrue)

				time.Sleep(100 * time.Millisecond)

				So(testutil.CompareWait(func() bool {
					return mr.storage.GetHighWatermark() == types.GLSN(5)
				}, time.Second), ShouldBeTrue)
			})

			Convey("LogStream which have wrong GLSN but have uncommitted should commit", func(ctx C) {
				So(testutil.CompareWait(func() bool {
					lls := makeLocalLogStream(snIds[0][0], types.InvalidGLSN, lsIds[0], types.MinLLSN, 6)
					return mr.proposeReport(lls) == nil
				}, time.Second), ShouldBeTrue)

				So(testutil.CompareWait(func() bool {
					lls := makeLocalLogStream(snIds[0][1], types.InvalidGLSN, lsIds[0], types.MinLLSN, 6)
					return mr.proposeReport(lls) == nil
				}, time.Second), ShouldBeTrue)

				So(testutil.CompareWait(func() bool {
					return mr.storage.GetHighWatermark() == types.GLSN(9)
				}, time.Second), ShouldBeTrue)
			})
		})
	})
}

func TestMRSimpleReportNCommit(t *testing.T) {
	Convey("Uncommitted LocalLogStream should be committed", t, func(ctx C) {
		clus := newMetadataRepoCluster(1, 1, false)
		Reset(func() {
			clus.closeNoErrors(t)
		})
		clus.Start()
		So(testutil.CompareWait(func() bool {
			return clus.leaderElected()
		}, time.Second), ShouldBeTrue)

		snID := types.StorageNodeID(0)
		snIDs := make([]types.StorageNodeID, 1)
		snIDs = append(snIDs, snID)

		lsID := types.LogStreamID(snID)

		sn := &varlogpb.StorageNodeDescriptor{
			StorageNodeID: snID,
		}

		err := clus.nodes[0].RegisterStorageNode(context.TODO(), sn)
		So(err, ShouldBeNil)

		So(testutil.CompareWait(func() bool {
			return clus.reporterClientFac.(*DummyReporterClientFactory).lookupClient(snID) != nil
		}, time.Second), ShouldBeTrue)

		ls := makeLogStream(lsID, snIDs)
		err = clus.nodes[0].RegisterLogStream(context.TODO(), ls)
		So(err, ShouldBeNil)

		reporterClient := clus.reporterClientFac.(*DummyReporterClientFactory).lookupClient(snID)
		reporterClient.increaseUncommitted()

		So(testutil.CompareWait(func() bool {
			return reporterClient.numUncommitted() == 0
		}, time.Second), ShouldBeTrue)
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

		T := 100 * time.Millisecond

		var wg sync.WaitGroup
		var st sync.WaitGroup

		st.Add(1)
		wg.Add(1)
		go func() {
			defer wg.Done()
			rctx, _ := context.WithTimeout(context.Background(), T)
			st.Done()
			mr.RegisterStorageNode(rctx, sn)
		}()

		st.Wait()
		So(testutil.CompareWait(func() bool {
			_, ok := mr.requestMap.Load(uint64(1))
			return ok
		}, T), ShouldBeTrue)

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

			testutil.CompareWait(func() bool {
				_, ok := mr.requestMap.Load(uint64(1))
				return ok
			}, time.Second)

			dummy := &pb.RaftEntry{
				NodeIndex:    2,
				RequestIndex: uint64(1),
			}
			mr.commitC <- dummy
		}()

		st.Wait()
		rctx, _ := context.WithTimeout(context.Background(), 200*time.Millisecond)
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

		rctx, _ := context.WithTimeout(context.Background(), 100*time.Millisecond)
		err := mr.RegisterStorageNode(rctx, sn)
		So(err, ShouldNotBeNil)

		_, ok := mr.requestMap.Load(uint64(1))
		So(ok, ShouldBeFalse)
	})

	Convey("requestMap should delete after ack", t, func(ctx C) {
		clus := newMetadataRepoCluster(1, 1, false)
		Reset(func() {
			clus.closeNoErrors(t)
		})
		clus.Start()
		So(testutil.CompareWait(func() bool {
			return clus.leaderElected()
		}, time.Second), ShouldBeTrue)

		mr := clus.nodes[0]

		sn := &varlogpb.StorageNodeDescriptor{
			StorageNodeID: types.StorageNodeID(0),
		}

		err := mr.RegisterStorageNode(context.TODO(), sn)
		So(err, ShouldBeNil)

		_, ok := mr.requestMap.Load(uint64(1))
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
		clus.Start()
		So(testutil.CompareWait(func() bool {
			return clus.leaderElected()
		}, time.Second), ShouldBeTrue)

		mr := clus.nodes[0]

		snIds := make([][]types.StorageNodeID, 2)
		for i := range snIds {
			snIds[i] = make([]types.StorageNodeID, rep)
			for j := range snIds[i] {
				snIds[i][j] = types.StorageNodeID(i*2 + j)

				sn := &varlogpb.StorageNodeDescriptor{
					StorageNodeID: snIds[i][j],
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
			ls := makeLogStream(lsId, snIds[i])
			err := mr.storage.registerLogStream(ls)
			So(err, ShouldBeNil)
		}

		Convey("getLastCommitted should return last committed GLSN", func(ctx C) {
			So(testutil.CompareWait(func() bool {
				lls := makeLocalLogStream(snIds[0][0], types.InvalidGLSN, lsIds[0], types.MinLLSN, 2)
				return mr.proposeReport(lls) == nil
			}, time.Second), ShouldBeTrue)

			So(testutil.CompareWait(func() bool {
				lls := makeLocalLogStream(snIds[0][1], types.InvalidGLSN, lsIds[0], types.MinLLSN, 2)
				return mr.proposeReport(lls) == nil
			}, time.Second), ShouldBeTrue)

			So(testutil.CompareWait(func() bool {
				lls := makeLocalLogStream(snIds[1][0], types.InvalidGLSN, lsIds[1], types.MinLLSN, 4)
				return mr.proposeReport(lls) == nil
			}, time.Second), ShouldBeTrue)

			So(testutil.CompareWait(func() bool {
				lls := makeLocalLogStream(snIds[1][1], types.InvalidGLSN, lsIds[1], types.MinLLSN, 3)
				return mr.proposeReport(lls) == nil
			}, time.Second), ShouldBeTrue)

			// global commit (2, 3) highest glsn: 5
			So(testutil.CompareWait(func() bool {
				return mr.storage.GetHighWatermark() == types.GLSN(5)
			}, time.Second), ShouldBeTrue)

			So(mr.getLastCommitted(lsIds[0]), ShouldEqual, types.GLSN(2))
			So(mr.getLastCommitted(lsIds[1]), ShouldEqual, types.GLSN(5))

			Convey("getLastCommitted should return same if not committed", func(ctx C) {
				for i := 0; i < 10; i++ {
					So(testutil.CompareWait(func() bool {
						lls := makeLocalLogStream(snIds[1][0], types.InvalidGLSN, lsIds[1], types.MinLLSN, uint64(4+i))
						return mr.proposeReport(lls) == nil
					}, time.Second), ShouldBeTrue)

					So(testutil.CompareWait(func() bool {
						lls := makeLocalLogStream(snIds[1][1], types.InvalidGLSN, lsIds[1], types.MinLLSN, uint64(4+i))
						return mr.proposeReport(lls) == nil
					}, time.Second), ShouldBeTrue)

					So(testutil.CompareWait(func() bool {
						return mr.storage.GetHighWatermark() == types.GLSN(6+i)
					}, time.Second), ShouldBeTrue)

					So(mr.getLastCommitted(lsIds[0]), ShouldEqual, types.GLSN(2))
					So(mr.getLastCommitted(lsIds[1]), ShouldEqual, types.GLSN(6+i))
				}
			})

			Convey("getLastCommitted should return same for sealed LS", func(ctx C) {
				rctx, _ := context.WithTimeout(context.Background(), time.Second)
				_, err := mr.Seal(rctx, lsIds[1])
				So(err, ShouldBeNil)

				for i := 0; i < 10; i++ {
					So(testutil.CompareWait(func() bool {
						lls := makeLocalLogStream(snIds[0][0], types.InvalidGLSN, lsIds[0], types.MinLLSN, uint64(3+i))
						return mr.proposeReport(lls) == nil
					}, time.Second), ShouldBeTrue)

					So(testutil.CompareWait(func() bool {
						lls := makeLocalLogStream(snIds[0][1], types.InvalidGLSN, lsIds[0], types.MinLLSN, uint64(3+i))
						return mr.proposeReport(lls) == nil
					}, time.Second), ShouldBeTrue)

					So(testutil.CompareWait(func() bool {
						lls := makeLocalLogStream(snIds[1][0], types.InvalidGLSN, lsIds[1], types.MinLLSN, uint64(4+i))
						return mr.proposeReport(lls) == nil
					}, time.Second), ShouldBeTrue)

					So(testutil.CompareWait(func() bool {
						lls := makeLocalLogStream(snIds[1][1], types.InvalidGLSN, lsIds[1], types.MinLLSN, uint64(4+i))
						return mr.proposeReport(lls) == nil
					}, time.Second), ShouldBeTrue)

					So(testutil.CompareWait(func() bool {
						return mr.storage.GetHighWatermark() == types.GLSN(6+i)
					}, time.Second), ShouldBeTrue)

					So(mr.getLastCommitted(lsIds[0]), ShouldEqual, types.GLSN(6+i))
					So(mr.getLastCommitted(lsIds[1]), ShouldEqual, types.GLSN(5))
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
		clus.Start()
		So(testutil.CompareWait(func() bool {
			return clus.leaderElected()
		}, time.Second), ShouldBeTrue)

		mr := clus.nodes[0]

		snIds := make([][]types.StorageNodeID, 2)
		for i := range snIds {
			snIds[i] = make([]types.StorageNodeID, rep)
			for j := range snIds[i] {
				snIds[i][j] = types.StorageNodeID(i*2 + j)

				sn := &varlogpb.StorageNodeDescriptor{
					StorageNodeID: snIds[i][j],
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
			ls := makeLogStream(lsId, snIds[i])
			err := mr.storage.registerLogStream(ls)
			So(err, ShouldBeNil)
		}

		Convey("Seal should commit and return last committed", func(ctx C) {
			So(testutil.CompareWait(func() bool {
				lls := makeLocalLogStream(snIds[0][0], types.InvalidGLSN, lsIds[0], types.MinLLSN, 2)
				return mr.proposeReport(lls) == nil
			}, time.Second), ShouldBeTrue)

			So(testutil.CompareWait(func() bool {
				lls := makeLocalLogStream(snIds[0][1], types.InvalidGLSN, lsIds[0], types.MinLLSN, 2)
				return mr.proposeReport(lls) == nil
			}, time.Second), ShouldBeTrue)

			So(testutil.CompareWait(func() bool {
				lls := makeLocalLogStream(snIds[1][0], types.InvalidGLSN, lsIds[1], types.MinLLSN, 4)
				return mr.proposeReport(lls) == nil
			}, time.Second), ShouldBeTrue)

			So(testutil.CompareWait(func() bool {
				lls := makeLocalLogStream(snIds[1][1], types.InvalidGLSN, lsIds[1], types.MinLLSN, 3)
				return mr.proposeReport(lls) == nil
			}, time.Second), ShouldBeTrue)

			rctx, _ := context.WithTimeout(context.Background(), time.Second)
			lc, err := mr.Seal(rctx, lsIds[1])
			So(err, ShouldBeNil)
			So(lc, ShouldEqual, types.GLSN(5))

			Convey("Seal should return same last committed", func(ctx C) {
				for i := 0; i < 10; i++ {
					rctx, _ := context.WithTimeout(context.Background(), time.Second)
					lc, err := mr.Seal(rctx, lsIds[1])
					So(err, ShouldBeNil)
					So(lc, ShouldEqual, types.GLSN(5))
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
		clus.Start()
		So(testutil.CompareWait(func() bool {
			return clus.leaderElected()
		}, time.Second), ShouldBeTrue)

		mr := clus.nodes[0]

		snIds := make([][]types.StorageNodeID, 2)
		for i := range snIds {
			snIds[i] = make([]types.StorageNodeID, rep)
			for j := range snIds[i] {
				snIds[i][j] = types.StorageNodeID(i*2 + j)

				sn := &varlogpb.StorageNodeDescriptor{
					StorageNodeID: snIds[i][j],
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
			ls := makeLogStream(lsId, snIds[i])
			err := mr.storage.registerLogStream(ls)
			So(err, ShouldBeNil)
		}

		So(testutil.CompareWait(func() bool {
			lls := makeLocalLogStream(snIds[0][0], types.InvalidGLSN, lsIds[0], types.MinLLSN, 2)
			return mr.proposeReport(lls) == nil
		}, time.Second), ShouldBeTrue)

		So(testutil.CompareWait(func() bool {
			lls := makeLocalLogStream(snIds[0][1], types.InvalidGLSN, lsIds[0], types.MinLLSN, 2)
			return mr.proposeReport(lls) == nil
		}, time.Second), ShouldBeTrue)

		So(testutil.CompareWait(func() bool {
			lls := makeLocalLogStream(snIds[1][0], types.InvalidGLSN, lsIds[1], types.MinLLSN, 4)
			return mr.proposeReport(lls) == nil
		}, time.Second), ShouldBeTrue)

		So(testutil.CompareWait(func() bool {
			lls := makeLocalLogStream(snIds[1][1], types.InvalidGLSN, lsIds[1], types.MinLLSN, 3)
			return mr.proposeReport(lls) == nil
		}, time.Second), ShouldBeTrue)

		rctx, _ := context.WithTimeout(context.Background(), time.Second)
		lc, err := mr.Seal(rctx, lsIds[1])
		So(err, ShouldBeNil)
		So(lc, ShouldEqual, types.GLSN(5))

		Convey("Unealed LS should update report", func(ctx C) {
			rctx, _ := context.WithTimeout(context.Background(), time.Second)
			err := mr.Unseal(rctx, lsIds[1])
			So(err, ShouldBeNil)

			for i := 0; i < 10; i++ {
				So(testutil.CompareWait(func() bool {
					lls := makeLocalLogStream(snIds[1][0], types.InvalidGLSN, lsIds[1], types.MinLLSN, uint64(4+i))
					return mr.proposeReport(lls) == nil
				}, time.Second), ShouldBeTrue)

				So(testutil.CompareWait(func() bool {
					lls := makeLocalLogStream(snIds[1][1], types.InvalidGLSN, lsIds[1], types.MinLLSN, uint64(4+i))
					return mr.proposeReport(lls) == nil
				}, time.Second), ShouldBeTrue)

				So(testutil.CompareWait(func() bool {
					return mr.storage.GetHighWatermark() == types.GLSN(6+i)
				}, time.Second), ShouldBeTrue)

				So(mr.getLastCommitted(lsIds[1]), ShouldEqual, types.GLSN(6+i))
			}
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
		So(testutil.CompareWait(func() bool {
			return clus.leaderElected()
		}, time.Second), ShouldBeTrue)

		snIDs := make([]types.StorageNodeID, nrRep)
		for i := range snIDs {
			snIDs[i] = types.StorageNodeID(i)

			sn := &varlogpb.StorageNodeDescriptor{
				StorageNodeID: snIDs[i],
			}

			err := clus.nodes[0].RegisterStorageNode(context.TODO(), sn)
			So(err, ShouldBeNil)
		}

		lsID := types.LogStreamID(0)

		ls := makeLogStream(lsID, snIDs)
		err := clus.nodes[0].RegisterLogStream(context.TODO(), ls)
		So(err, ShouldBeNil)

		reporterClient := clus.reporterClientFac.(*DummyReporterClientFactory).lookupClient(snIDs[0])
		So(testutil.CompareWait(func() bool {
			return !reporterClient.getKnownHighWatermark().Invalid()
		}, time.Second), ShouldBeTrue)

		Convey("When node fail", func(ctx C) {
			So(clus.leaderFail(), ShouldBeTrue)

			Convey("Then MR Cluster should elect", func(ctx C) {
				So(testutil.CompareWait(func() bool {
					return clus.leaderElected()
				}, time.Second), ShouldBeTrue)

				prev := reporterClient.getKnownHighWatermark()

				So(testutil.CompareWait(func() bool {
					return reporterClient.getKnownHighWatermark() > prev
				}, time.Second), ShouldBeTrue)
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
		So(testutil.CompareWait(func() bool {
			return clus.leaderElected()
		}, time.Second), ShouldBeTrue)

		snIDs := make([]types.StorageNodeID, nrRep)
		for i := range snIDs {
			snIDs[i] = types.StorageNodeID(i)

			sn := &varlogpb.StorageNodeDescriptor{
				StorageNodeID: snIDs[i],
			}

			err := clus.nodes[0].RegisterStorageNode(context.TODO(), sn)
			So(err, ShouldBeNil)
		}

		lsID := types.LogStreamID(0)

		ls := makeLogStream(lsID, snIDs)
		err := clus.nodes[0].RegisterLogStream(context.TODO(), ls)
		So(err, ShouldBeNil)

		Convey("When new node join", func(ctx C) {
			newNode := nrNode
			So(clus.appendMetadataRepo(), ShouldBeNil)

			rctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()

			So(clus.nodes[0].AddPeer(rctx,
				types.InvalidNodeID,
				clus.nodes[newNode].index,
				clus.peers[newNode]), ShouldBeNil)
			So(clus.start(newNode), ShouldBeNil)
			nrNode += 1

			Convey("Then getMeta from new node should be success", func(ctx C) {
				So(testutil.CompareWait(func() bool {
					meta, err := clus.nodes[newNode].GetMetadata(context.TODO())
					if err != nil {
						return false
					}

					return meta.GetStorageNode(snIDs[0]) != nil
				}, time.Second), ShouldBeTrue)

				Convey("Register to new node should be success", func(ctx C) {
					snID := snIDs[nrRep-1] + types.StorageNodeID(1)

					sn := &varlogpb.StorageNodeDescriptor{
						StorageNodeID: snID,
					}

					err := clus.nodes[newNode].RegisterStorageNode(context.TODO(), sn)
					So(err, ShouldBeNil)

					meta, err := clus.nodes[newNode].GetMetadata(context.TODO())
					So(err, ShouldBeNil)
					So(meta.GetStorageNode(snID), ShouldNotBeNil)
				})
			})
		})

		Convey("When new nodes joining", func(ctx C) {
			newNode := nrNode
			So(clus.appendMetadataRepo(), ShouldBeNil)

			rctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()

			So(clus.nodes[0].AddPeer(rctx,
				types.InvalidNodeID,
				clus.nodes[newNode].index,
				clus.peers[newNode]), ShouldBeNil)
			So(clus.start(newNode), ShouldBeNil)
			nrNode += 1

			Convey("Then proposal should be operated", func(ctx C) {
				snID := snIDs[nrRep-1] + types.StorageNodeID(1)

				sn := &varlogpb.StorageNodeDescriptor{
					StorageNodeID: snID,
				}

				err := clus.nodes[newNode].RegisterStorageNode(context.TODO(), sn)
				So(err, ShouldBeNil)
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
		So(testutil.CompareWait(func() bool {
			return clus.leaderElected()
		}, time.Second), ShouldBeTrue)

		leader := clus.leader()

		Convey("When follower leave", func(ctx C) {
			leaveNode := (leader + 1) % nrNode
			checkNode := (leaveNode + 1) % nrNode
			clus.stop(leaveNode)

			rctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()

			So(clus.nodes[checkNode].RemovePeer(rctx,
				types.InvalidNodeID,
				clus.nodes[leaveNode].index), ShouldBeNil)

			Convey("Then GetMembership should return 2 peers", func(ctx C) {
				_, membership, err := clus.nodes[checkNode].GetClusterInfo(context.TODO(), 0)
				So(err, ShouldBeNil)
				So(len(membership), ShouldEqual, nrNode-1)
			})
		})

		Convey("When leader leave", func(ctx C) {
			leaveNode := leader
			checkNode := (leaveNode + 1) % nrNode
			clus.stop(leaveNode)

			rctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()

			So(clus.nodes[checkNode].RemovePeer(rctx,
				types.InvalidNodeID,
				clus.nodes[leaveNode].index), ShouldBeNil)

			Convey("Then GetMembership should return 2 peers", func(ctx C) {
				_, membership, err := clus.nodes[checkNode].GetClusterInfo(context.TODO(), 0)
				So(err, ShouldBeNil)
				So(len(membership), ShouldEqual, nrNode-1)
			})
		})
	})
}
