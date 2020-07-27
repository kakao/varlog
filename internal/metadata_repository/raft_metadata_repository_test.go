package metadata_repository

import (
	"context"
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
	peers             []string
	nodes             []*RaftMetadataRepository
	reporterClientFac ReporterClientFactory
	logger            *zap.Logger
}

func newMetadataRepoCluster(n, nrRep int) *metadataRepoCluster {
	peers := make([]string, n)
	nodes := make([]*RaftMetadataRepository, n)

	for i := range peers {
		peers[i] = fmt.Sprintf("http://127.0.0.1:%d", 10000+i)
	}

	logger, _ := zap.NewDevelopment()
	clus := &metadataRepoCluster{
		peers:             peers,
		nodes:             nodes,
		reporterClientFac: NewDummyReporterClientFactory(true),
		logger:            logger,
	}

	for i, peer := range clus.peers {
		url, err := url.Parse(peer)
		if err != nil {
			return nil
		}
		nodeID := types.NewNodeID(url.Host)

		os.RemoveAll(fmt.Sprintf("raft-%d", nodeID))
		os.RemoveAll(fmt.Sprintf("raft-%d-snap", nodeID))

		config := &Config{
			Index:             nodeID,
			NumRep:            nrRep,
			PeerList:          clus.peers,
			ReporterClientFac: clus.reporterClientFac,
			Logger:            clus.logger,
		}

		clus.nodes[i] = NewRaftMetadataRepository(config)
	}

	return clus
}

func (clus *metadataRepoCluster) Start() {
	for _, n := range clus.nodes {
		n.Run()
	}
}

// Close closes all cluster nodes
func (clus *metadataRepoCluster) Close() (err error) {
	for i := range clus.peers {
		err = clus.nodes[i].Close()

		os.RemoveAll(fmt.Sprintf("raft-%d", i+1))
		os.RemoveAll(fmt.Sprintf("raft-%d-snap", i+1))
	}
	return err
}

func (clus *metadataRepoCluster) waitVote() {
Loop:
	for {
		for _, n := range clus.nodes {
			if n.isLeader() {
				break Loop
			}
		}

		time.Sleep(10 * time.Millisecond)
	}
}

func (clus *metadataRepoCluster) closeNoErrors(t *testing.T) {
	if err := clus.Close(); err != nil {
		t.Fatal(err)
	}
}

func makeLocalLogStream(snId types.StorageNodeID, knownNextGLSN types.GLSN, lsId types.LogStreamID, uncommitBegin, uncommitEnd types.LLSN) *snpb.LocalLogStreamDescriptor {
	lls := &snpb.LocalLogStreamDescriptor{
		StorageNodeID: snId,
		NextGLSN:      knownNextGLSN,
	}
	ls := &snpb.LocalLogStreamDescriptor_LogStreamUncommitReport{
		LogStreamID:          lsId,
		UncommittedLLSNBegin: uncommitBegin,
		UncommittedLLSNEnd:   uncommitEnd,
	}
	lls.Uncommit = append(lls.Uncommit, ls)

	return lls
}

func makeLogStream(lsID types.LogStreamID, snIDs []types.StorageNodeID) *varlogpb.LogStreamDescriptor {
	ls := &varlogpb.LogStreamDescriptor{
		LogStreamID: lsID,
		Status:      varlogpb.LogStreamStatusNormal,
	}

	for _, snID := range snIDs {
		r := &varlogpb.ReplicaDescriptor{
			StorageNodeID: snID,
		}

		ls.Replicas = append(ls.Replicas, r)
	}

	return ls
}

func TestApplyReport(t *testing.T) {

	Convey("Report Should not be applied if not register LogStream", t, func(ctx C) {
		rep := 2
		clus := newMetadataRepoCluster(1, rep)
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

		lls := makeLocalLogStream(snIds[0], types.GLSN(0), lsId, types.LLSN(0), types.LLSN(2))
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
				lls := makeLocalLogStream(notExistSnID, types.GLSN(0), lsId, types.LLSN(0), types.LLSN(2))
				mr.applyReport(&pb.Report{LogStream: lls})

				r := mr.storage.LookupLocalLogStreamReplica(lsId, notExistSnID)
				So(r, ShouldBeNil)
			})

			Convey("Report should apply if snID is exist in LocalLogStream", func(ctx C) {
				snId := snIds[0]
				lls := makeLocalLogStream(snId, types.GLSN(0), lsId, types.LLSN(0), types.LLSN(2))
				mr.applyReport(&pb.Report{LogStream: lls})

				r := mr.storage.LookupLocalLogStreamReplica(lsId, snId)
				So(r, ShouldNotBeNil)
				So(r.EndLLSN, ShouldEqual, types.LLSN(2))

				Convey("Report which have bigger END LLSN Should be applied", func(ctx C) {
					lls := makeLocalLogStream(snId, types.GLSN(0), lsId, types.LLSN(0), types.LLSN(3))
					mr.applyReport(&pb.Report{LogStream: lls})

					r := mr.storage.LookupLocalLogStreamReplica(lsId, snId)
					So(r, ShouldNotBeNil)
					So(r.EndLLSN, ShouldEqual, types.LLSN(3))
				})

				Convey("Report which have smaller END LLSN Should Not be applied", func(ctx C) {
					lls := makeLocalLogStream(snId, types.GLSN(0), lsId, types.LLSN(0), types.LLSN(1))
					mr.applyReport(&pb.Report{LogStream: lls})

					r := mr.storage.LookupLocalLogStreamReplica(lsId, snId)
					So(r, ShouldNotBeNil)
					So(r.EndLLSN, ShouldNotEqual, types.LLSN(1))
				})
			})
		})
	})
}

func TestCalculateCommit(t *testing.T) {
	Convey("Calculate commit", t, func(ctx C) {
		clus := newMetadataRepoCluster(1, 2)
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
			lls := makeLocalLogStream(snIds[0], types.GLSN(0), lsId, types.LLSN(0), types.LLSN(2))
			mr.applyReport(&pb.Report{LogStream: lls})

			replicas := mr.storage.LookupLocalLogStream(lsId)
			_, nrCommit := mr.calculateCommit(replicas)
			So(nrCommit, ShouldEqual, 0)
		})

		Convey("LogStream which all reports are disjoint cannot be commit", func(ctx C) {
			lls := makeLocalLogStream(snIds[0], types.GLSN(10), lsId, types.LLSN(5), types.LLSN(6))
			mr.applyReport(&pb.Report{LogStream: lls})

			lls = makeLocalLogStream(snIds[1], types.GLSN(7), lsId, types.LLSN(3), types.LLSN(5))
			mr.applyReport(&pb.Report{LogStream: lls})

			replicas := mr.storage.LookupLocalLogStream(lsId)
			_, nrCommit := mr.calculateCommit(replicas)
			So(nrCommit, ShouldEqual, 0)
		})

		Convey("LogStream Should be commit where replication is completed", func(ctx C) {
			lls := makeLocalLogStream(snIds[0], types.GLSN(10), lsId, types.LLSN(3), types.LLSN(6))
			mr.applyReport(&pb.Report{LogStream: lls})

			lls = makeLocalLogStream(snIds[1], types.GLSN(9), lsId, types.LLSN(3), types.LLSN(5))
			mr.applyReport(&pb.Report{LogStream: lls})

			replicas := mr.storage.LookupLocalLogStream(lsId)
			glsn, nrCommit := mr.calculateCommit(replicas)
			So(nrCommit, ShouldEqual, 2)
			So(glsn, ShouldEqual, types.GLSN(10))
		})
	})
}

func TestGlobalCommit(t *testing.T) {
	Convey("Calculate commit", t, func(ctx C) {
		rep := 2
		clus := newMetadataRepoCluster(1, rep)
		clus.Start()
		clus.waitVote()

		Reset(func() {
			clus.closeNoErrors(t)
		})

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
			lls := makeLocalLogStream(snIds[0][0], types.GLSN(0), lsIds[0], types.LLSN(0), types.LLSN(2))
			mr.proposeReport(lls)

			lls = makeLocalLogStream(snIds[0][1], types.GLSN(0), lsIds[0], types.LLSN(0), types.LLSN(2))
			mr.proposeReport(lls)

			lls = makeLocalLogStream(snIds[1][0], types.GLSN(0), lsIds[1], types.LLSN(0), types.LLSN(4))
			mr.proposeReport(lls)

			lls = makeLocalLogStream(snIds[1][1], types.GLSN(0), lsIds[1], types.LLSN(0), types.LLSN(3))
			mr.proposeReport(lls)

			// global commit (2, 3) highest glsn: 5
			So(testutil.CompareWait(func() bool {
				return mr.storage.GetNextGLSN() == types.GLSN(5)
			}, time.Second), ShouldBeTrue)

			Convey("LogStream should be dedup", func(ctx C) {
				lls := makeLocalLogStream(snIds[0][0], types.GLSN(0), lsIds[0], types.LLSN(0), types.LLSN(3))
				mr.proposeReport(lls)

				lls = makeLocalLogStream(snIds[0][1], types.GLSN(0), lsIds[0], types.LLSN(0), types.LLSN(2))
				mr.proposeReport(lls)

				time.Sleep(100 * time.Millisecond)

				So(testutil.CompareWait(func() bool {
					return mr.storage.GetNextGLSN() == types.GLSN(5)
				}, time.Second), ShouldBeTrue)
			})

			Convey("LogStream which have wrong GLSN but have uncommitted should commit", func(ctx C) {
				lls := makeLocalLogStream(snIds[0][0], types.GLSN(0), lsIds[0], types.LLSN(0), types.LLSN(6))
				mr.proposeReport(lls)

				lls = makeLocalLogStream(snIds[0][1], types.GLSN(0), lsIds[0], types.LLSN(0), types.LLSN(6))
				mr.proposeReport(lls)

				So(testutil.CompareWait(func() bool {
					return mr.storage.GetNextGLSN() == types.GLSN(9)
				}, time.Second), ShouldBeTrue)
			})
		})
	})
}

func TestSimpleReportNCommit(t *testing.T) {
	Convey("Uncommitted LocalLogStream should be committed", t, func(ctx C) {
		clus := newMetadataRepoCluster(1, 1)
		clus.Start()
		clus.waitVote()

		Reset(func() {
			clus.closeNoErrors(t)
		})

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

func TestRequestMap(t *testing.T) {
	Convey("requestMap should have request when wait ack", t, func(ctx C) {
		clus := newMetadataRepoCluster(1, 1)
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
		clus := newMetadataRepoCluster(1, 1)
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
		clus := newMetadataRepoCluster(1, 1)
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
		clus := newMetadataRepoCluster(1, 1)
		clus.Start()
		clus.waitVote()

		Reset(func() {
			clus.closeNoErrors(t)
		})

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
