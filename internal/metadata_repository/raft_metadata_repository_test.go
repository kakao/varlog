package metadata_repository

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	types "github.com/kakao/varlog/pkg/varlog/types"
	pb "github.com/kakao/varlog/proto/metadata_repository"
	snpb "github.com/kakao/varlog/proto/storage_node"
	varlogpb "github.com/kakao/varlog/proto/varlog"
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

	for i := range clus.peers {
		os.RemoveAll(fmt.Sprintf("raft-%d", i+1))
		os.RemoveAll(fmt.Sprintf("raft-%d-snap", i+1))

		config := &Config{
			Index:             i,
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
		n.Start()
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

func TestApplyReport(t *testing.T) {
	Convey("Report Should be applied", t, func(ctx C) {
		clus := newMetadataRepoCluster(1, 2)
		mr := clus.nodes[0]

		snId := types.StorageNodeID(0)
		lsId := types.LogStreamID(0)

		// propose report
		lls := makeLocalLogStream(snId, types.GLSN(0), lsId, types.LLSN(0), types.LLSN(2))
		mr.applyReport(&pb.Report{LogStream: lls})

		m, ok := mr.localLogStreams[lsId]
		So(ok, ShouldBeTrue)

		lc, ok := m[snId]
		So(ok, ShouldBeTrue)
		So(lc.endLlsn, ShouldEqual, types.LLSN(2))

		Convey("Report which have bigger END LLSN Should be applied", func(ctx C) {
			// propose report
			lls := makeLocalLogStream(snId, types.GLSN(0), lsId, types.LLSN(0), types.LLSN(3))
			mr.applyReport(&pb.Report{LogStream: lls})

			m, ok := mr.localLogStreams[lsId]
			So(ok, ShouldBeTrue)

			lc, ok := m[snId]
			So(ok, ShouldBeTrue)
			So(lc.endLlsn, ShouldEqual, types.LLSN(3))
		})

		Convey("Report which have smaller END LLSN Should Not be applied", func(ctx C) {
			// propose report
			lls := makeLocalLogStream(snId, types.GLSN(0), lsId, types.LLSN(0), types.LLSN(1))
			mr.applyReport(&pb.Report{LogStream: lls})

			m, ok := mr.localLogStreams[lsId]
			So(ok, ShouldBeTrue)

			lc, ok := m[snId]
			So(ok, ShouldBeTrue)
			So(lc.endLlsn, ShouldNotEqual, types.LLSN(1))
		})
	})
}

func TestCalculateCommit(t *testing.T) {
	Convey("Calculate commit", t, func(ctx C) {
		clus := newMetadataRepoCluster(1, 2)

		snIds := make([]types.StorageNodeID, 2)
		for i := range snIds {
			snIds[i] = types.StorageNodeID(i)
		}
		lsId := types.LogStreamID(0)

		mr := clus.nodes[0]

		Convey("LogStream which all reports have not arrived cannot be commit", func(ctx C) {
			lls := makeLocalLogStream(snIds[0], types.GLSN(0), lsId, types.LLSN(0), types.LLSN(2))
			mr.applyReport(&pb.Report{LogStream: lls})

			m, ok := mr.localLogStreams[lsId]
			So(ok, ShouldBeTrue)

			_, nrCommit := mr.calculateCommit(m)
			So(nrCommit, ShouldEqual, 0)
		})

		Convey("LogStream which all reports are disjoint cannot be commit", func(ctx C) {
			lls := makeLocalLogStream(snIds[0], types.GLSN(10), lsId, types.LLSN(5), types.LLSN(6))
			mr.applyReport(&pb.Report{LogStream: lls})

			lls = makeLocalLogStream(snIds[1], types.GLSN(7), lsId, types.LLSN(3), types.LLSN(5))
			mr.applyReport(&pb.Report{LogStream: lls})

			m, ok := mr.localLogStreams[lsId]
			So(ok, ShouldBeTrue)

			_, nrCommit := mr.calculateCommit(m)
			So(nrCommit, ShouldEqual, 0)
		})

		Convey("LogStream Should be commit where replication is completed", func(ctx C) {
			lls := makeLocalLogStream(snIds[0], types.GLSN(10), lsId, types.LLSN(3), types.LLSN(6))
			mr.applyReport(&pb.Report{LogStream: lls})

			lls = makeLocalLogStream(snIds[1], types.GLSN(9), lsId, types.LLSN(3), types.LLSN(5))
			mr.applyReport(&pb.Report{LogStream: lls})

			m, ok := mr.localLogStreams[lsId]
			So(ok, ShouldBeTrue)

			glsn, nrCommit := mr.calculateCommit(m)
			So(nrCommit, ShouldEqual, 2)
			So(glsn, ShouldEqual, types.GLSN(10))
		})
	})
}

func waitCommit(resultF func() types.GLSN, glsn types.GLSN) error {
	t := time.After(time.Second)

	for {
		select {
		case <-t:
			return errors.New("timeout")
		default:
			if resultF() == glsn {
				return nil
			}

			time.Sleep(time.Millisecond)
		}
	}
}

func TestGlobalCommit(t *testing.T) {
	Convey("Calculate commit", t, func(ctx C) {
		clus := newMetadataRepoCluster(1, 2)
		clus.Start()
		clus.waitVote()

		snIds := make([]types.StorageNodeID, 4)
		for i := range snIds {
			snIds[i] = types.StorageNodeID(i)
		}

		lsIds := make([]types.LogStreamID, 2)
		for i := range lsIds {
			lsIds[i] = types.LogStreamID(i)
		}

		mr := clus.nodes[0]

		Convey("global commit", func(ctx C) {
			lls := makeLocalLogStream(snIds[0], types.GLSN(0), lsIds[0], types.LLSN(0), types.LLSN(2))
			mr.proposeReport(lls)

			lls = makeLocalLogStream(snIds[1], types.GLSN(0), lsIds[0], types.LLSN(0), types.LLSN(2))
			mr.proposeReport(lls)

			lls = makeLocalLogStream(snIds[2], types.GLSN(0), lsIds[1], types.LLSN(0), types.LLSN(4))
			mr.proposeReport(lls)

			lls = makeLocalLogStream(snIds[3], types.GLSN(0), lsIds[1], types.LLSN(0), types.LLSN(3))
			mr.proposeReport(lls)

			// global commit (2, 3) highest glsn: 5
			So(waitCommit(mr.getNextGLSN4Test, types.GLSN(5)), ShouldBeNil)

			Convey("LogStream should be dedup", func(ctx C) {
				lls := makeLocalLogStream(snIds[0], types.GLSN(0), lsIds[0], types.LLSN(0), types.LLSN(3))
				mr.proposeReport(lls)

				lls = makeLocalLogStream(snIds[1], types.GLSN(0), lsIds[0], types.LLSN(0), types.LLSN(2))
				mr.proposeReport(lls)

				time.Sleep(100 * time.Millisecond)
				So(waitCommit(mr.getNextGLSN4Test, types.GLSN(5)), ShouldBeNil)
			})

			Convey("LogStream which have wrong GLSN but have uncommitted should commit", func(ctx C) {
				lls := makeLocalLogStream(snIds[0], types.GLSN(0), lsIds[0], types.LLSN(0), types.LLSN(6))
				mr.proposeReport(lls)

				lls = makeLocalLogStream(snIds[1], types.GLSN(0), lsIds[0], types.LLSN(0), types.LLSN(6))
				mr.proposeReport(lls)

				So(waitCommit(mr.getNextGLSN4Test, types.GLSN(9)), ShouldBeNil)
			})
		})

		Reset(func() {
			clus.closeNoErrors(t)
		})
	})
}

func TestSimpleReportNCommit(t *testing.T) {
	clus := newMetadataRepoCluster(1, 1)
	defer clus.closeNoErrors(t)

	clus.Start()
	clus.waitVote()

	snID := types.StorageNodeID(0)

	sn := &varlogpb.StorageNodeDescriptor{
		StorageNodeID: snID,
	}

	err := clus.nodes[0].RegisterStorageNode(context.TODO(), sn)
	if err != nil {
		t.Fatal(err)
	}

REGISTER_CHECK:
	for {
		cli := clus.reporterClientFac.(*DummyReporterClientFactory).lookupClient(snID)
		if cli != nil {
			break REGISTER_CHECK
		}

		time.Sleep(time.Millisecond)
	}

	Convey("Uncommitted LocalLogStream should be committed", t, func(ctx C) {
		reporterClient := clus.reporterClientFac.(*DummyReporterClientFactory).lookupClient(snID)
		reporterClient.increaseUncommitted()

		time.Sleep(time.Second)

		So(reporterClient.numUncommitted(), ShouldBeZeroValue)
	})
}

func TestRequestMap(t *testing.T) {
	Convey("requestMap should have request when wait ack", t, func(ctx C) {
		clus := newMetadataRepoCluster(1, 1)
		mr := clus.nodes[0]

		sn := &varlogpb.StorageNodeDescriptor{
			StorageNodeID: types.StorageNodeID(0),
		}

		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			rctx, _ := context.WithTimeout(context.Background(), 20*time.Millisecond)
			mr.RegisterStorageNode(rctx, sn)
		}()

		time.Sleep(10 * time.Millisecond)
		_, ok := mr.requestMap.Load(uint64(1))

		wg.Wait()
		So(ok, ShouldBeTrue)
	})

	Convey("requestMap should ignore request that have different nodeIndex", t, func(ctx C) {
		clus := newMetadataRepoCluster(1, 1)
		mr := clus.nodes[0]

		sn := &varlogpb.StorageNodeDescriptor{
			StorageNodeID: types.StorageNodeID(0),
		}

		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()

			time.Sleep(10 * time.Millisecond)

			dummy := &pb.RaftEntry{
				NodeIndex:  2,
				RequestNum: uint64(1),
			}
			mr.commitC <- dummy
		}()

		rctx, _ := context.WithTimeout(context.Background(), 100*time.Millisecond)
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

		mr := clus.nodes[0]

		sn := &varlogpb.StorageNodeDescriptor{
			StorageNodeID: types.StorageNodeID(0),
		}

		err := mr.RegisterStorageNode(context.TODO(), sn)
		So(err, ShouldBeNil)

		_, ok := mr.requestMap.Load(uint64(1))
		So(ok, ShouldBeFalse)

		Reset(func() {
			clus.closeNoErrors(t)
		})
	})
}
