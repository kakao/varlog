package metadata_repository

import (
	"fmt"
	"os"
	"testing"
	"time"

	types "github.com/kakao/varlog/pkg/varlog/types"
	snpb "github.com/kakao/varlog/proto/storage_node"

	. "github.com/smartystreets/goconvey/convey"
)

type metadataRepoCluster struct {
	peers []string
	nodes []*RaftMetadataRepository
}

func newMetadataRepoCluster(n, nrRep int) *metadataRepoCluster {
	peers := make([]string, n)
	nodes := make([]*RaftMetadataRepository, n)

	for i := range peers {
		peers[i] = fmt.Sprintf("http://127.0.0.1:%d", 10000+i)
	}

	clus := &metadataRepoCluster{
		peers: peers,
		nodes: nodes,
	}

	for i := range clus.peers {
		os.RemoveAll(fmt.Sprintf("raftexample-%d", i+1))
		os.RemoveAll(fmt.Sprintf("raftexample-%d-snap", i+1))

		clus.nodes[i] = NewRaftMetadataRepository(i, nrRep, clus.peers)
		clus.nodes[i].Start()
	}

	return clus
}

// Close closes all cluster nodes
func (clus *metadataRepoCluster) Close() (err error) {
	for i := range clus.peers {
		err = clus.nodes[i].Close()

		os.RemoveAll(fmt.Sprintf("raftexample-%d", i+1))
		os.RemoveAll(fmt.Sprintf("raftexample-%d-snap", i+1))
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

func (clus *metadataRepoCluster) clearLocalCut() {
	for _, n := range clus.nodes {
		n.localCuts = make(map[types.LogStreamID]map[types.StorageNodeID]localCutInfo)
	}
}

func (clus *metadataRepoCluster) clearGlobalCut() {
	for _, n := range clus.nodes {
		n.smr.GlobalLogStreams = nil
		n.smr.GlobalLogStreams = append(n.smr.GlobalLogStreams, &snpb.GlobalLogStreamDescriptor{})
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
	clus := newMetadataRepoCluster(1, 2)
	defer clus.closeNoErrors(t)

	clus.waitVote()

	Convey("Report Should be applied", t, func(ctx C) {
		clus.clearLocalCut()

		snId := types.StorageNodeID(0)
		lsId := types.LogStreamID(0)

		// propose report
		lls := makeLocalLogStream(snId, types.GLSN(0), lsId, types.LLSN(0), types.LLSN(2))
		clus.nodes[0].proposeReport(lls)

		// wait commit
		time.Sleep(10 * time.Millisecond)

		m, ok := clus.nodes[0].localCuts[lsId]
		So(ok, ShouldBeTrue)

		lc, ok := m[snId]
		So(ok, ShouldBeTrue)
		So(lc.endLlsn, ShouldEqual, types.LLSN(2))

		Convey("Report which have bigger END LLSN Should be applied", func(ctx C) {
			// propose report
			lls := makeLocalLogStream(snId, types.GLSN(0), lsId, types.LLSN(0), types.LLSN(3))
			clus.nodes[0].proposeReport(lls)

			// wait commit
			time.Sleep(10 * time.Millisecond)

			m, ok := clus.nodes[0].localCuts[lsId]
			So(ok, ShouldBeTrue)

			lc, ok := m[snId]
			So(ok, ShouldBeTrue)
			So(lc.endLlsn, ShouldEqual, types.LLSN(3))
		})

		Convey("Report which have smaller END LLSN Should Not be applied", func(ctx C) {
			// propose report
			lls := makeLocalLogStream(snId, types.GLSN(0), lsId, types.LLSN(0), types.LLSN(1))
			clus.nodes[0].proposeReport(lls)

			// wait commit
			time.Sleep(10 * time.Millisecond)

			m, ok := clus.nodes[0].localCuts[lsId]
			So(ok, ShouldBeTrue)

			lc, ok := m[snId]
			So(ok, ShouldBeTrue)
			So(lc.endLlsn, ShouldNotEqual, types.LLSN(1))
		})
	})
}

func TestCalculateCut(t *testing.T) {
	clus := newMetadataRepoCluster(1, 2)
	defer clus.closeNoErrors(t)

	clus.waitVote()

	Convey("Calculate cut", t, func(ctx C) {
		clus.clearLocalCut()

		snIds := make([]types.StorageNodeID, 2)
		for i := range snIds {
			snIds[i] = types.StorageNodeID(i)
		}
		lsId := types.LogStreamID(0)

		mr := clus.nodes[0]

		Convey("LogStream which all reports have not arrived cannot be cut", func(ctx C) {
			lls := makeLocalLogStream(snIds[0], types.GLSN(0), lsId, types.LLSN(0), types.LLSN(2))
			mr.proposeReport(lls)

			// wait commit
			time.Sleep(10 * time.Millisecond)

			m, ok := mr.localCuts[lsId]
			So(ok, ShouldBeTrue)

			_, nrCut := mr.calculateCut(m)
			So(nrCut, ShouldEqual, 0)
		})

		Convey("LogStream which all reports are disjoint cannot be cut", func(ctx C) {
			lls := makeLocalLogStream(snIds[0], types.GLSN(10), lsId, types.LLSN(5), types.LLSN(6))
			mr.proposeReport(lls)

			lls = makeLocalLogStream(snIds[1], types.GLSN(7), lsId, types.LLSN(3), types.LLSN(5))
			mr.proposeReport(lls)

			// wait commit
			time.Sleep(10 * time.Millisecond)

			m, ok := mr.localCuts[lsId]
			So(ok, ShouldBeTrue)

			_, nrCut := mr.calculateCut(m)
			So(nrCut, ShouldEqual, 0)
		})

		Convey("LogStream Should be cut where replication is completed", func(ctx C) {
			lls := makeLocalLogStream(snIds[0], types.GLSN(10), lsId, types.LLSN(3), types.LLSN(6))
			mr.proposeReport(lls)

			lls = makeLocalLogStream(snIds[1], types.GLSN(9), lsId, types.LLSN(3), types.LLSN(5))
			mr.proposeReport(lls)

			// wait commit
			time.Sleep(10 * time.Millisecond)

			m, ok := mr.localCuts[lsId]
			So(ok, ShouldBeTrue)

			glsn, nrCut := mr.calculateCut(m)
			So(nrCut, ShouldEqual, 2)
			So(glsn, ShouldEqual, types.GLSN(10))
		})
	})
}

func TestGlobalCut(t *testing.T) {
	clus := newMetadataRepoCluster(1, 2)
	defer clus.closeNoErrors(t)

	clus.waitVote()

	Convey("Calculate cut", t, func(ctx C) {
		clus.clearLocalCut()
		clus.clearGlobalCut()

		snIds := make([]types.StorageNodeID, 4)
		for i := range snIds {
			snIds[i] = types.StorageNodeID(i)
		}

		lsIds := make([]types.LogStreamID, 2)
		for i := range lsIds {
			lsIds[i] = types.LogStreamID(i)
		}

		mr := clus.nodes[0]

		Convey("global cut", func(ctx C) {
			lls := makeLocalLogStream(snIds[0], types.GLSN(0), lsIds[0], types.LLSN(0), types.LLSN(2))
			mr.proposeReport(lls)

			lls = makeLocalLogStream(snIds[1], types.GLSN(0), lsIds[0], types.LLSN(0), types.LLSN(2))
			mr.proposeReport(lls)

			lls = makeLocalLogStream(snIds[2], types.GLSN(0), lsIds[1], types.LLSN(0), types.LLSN(4))
			mr.proposeReport(lls)

			lls = makeLocalLogStream(snIds[3], types.GLSN(0), lsIds[1], types.LLSN(0), types.LLSN(3))
			mr.proposeReport(lls)

			// global cut (2, 3) highest glsn: 5
			mr.proposeCut()

			// wait commit
			time.Sleep(10 * time.Millisecond)

			So(len(mr.smr.GlobalLogStreams), ShouldBeGreaterThan, 0)
			gls := mr.smr.GlobalLogStreams[len(mr.smr.GlobalLogStreams)-1]

			So(gls.NextGLSN, ShouldEqual, types.GLSN(5))

			Convey("LogStream should be dedup", func(ctx C) {
				lls := makeLocalLogStream(snIds[0], types.GLSN(0), lsIds[0], types.LLSN(0), types.LLSN(3))
				mr.proposeReport(lls)

				lls = makeLocalLogStream(snIds[1], types.GLSN(0), lsIds[0], types.LLSN(0), types.LLSN(2))
				mr.proposeReport(lls)

				mr.proposeCut()

				// wait commit
				time.Sleep(10 * time.Millisecond)

				So(len(mr.smr.GlobalLogStreams), ShouldBeGreaterThan, 0)
				gls := mr.smr.GlobalLogStreams[len(mr.smr.GlobalLogStreams)-1]

				So(gls.NextGLSN, ShouldEqual, types.GLSN(5))
			})

			Convey("LogStream which have wrong GLSN but have uncommitted should cut", func(ctx C) {
				lls := makeLocalLogStream(snIds[0], types.GLSN(0), lsIds[0], types.LLSN(0), types.LLSN(6))
				mr.proposeReport(lls)

				lls = makeLocalLogStream(snIds[1], types.GLSN(0), lsIds[0], types.LLSN(0), types.LLSN(6))
				mr.proposeReport(lls)

				mr.proposeCut()

				// wait commit
				time.Sleep(10 * time.Millisecond)

				So(len(mr.smr.GlobalLogStreams), ShouldBeGreaterThan, 0)
				gls := mr.smr.GlobalLogStreams[len(mr.smr.GlobalLogStreams)-1]

				So(gls.NextGLSN, ShouldEqual, types.GLSN(9))
			})
		})
	})
}
