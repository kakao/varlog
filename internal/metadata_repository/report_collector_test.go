package metadata_repository

import (
	"sort"
	"sync"
	"testing"
	"time"

	types "github.daumkakao.com/varlog/varlog/pkg/varlog/types"
	"github.daumkakao.com/varlog/varlog/pkg/varlog/util/testutil"
	snpb "github.daumkakao.com/varlog/varlog/proto/storage_node"
	varlogpb "github.daumkakao.com/varlog/varlog/proto/varlog"

	. "github.com/smartystreets/goconvey/convey"
	"go.uber.org/zap"
)

type dummyMetadataRepository struct {
	reportC chan *snpb.LocalLogStreamDescriptor
	m       []*snpb.GlobalLogStreamDescriptor
	mt      sync.Mutex
}

func NewDummyMetadataRepository() *dummyMetadataRepository {
	return &dummyMetadataRepository{
		reportC: make(chan *snpb.LocalLogStreamDescriptor),
	}
}

func (mr *dummyMetadataRepository) report(lls *snpb.LocalLogStreamDescriptor) {
	select {
	case mr.reportC <- lls:
	default:
	}
}

func (mr *dummyMetadataRepository) getNextGLS(glsn types.GLSN) *snpb.GlobalLogStreamDescriptor {
	mr.mt.Lock()
	defer mr.mt.Unlock()

	i := sort.Search(len(mr.m), func(i int) bool {
		return mr.m[i].PrevNextGLSN >= glsn
	})

	if i < len(mr.m) && mr.m[i].PrevNextGLSN == glsn {
		return mr.m[i]
	}

	return nil
}

func (mr *dummyMetadataRepository) appendGLS(gls *snpb.GlobalLogStreamDescriptor) {
	mr.mt.Lock()
	defer mr.mt.Unlock()

	mr.m = append(mr.m, gls)
	sort.Slice(mr.m, func(i, j int) bool { return mr.m[i].NextGLSN < mr.m[j].NextGLSN })
}

func (mr *dummyMetadataRepository) trimGLS(glsn types.GLSN) {
	mr.mt.Lock()
	defer mr.mt.Unlock()

	for i, gls := range mr.m {
		if glsn == gls.NextGLSN {
			mr.m = mr.m[i:]
			return
		}
	}
}

func TestRegisterStorageNode(t *testing.T) {
	Convey("Registering nil storage node should return an error", t, func() {
		a := NewDummyReporterClientFactory(false)
		mr := NewDummyMetadataRepository()
		cb := ReportCollectorCallbacks{
			report:     mr.report,
			getClient:  a.GetClient,
			getNextGLS: mr.getNextGLS,
		}
		logger, _ := zap.NewDevelopment()
		reportCollector := NewReportCollector(cb, logger)
		defer reportCollector.Close()

		err := reportCollector.RegisterStorageNode(nil, types.GLSN(0))
		So(err, ShouldNotBeNil)
	})

	Convey("Registering dup storage node should return an error", t, func() {
		a := NewDummyReporterClientFactory(false)
		mr := NewDummyMetadataRepository()
		cb := ReportCollectorCallbacks{
			report:     mr.report,
			getClient:  a.GetClient,
			getNextGLS: mr.getNextGLS,
		}
		logger, _ := zap.NewDevelopment()
		reportCollector := NewReportCollector(cb, logger)
		defer reportCollector.Close()

		err := reportCollector.RegisterStorageNode(&varlogpb.StorageNodeDescriptor{}, types.GLSN(0))
		So(err, ShouldBeNil)

		err = reportCollector.RegisterStorageNode(&varlogpb.StorageNodeDescriptor{}, types.GLSN(0))
		So(err, ShouldNotBeNil)
	})
}

func TestReport(t *testing.T) {
	Convey("ReportCollector should collect report from registered storage node", t, func() {
		nrStorage := 5
		a := NewDummyReporterClientFactory(false)
		mr := NewDummyMetadataRepository()
		cb := ReportCollectorCallbacks{
			report:     mr.report,
			getClient:  a.GetClient,
			getNextGLS: mr.getNextGLS,
		}

		logger, _ := zap.NewDevelopment()
		reportCollector := NewReportCollector(cb, logger)
		defer reportCollector.Close()

		var wg sync.WaitGroup
		wg.Add(1)
		go func(nrStorage int) {
			defer wg.Done()
			m := make(map[types.StorageNodeID]int)

			after := time.After(time.Second)

		Loop:
			for {
				select {
				case <-after:
					t.Error("timeout")
					break Loop
				case lls := <-mr.reportC:
					if num, ok := m[lls.StorageNodeID]; ok {
						m[lls.StorageNodeID] = num + 1
					} else {
						m[lls.StorageNodeID] = 1
					}

					if len(m) == nrStorage {
						for _, num := range m {
							if num < 10 {
								continue Loop
							}
						}

						break Loop
					}
				}
			}
		}(nrStorage)

		for i := 0; i < nrStorage; i++ {
			sn := &varlogpb.StorageNodeDescriptor{
				StorageNodeID: types.StorageNodeID(i),
			}

			err := reportCollector.RegisterStorageNode(sn, types.GLSN(0))
			if err != nil {
				t.Fatal(err)
			}
		}

		wg.Wait()
	})
}

func newDummyGlobalLogStream(prev types.GLSN, nrStorage int) *snpb.GlobalLogStreamDescriptor {
	gls := &snpb.GlobalLogStreamDescriptor{
		NextGLSN:     prev + types.GLSN(nrStorage),
		PrevNextGLSN: prev,
	}
	glsn := prev

	for i := 0; i < nrStorage; i++ {
		lls := &snpb.GlobalLogStreamDescriptor_LogStreamCommitResult{
			LogStreamID:        types.LogStreamID(i),
			CommittedGLSNBegin: glsn,
			CommittedGLSNEnd:   glsn + 1,
		}
		glsn += 1

		gls.CommitResult = append(gls.CommitResult, lls)
	}

	return gls
}

func TestCommit(t *testing.T) {
	nrStorage := 5
	knownGLSN := types.GLSN(0)

	a := NewDummyReporterClientFactory(false)
	mr := NewDummyMetadataRepository()
	cb := ReportCollectorCallbacks{
		report:     mr.report,
		getClient:  a.GetClient,
		getNextGLS: mr.getNextGLS,
	}

	logger, _ := zap.NewDevelopment()
	reportCollector := NewReportCollector(cb, logger)
	defer reportCollector.Close()

	for i := 0; i < nrStorage; i++ {
		sn := &varlogpb.StorageNodeDescriptor{
			StorageNodeID: types.StorageNodeID(i),
		}

		err := reportCollector.RegisterStorageNode(sn, types.GLSN(0))
		if err != nil {
			t.Fatal(err)
		}
	}

	Convey("ReportCollector should broadcast commit result to registered storage node", t, func() {
		gls := newDummyGlobalLogStream(knownGLSN, nrStorage)
		mr.appendGLS(gls)
		knownGLSN = gls.NextGLSN

		reportCollector.Commit(gls)

		for _, cli := range a.m {
			So(testutil.CompareWait(func() bool {
				cli.mu.Lock()
				defer cli.mu.Unlock()

				return cli.knownNextGLSN == knownGLSN
			}, 100*time.Millisecond), ShouldBeTrue)
		}

		So(testutil.CompareWait(func() bool {
			return reportCollector.GetTrimmableNextGLSN() == knownGLSN
		}, 100*time.Millisecond), ShouldBeTrue)
	})

	Convey("ReportCollector should send ordered commit result to registered storage node", t, func() {
		gls := newDummyGlobalLogStream(knownGLSN, nrStorage)
		mr.appendGLS(gls)
		knownGLSN = gls.NextGLSN

		gls = newDummyGlobalLogStream(knownGLSN, nrStorage)
		mr.appendGLS(gls)
		knownGLSN = gls.NextGLSN

		reportCollector.Commit(gls)

		for _, cli := range a.m {
			So(testutil.CompareWait(func() bool {
				cli.mu.Lock()
				defer cli.mu.Unlock()

				return cli.knownNextGLSN == knownGLSN
			}, 100*time.Millisecond), ShouldBeTrue)
		}

		So(testutil.CompareWait(func() bool {
			return reportCollector.GetTrimmableNextGLSN() == knownGLSN
		}, 100*time.Millisecond), ShouldBeTrue)
	})

	Convey("ReportCollector should send proper commit against new StorageNode", t, func() {
		mr.trimGLS(knownGLSN)

		sn := &varlogpb.StorageNodeDescriptor{
			StorageNodeID: types.StorageNodeID(nrStorage),
		}

		err := reportCollector.RegisterStorageNode(sn, knownGLSN)
		if err != nil {
			t.Fatal(err)
		}

		gls := newDummyGlobalLogStream(knownGLSN, nrStorage+1)
		mr.appendGLS(gls)
		knownGLSN = gls.NextGLSN

		reportCollector.Commit(gls)

		for _, cli := range a.m {
			So(testutil.CompareWait(func() bool {
				cli.mu.Lock()
				defer cli.mu.Unlock()

				return cli.knownNextGLSN == knownGLSN
			}, 100*time.Millisecond), ShouldBeTrue)
		}
	})
}
