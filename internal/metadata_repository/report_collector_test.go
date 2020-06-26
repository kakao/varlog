package metadata_repository

import (
	"errors"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/kakao/varlog/internal/storage"
	types "github.com/kakao/varlog/pkg/varlog/types"
	snpb "github.com/kakao/varlog/proto/storage_node"
	varlogpb "github.com/kakao/varlog/proto/varlog"

	. "github.com/smartystreets/goconvey/convey"
)

type dummyLogStreamReporterClient struct {
	storageNodeID types.StorageNodeID
	knownNextGLSN types.GLSN

	logStreamID          types.LogStreamID
	uncommittedLLSNBegin types.LLSN
	uncommittedLLSNEnd   types.LLSN

	mu sync.Mutex
}

type dummyLogStreamReporterClientAllocator struct {
	m map[types.StorageNodeID]*dummyLogStreamReporterClient
}

func newDummyLogStreamReporterClientAllocator() *dummyLogStreamReporterClientAllocator {
	a := &dummyLogStreamReporterClientAllocator{
		m: make(map[types.StorageNodeID]*dummyLogStreamReporterClient),
	}

	return a
}

func (a *dummyLogStreamReporterClientAllocator) connect(sn *varlogpb.StorageNodeDescriptor) (storage.LogStreamRepoterClient, error) {
	cli := &dummyLogStreamReporterClient{
		storageNodeID: sn.StorageNodeID,
		logStreamID:   types.LogStreamID(sn.StorageNodeID),
	}

	a.m[sn.StorageNodeID] = cli

	return cli, nil
}

func (r *dummyLogStreamReporterClient) GetReport() (snpb.LocalLogStreamDescriptor, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.uncommittedLLSNEnd += 1

	lls := snpb.LocalLogStreamDescriptor{
		StorageNodeID: r.storageNodeID,
		NextGLSN:      r.knownNextGLSN,
		Uncommit: []*snpb.LocalLogStreamDescriptor_LogStreamUncommitReport{
			{
				LogStreamID:          r.logStreamID,
				UncommittedLLSNBegin: r.uncommittedLLSNBegin,
				UncommittedLLSNEnd:   r.uncommittedLLSNEnd,
			},
		},
	}

	return lls, nil
}

func (r *dummyLogStreamReporterClient) Commit(glsn snpb.GlobalLogStreamDescriptor) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if glsn.PrevNextGLSN != r.knownNextGLSN {
		return nil
	}

	r.knownNextGLSN = glsn.NextGLSN

	for _, result := range glsn.CommitResult {
		if result.LogStreamID != r.logStreamID {
			return errors.New("invalid log stream ID")
		}

		nrCommitted := uint64(result.CommittedGLSNEnd - result.CommittedGLSNBegin)
		r.uncommittedLLSNBegin += types.LLSN(nrCommitted)
	}

	return nil
}

type dummyMetadataRepository struct {
	reportC chan *snpb.LocalLogStreamDescriptor
	m       []*snpb.GlobalLogStreamDescriptor
}

func NewDummyMetadataRepository() *dummyMetadataRepository {
	m := make([]*snpb.GlobalLogStreamDescriptor, 1)
	m[0] = &snpb.GlobalLogStreamDescriptor{}

	return &dummyMetadataRepository{
		reportC: make(chan *snpb.LocalLogStreamDescriptor),
		m:       m,
	}
}

func (mr *dummyMetadataRepository) report(lls *snpb.LocalLogStreamDescriptor) {
	select {
	case mr.reportC <- lls:
	default:
	}
}

func (mr *dummyMetadataRepository) getNextGLS(glsn types.GLSN) *snpb.GlobalLogStreamDescriptor {
	i := sort.Search(len(mr.m), func(i int) bool {
		return mr.m[i].PrevNextGLSN >= glsn
	})

	if i < len(mr.m) && mr.m[i].PrevNextGLSN == glsn {
		return mr.m[i]
	}

	return nil
}

func (mr *dummyMetadataRepository) appendGLS(gls *snpb.GlobalLogStreamDescriptor) {
	mr.m = append(mr.m, gls)
	sort.Slice(mr.m, func(i, j int) bool { return mr.m[i].NextGLSN < mr.m[j].NextGLSN })
}

func TestRegisterStorageNode(t *testing.T) {
	Convey("Registering nil storage node should return an error", t, func() {
		a := newDummyLogStreamReporterClientAllocator()
		mr := NewDummyMetadataRepository()
		cb := ReportCollectorCallbacks{
			connect:    a.connect,
			report:     mr.report,
			getNextGLS: mr.getNextGLS,
		}
		reportCollector := NewReportCollector(cb)
		defer reportCollector.Close()

		err := reportCollector.RegisterStorageNode(nil)
		So(err, ShouldNotBeNil)
	})

	Convey("Registering dup storage node should return an error", t, func() {
		a := newDummyLogStreamReporterClientAllocator()
		mr := NewDummyMetadataRepository()
		cb := ReportCollectorCallbacks{
			connect:    a.connect,
			report:     mr.report,
			getNextGLS: mr.getNextGLS,
		}
		reportCollector := NewReportCollector(cb)
		defer reportCollector.Close()

		err := reportCollector.RegisterStorageNode(&varlogpb.StorageNodeDescriptor{})
		So(err, ShouldBeNil)

		err = reportCollector.RegisterStorageNode(&varlogpb.StorageNodeDescriptor{})
		So(err, ShouldNotBeNil)
	})
}

func TestReport(t *testing.T) {
	Convey("ReportCollector should collect report from registered storage node", t, func() {
		nrStorage := 5
		a := newDummyLogStreamReporterClientAllocator()
		mr := NewDummyMetadataRepository()
		cb := ReportCollectorCallbacks{
			connect:    a.connect,
			report:     mr.report,
			getNextGLS: mr.getNextGLS,
		}

		reportCollector := NewReportCollector(cb)
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

			err := reportCollector.RegisterStorageNode(sn)
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

	a := newDummyLogStreamReporterClientAllocator()
	mr := NewDummyMetadataRepository()
	cb := ReportCollectorCallbacks{
		connect:    a.connect,
		report:     mr.report,
		getNextGLS: mr.getNextGLS,
	}

	reportCollector := NewReportCollector(cb)
	defer reportCollector.Close()

	for i := 0; i < nrStorage; i++ {
		sn := &varlogpb.StorageNodeDescriptor{
			StorageNodeID: types.StorageNodeID(i),
		}

		err := reportCollector.RegisterStorageNode(sn)
		if err != nil {
			t.Fatal(err)
		}
	}

	Convey("ReportCollector should broadcast commit result to registered storage node", t, func() {
		gls := newDummyGlobalLogStream(types.GLSN(0), nrStorage)
		mr.appendGLS(gls)

		reportCollector.Commit(gls)

		time.Sleep(10 * time.Millisecond)

		for _, cli := range a.m {
			cli.mu.RLock()
			So(cli.knownNextGLSN, ShouldEqual, types.GLSN(5))
			cli.mu.RUnlock()
		}
	})

	Convey("ReportCollector should send ordered commit result to registered storage node", t, func() {
		gls := newDummyGlobalLogStream(types.GLSN(5), nrStorage)
		mr.appendGLS(gls)

		gls = newDummyGlobalLogStream(types.GLSN(10), nrStorage)
		mr.appendGLS(gls)

		reportCollector.Commit(gls)

		time.Sleep(10 * time.Millisecond)

		for _, cli := range a.m {
			So(cli.knownNextGLSN, ShouldEqual, types.GLSN(15))
		}
	})
}
