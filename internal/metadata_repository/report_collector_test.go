package metadata_repository

import (
	"context"
	"sort"
	"sync"
	"testing"
	"time"

	"github.daumkakao.com/varlog/varlog/internal/storage"
	varlog "github.daumkakao.com/varlog/varlog/pkg/varlog"
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
		reportC: make(chan *snpb.LocalLogStreamDescriptor, 4096),
	}
}

func (mr *dummyMetadataRepository) report(lls *snpb.LocalLogStreamDescriptor) error {
	select {
	case mr.reportC <- lls:
	default:
		return varlog.ErrIgnore
	}

	return nil
}

func (mr *dummyMetadataRepository) lookupNextGLS(glsn types.GLSN) *snpb.GlobalLogStreamDescriptor {
	mr.mt.Lock()
	defer mr.mt.Unlock()

	i := sort.Search(len(mr.m), func(i int) bool {
		return mr.m[i].PrevHighWatermark >= glsn
	})

	if i < len(mr.m) && mr.m[i].PrevHighWatermark == glsn {
		return mr.m[i]
	}

	return nil
}

func (mr *dummyMetadataRepository) appendGLS(gls *snpb.GlobalLogStreamDescriptor) {
	mr.mt.Lock()
	defer mr.mt.Unlock()

	mr.m = append(mr.m, gls)
	sort.Slice(mr.m, func(i, j int) bool { return mr.m[i].HighWatermark < mr.m[j].HighWatermark })
}

func (mr *dummyMetadataRepository) trimGLS(glsn types.GLSN) {
	mr.mt.Lock()
	defer mr.mt.Unlock()

	for i, gls := range mr.m {
		if glsn == gls.HighWatermark {
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
			report:        mr.report,
			getClient:     a.GetClient,
			lookupNextGLS: mr.lookupNextGLS,
		}
		logger, _ := zap.NewDevelopment()
		reportCollector := NewReportCollector(cb, logger)
		reportCollector.Run()
		defer reportCollector.Close()

		err := reportCollector.RegisterStorageNode(nil, types.InvalidGLSN)
		So(err, ShouldNotBeNil)
	})

	Convey("Registering dup storage node should return an error", t, func() {
		a := NewDummyReporterClientFactory(false)
		mr := NewDummyMetadataRepository()
		cb := ReportCollectorCallbacks{
			report:        mr.report,
			getClient:     a.GetClient,
			lookupNextGLS: mr.lookupNextGLS,
		}
		logger, _ := zap.NewDevelopment()
		reportCollector := NewReportCollector(cb, logger)
		reportCollector.Run()
		defer reportCollector.Close()

		sn := &varlogpb.StorageNodeDescriptor{
			StorageNodeID: types.StorageNodeID(time.Now().UnixNano()),
		}

		err := reportCollector.RegisterStorageNode(sn, types.InvalidGLSN)
		So(err, ShouldBeNil)

		reportCollector.mu.RLock()

		_, ok := reportCollector.executors[sn.StorageNodeID]
		So(ok, ShouldBeTrue)

		reportCollector.mu.RUnlock()

		err = reportCollector.RegisterStorageNode(sn, types.InvalidGLSN)
		So(err, ShouldNotBeNil)
	})
}

func TestUnregisterStorageNode(t *testing.T) {
	Convey("Registering dup storage node should return an error", t, func() {
		a := NewDummyReporterClientFactory(false)
		mr := NewDummyMetadataRepository()
		cb := ReportCollectorCallbacks{
			report:        mr.report,
			getClient:     a.GetClient,
			lookupNextGLS: mr.lookupNextGLS,
		}
		logger, _ := zap.NewDevelopment()
		reportCollector := NewReportCollector(cb, logger)
		reportCollector.Run()
		defer reportCollector.Close()

		snID := types.StorageNodeID(time.Now().UnixNano())
		sn := &varlogpb.StorageNodeDescriptor{
			StorageNodeID: snID,
		}

		err := reportCollector.RegisterStorageNode(sn, types.InvalidGLSN)
		So(err, ShouldBeNil)

		err = reportCollector.UnregisterStorageNode(snID)
		So(err, ShouldBeNil)

		reportCollector.mu.RLock()

		_, ok := reportCollector.executors[sn.StorageNodeID]
		So(ok, ShouldBeFalse)

		reportCollector.mu.RUnlock()
	})
}

func TestRecoverStorageNode(t *testing.T) {
	Convey("Given ReportCollector", t, func() {
		a := NewDummyReporterClientFactory(false)
		mr := NewDummyMetadataRepository()
		cb := ReportCollectorCallbacks{
			report:        mr.report,
			getClient:     a.GetClient,
			lookupNextGLS: mr.lookupNextGLS,
		}
		logger, _ := zap.NewDevelopment()
		reportCollector := NewReportCollector(cb, logger)
		reportCollector.Run()
		defer reportCollector.Close()

		nrSN := 5
		hwm := types.MinGLSN
		var SNs []*varlogpb.StorageNodeDescriptor

		for i := 0; i < nrSN; i++ {
			sn := &varlogpb.StorageNodeDescriptor{
				StorageNodeID: types.StorageNodeID(time.Now().UnixNano()),
			}

			SNs = append(SNs, sn)

			err := reportCollector.RegisterStorageNode(sn, hwm)
			So(err, ShouldBeNil)
		}

		for i := 0; i < nrSN; i++ {
			reportCollector.mu.RLock()

			_, ok := reportCollector.executors[SNs[i].StorageNodeID]
			So(ok, ShouldBeTrue)

			reportCollector.mu.RUnlock()
		}

		Convey("When ReportCollector Close", func(ctx C) {
			reportCollector.Close()

			Convey("Then there shoulb be no ReportCollectExecutor", func(ctx C) {
				for i := 0; i < nrSN; i++ {
					reportCollector.mu.RLock()

					_, ok := reportCollector.executors[SNs[i].StorageNodeID]
					So(ok, ShouldBeFalse)

					reportCollector.mu.RUnlock()
				}

				Convey("When ReportCollector Recover", func(ctx C) {
					reportCollector.Recover(SNs, hwm)
					Convey("Then there shoulb be no ReportCollectExecutor", func(ctx C) {
						for i := 0; i < nrSN; i++ {
							reportCollector.mu.RLock()

							_, ok := reportCollector.executors[SNs[i].StorageNodeID]
							So(ok, ShouldBeTrue)

							reportCollector.mu.RUnlock()
						}
					})
				})
			})
		})
	})
}

func TestReport(t *testing.T) {
	Convey("ReportCollector should collect report from registered storage node", t, func() {
		nrStorage := 5
		a := NewDummyReporterClientFactory(false)
		mr := NewDummyMetadataRepository()
		cb := ReportCollectorCallbacks{
			report:        mr.report,
			getClient:     a.GetClient,
			lookupNextGLS: mr.lookupNextGLS,
		}

		logger, _ := zap.NewDevelopment()
		reportCollector := NewReportCollector(cb, logger)
		reportCollector.Run()
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

			err := reportCollector.RegisterStorageNode(sn, types.InvalidGLSN)
			if err != nil {
				t.Fatal(err)
			}
		}

		wg.Wait()
	})
}

func newDummyGlobalLogStream(prev types.GLSN, nrStorage int) *snpb.GlobalLogStreamDescriptor {
	gls := &snpb.GlobalLogStreamDescriptor{
		HighWatermark:     prev + types.GLSN(nrStorage),
		PrevHighWatermark: prev,
	}
	glsn := prev + types.GLSN(1)

	for i := 0; i < nrStorage; i++ {
		lls := &snpb.GlobalLogStreamDescriptor_LogStreamCommitResult{
			LogStreamID:         types.LogStreamID(i),
			CommittedGLSNOffset: glsn,
			CommittedGLSNLength: 1,
		}
		glsn += 1

		gls.CommitResult = append(gls.CommitResult, lls)
	}

	return gls
}

func TestCommit(t *testing.T) {
	Convey("Given ReportCollector", t, func() {
		nrStorage := 5
		knownHWM := types.InvalidGLSN

		a := NewDummyReporterClientFactory(false)
		mr := NewDummyMetadataRepository()
		cb := ReportCollectorCallbacks{
			report:        mr.report,
			getClient:     a.GetClient,
			lookupNextGLS: mr.lookupNextGLS,
		}

		logger, _ := zap.NewDevelopment()
		reportCollector := NewReportCollector(cb, logger)
		reportCollector.Run()
		Reset(func() {
			reportCollector.Close()
		})

		for i := 0; i < nrStorage; i++ {
			sn := &varlogpb.StorageNodeDescriptor{
				StorageNodeID: types.StorageNodeID(i),
			}

			err := reportCollector.RegisterStorageNode(sn, types.InvalidGLSN)
			if err != nil {
				t.Fatal(err)
			}

			So(testutil.CompareWait(func() bool {
				return a.lookupClient(sn.StorageNodeID) != nil
			}, 100*time.Millisecond), ShouldBeTrue)
		}

		Convey("ReportCollector should broadcast commit result to registered storage node", func() {
			gls := newDummyGlobalLogStream(knownHWM, nrStorage)
			mr.appendGLS(gls)
			knownHWM = gls.HighWatermark

			reportCollector.Commit(gls)

			a.m.Range(func(k, v interface{}) bool {
				cli := v.(*DummyReporterClient)
				So(testutil.CompareWait(func() bool {
					reportCollector.Commit(gls)

					cli.mu.Lock()
					defer cli.mu.Unlock()

					return cli.knownHighWatermark == knownHWM
				}, time.Second), ShouldBeTrue)
				return true
			})

			So(testutil.CompareWait(func() bool {
				return reportCollector.getMinHighWatermark() == knownHWM
			}, time.Second), ShouldBeTrue)

			Convey("ReportCollector should send ordered commit result to registered storage node", func() {
				gls := newDummyGlobalLogStream(knownHWM, nrStorage)
				mr.appendGLS(gls)
				knownHWM = gls.HighWatermark

				gls = newDummyGlobalLogStream(knownHWM, nrStorage)
				mr.appendGLS(gls)
				knownHWM = gls.HighWatermark

				reportCollector.Commit(gls)

				a.m.Range(func(k, v interface{}) bool {
					cli := v.(*DummyReporterClient)
					So(testutil.CompareWait(func() bool {
						reportCollector.Commit(gls)

						cli.mu.Lock()
						defer cli.mu.Unlock()

						return cli.knownHighWatermark == knownHWM
					}, time.Second), ShouldBeTrue)
					return true
				})

				So(testutil.CompareWait(func() bool {
					return reportCollector.getMinHighWatermark() == knownHWM
				}, time.Second), ShouldBeTrue)

				Convey("ReportCollector should send proper commit against new StorageNode", func() {
					mr.trimGLS(knownHWM)

					sn := &varlogpb.StorageNodeDescriptor{
						StorageNodeID: types.StorageNodeID(nrStorage),
					}

					err := reportCollector.RegisterStorageNode(sn, knownHWM)
					if err != nil {
						t.Fatal(err)
					}

					gls := newDummyGlobalLogStream(knownHWM, nrStorage+1)
					mr.appendGLS(gls)
					knownHWM = gls.HighWatermark

					reportCollector.Commit(gls)

					a.m.Range(func(k, v interface{}) bool {
						cli := v.(*DummyReporterClient)
						So(testutil.CompareWait(func() bool {
							reportCollector.Commit(gls)

							cli.mu.Lock()
							defer cli.mu.Unlock()

							return cli.knownHighWatermark == knownHWM
						}, time.Second), ShouldBeTrue)
						return true
					})
				})
			})
		})
	})
}

func TestCatchupRace(t *testing.T) {
	Convey("Given reportCollectExecutor", t, func(ctx C) {
		nrStorage := 5
		knownHWM := types.InvalidGLSN

		mr := NewDummyMetadataRepository()
		clientFac := NewDummyReporterClientFactory(false)
		cb := ReportCollectorCallbacks{
			report:        mr.report,
			getClient:     clientFac.GetClient,
			lookupNextGLS: mr.lookupNextGLS,
		}

		sn := &varlogpb.StorageNodeDescriptor{
			StorageNodeID: types.StorageNodeID(0),
		}

		logger, _ := zap.NewDevelopment()

		executor := &ReportCollectExecutor{
			resultC:       make(chan *snpb.GlobalLogStreamDescriptor, 1),
			highWatermark: knownHWM,
			sn:            sn,
			cb:            cb,
			logger:        logger,
		}

		Convey("When HWM Update during commit", func(ctx C) {
			gls := newDummyGlobalLogStream(knownHWM, nrStorage)
			mr.appendGLS(gls)
			knownHWM = gls.HighWatermark

			gls = newDummyGlobalLogStream(knownHWM, nrStorage)
			mr.appendGLS(gls)
			knownHWM = gls.HighWatermark

			executor.lsmu.Lock()
			executor.highWatermark = knownHWM
			executor.lsmu.Unlock()

			mr.trimGLS(knownHWM)

			gls = newDummyGlobalLogStream(knownHWM, nrStorage)
			mr.appendGLS(gls)
			knownHWM = gls.HighWatermark

			Convey("Then catchup should not panic", func(ctx C) {
				err := executor.catchup(types.InvalidGLSN, gls)
				So(err, ShouldBeNil)
			})
		})
	})
}

func TestRPCFail(t *testing.T) {
	Convey("Given ReportCollector", t, func(ctx C) {
		//knownHWM := types.InvalidGLSN

		clientFac := NewDummyReporterClientFactory(false)
		mr := NewDummyMetadataRepository()
		cb := ReportCollectorCallbacks{
			report:        mr.report,
			getClient:     clientFac.GetClient,
			lookupNextGLS: mr.lookupNextGLS,
		}

		logger, _ := zap.NewDevelopment()
		reportCollector := NewReportCollector(cb, logger)
		reportCollector.Run()
		Reset(func() {
			reportCollector.Close()
		})

		sn := &varlogpb.StorageNodeDescriptor{
			StorageNodeID: types.StorageNodeID(0),
		}

		err := reportCollector.RegisterStorageNode(sn, types.InvalidGLSN)
		So(err, ShouldBeNil)

		So(testutil.CompareWait(func() bool {
			return clientFac.lookupClient(sn.StorageNodeID) != nil
		}, 100*time.Millisecond), ShouldBeTrue)

		Convey("When reporter is crashed", func(ctx C) {
			clientFac.crashRPC(sn.StorageNodeID)

			// clear reportC
			nrReport := len(mr.reportC)
			for i := 0; i < nrReport; i++ {
				<-mr.reportC
			}

			select {
			case <-mr.reportC:
			case <-time.After(100 * time.Millisecond):
			}

			Convey("reportCollector should not callback report", func(ctx C) {
				So(testutil.CompareWait(func() bool {
					select {
					case <-mr.reportC:
						return false
					case <-time.After(100 * time.Millisecond):
						return true
					}
				}, 100*time.Millisecond), ShouldBeTrue)
			})

			Convey("When repoter recover", func(ctx C) {
				clientFac.recoverRPC(sn.StorageNodeID)

				Convey("reportCollector should callback report", func(ctx C) {
					So(testutil.CompareWait(func() bool {
						select {
						case <-mr.reportC:
							return true
						default:
							return false
						}
					}, 100*time.Millisecond), ShouldBeTrue)
				})
			})
		})
	})
}

func TestReporterClientReconnect(t *testing.T) {
	Convey("Given Reporter Client", t, func(ctx C) {
		clientFac := NewDummyReporterClientFactory(false)
		cb := ReportCollectorCallbacks{
			getClient: clientFac.GetClient,
		}

		sn := &varlogpb.StorageNodeDescriptor{
			StorageNodeID: types.StorageNodeID(0),
		}

		logger, _ := zap.NewDevelopment()

		executor := &ReportCollectExecutor{
			resultC:       make(chan *snpb.GlobalLogStreamDescriptor, 1),
			highWatermark: types.MinGLSN,
			sn:            sn,
			cb:            cb,
			logger:        logger,
		}

		cli := make([]storage.LogStreamReporterClient, 2)
		for i := 0; i < 2; i++ {
			var err error

			cli[i], err = executor.getClient()
			So(err, ShouldBeNil)
		}

		So(cli[0], ShouldEqual, cli[1])

		Convey("When cli[0] reconnect", func(ctx C) {
			var err error

			executor.closeClient(cli[0])
			cli[0], err = executor.getClient()
			So(err, ShouldBeNil)
			So(cli[0], ShouldNotEqual, cli[1])

			_, err = cli[0].GetReport(context.TODO())
			So(err, ShouldBeNil)

			_, err = cli[1].GetReport(context.TODO())
			So(err, ShouldNotBeNil)

			Convey("Then closeClient(cli[1]) should not closed the client", func(ctx C) {
				executor.closeClient(cli[1])

				_, err = cli[0].GetReport(context.TODO())
				So(err, ShouldBeNil)

				cli[1], err = executor.getClient()
				So(err, ShouldBeNil)
				So(cli[0], ShouldEqual, cli[1])

				_, err = cli[0].GetReport(context.TODO())
				So(err, ShouldBeNil)
			})
		})
	})
}
