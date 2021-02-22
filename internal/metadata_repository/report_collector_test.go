package metadata_repository

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
	"go.uber.org/zap"

	"github.daumkakao.com/varlog/varlog/internal/storagenode"
	"github.daumkakao.com/varlog/varlog/pkg/types"
	"github.daumkakao.com/varlog/varlog/pkg/util/runner"
	"github.daumkakao.com/varlog/varlog/pkg/util/testutil"
	"github.daumkakao.com/varlog/varlog/pkg/verrors"
	"github.daumkakao.com/varlog/varlog/proto/mrpb"
	"github.daumkakao.com/varlog/varlog/proto/snpb"
	"github.daumkakao.com/varlog/varlog/proto/varlogpb"
	"github.daumkakao.com/varlog/varlog/vtesting"
)

type dummyMetadataRepository struct {
	reportC        chan *mrpb.StorageNodeUncommitReport
	m              []*mrpb.LogStreamCommitResults
	reporterCliFac ReporterClientFactory
	mt             sync.Mutex
}

func NewDummyMetadataRepository(reporterCliFac ReporterClientFactory) *dummyMetadataRepository {
	return &dummyMetadataRepository{
		reportC:        make(chan *mrpb.StorageNodeUncommitReport, 4096),
		reporterCliFac: reporterCliFac,
	}
}

func (mr *dummyMetadataRepository) GetClient(ctx context.Context, sn *varlogpb.StorageNodeDescriptor) (storagenode.LogStreamReporterClient, error) {
	return mr.reporterCliFac.GetClient(ctx, sn)
}

func (mr *dummyMetadataRepository) ProposeReport(snID types.StorageNodeID, ur []*snpb.LogStreamUncommitReport) error {
	r := &mrpb.StorageNodeUncommitReport{
		StorageNodeID:   snID,
		UncommitReports: ur,
	}
	select {
	case mr.reportC <- r:
	default:
		return verrors.ErrIgnore
	}

	return nil
}

func (mr *dummyMetadataRepository) LookupCommitResults(glsn types.GLSN) *mrpb.LogStreamCommitResults {
	mr.mt.Lock()
	defer mr.mt.Unlock()

	i := sort.Search(len(mr.m), func(i int) bool {
		return mr.m[i].HighWatermark >= glsn
	})

	if i < len(mr.m) && mr.m[i].HighWatermark == glsn {
		return mr.m[i]
	}

	return nil
}

func (mr *dummyMetadataRepository) LookupNextCommitResults(glsn types.GLSN) *mrpb.LogStreamCommitResults {
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

func (mr *dummyMetadataRepository) appendGLS(gls *mrpb.LogStreamCommitResults) {
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
			if i > 0 {
				mr.m = mr.m[i-1:]
				return
			}
		}
	}
}

func TestRegisterStorageNode(t *testing.T) {
	Convey("Registering nil storage node should return an error", t, func() {
		a := NewDummyReporterClientFactory(1, false)
		mr := NewDummyMetadataRepository(a)

		logger, _ := zap.NewDevelopment()
		reportCollector := NewReportCollector(mr, DefaultRPCTimeout, logger)
		reportCollector.Run()
		defer reportCollector.Close()

		err := reportCollector.RegisterStorageNode(nil, types.InvalidGLSN)
		So(err, ShouldNotBeNil)
	})

	Convey("Registering dup storage node should return an error", t, func() {
		a := NewDummyReporterClientFactory(1, false)
		mr := NewDummyMetadataRepository(a)

		logger, _ := zap.NewDevelopment()
		reportCollector := NewReportCollector(mr, DefaultRPCTimeout, logger)
		reportCollector.Run()
		defer reportCollector.Close()

		sn := &varlogpb.StorageNodeDescriptor{
			StorageNodeID: types.StorageNodeID(time.Now().UnixNano()),
		}

		err := reportCollector.RegisterStorageNode(sn, types.InvalidGLSN)
		So(err, ShouldBeNil)

		reportCollector.mu.RLock()
		_, ok := reportCollector.executors[sn.StorageNodeID]
		reportCollector.mu.RUnlock()

		So(ok, ShouldBeTrue)

		err = reportCollector.RegisterStorageNode(sn, types.InvalidGLSN)
		So(err, ShouldNotBeNil)
	})
}

func TestRegisterLogStream(t *testing.T) {
	Convey("Register LogStream", t, func() {
		a := NewDummyReporterClientFactory(1, false)
		mr := NewDummyMetadataRepository(a)

		logger, _ := zap.NewDevelopment()
		reportCollector := NewReportCollector(mr, DefaultRPCTimeout, logger)
		reportCollector.Run()
		defer reportCollector.Close()

		snID := types.StorageNodeID(0)
		lsID := types.LogStreamID(0)

		Convey("registeration LogStream with not existing storageNodeID should be failed", func() {
			err := reportCollector.RegisterLogStream(snID, lsID)
			So(err, ShouldResemble, verrors.ErrNotExist)
		})

		Convey("registeration LogStream with existing storageNodeID should be succeed", func() {
			sn := &varlogpb.StorageNodeDescriptor{
				StorageNodeID: snID,
			}

			err := reportCollector.RegisterStorageNode(sn, types.InvalidGLSN)
			So(err, ShouldBeNil)
			So(reportCollector.NumExecutors(), ShouldEqual, 1)

			err = reportCollector.RegisterLogStream(snID, lsID)
			So(err, ShouldBeNil)
			So(reportCollector.NumCommitter(), ShouldEqual, 1)

			Convey("duplicated registeration LogStream should be failed", func() {
				err = reportCollector.RegisterLogStream(snID, lsID)
				So(err, ShouldResemble, verrors.ErrExist)
			})
		})
	})
}

func TestUnregisterStorageNode(t *testing.T) {
	Convey("Unregister StorageNode", t, func() {
		a := NewDummyReporterClientFactory(1, false)
		mr := NewDummyMetadataRepository(a)

		logger, _ := zap.NewDevelopment()
		reportCollector := NewReportCollector(mr, DefaultRPCTimeout, logger)
		reportCollector.Run()
		defer reportCollector.Close()

		snID := types.StorageNodeID(time.Now().UnixNano())
		lsID := types.LogStreamID(0)

		sn := &varlogpb.StorageNodeDescriptor{
			StorageNodeID: snID,
		}

		err := reportCollector.RegisterStorageNode(sn, types.InvalidGLSN)
		So(err, ShouldBeNil)
		So(reportCollector.NumExecutors(), ShouldEqual, 1)

		Convey("unregisteration storageNode should be succeed", func() {
			err := reportCollector.UnregisterStorageNode(snID)
			So(err, ShouldBeNil)

			So(reportCollector.NumExecutors(), ShouldEqual, 0)
		})

		Convey("unregisteration storageNode with logstream should be failed", func() {
			err = reportCollector.RegisterLogStream(snID, lsID)
			So(err, ShouldBeNil)
			So(reportCollector.NumCommitter(), ShouldEqual, 1)

			err := reportCollector.UnregisterStorageNode(snID)
			So(err, ShouldResemble, verrors.ErrNotEmpty)

			So(reportCollector.NumExecutors(), ShouldEqual, 1)
			So(reportCollector.NumCommitter(), ShouldEqual, 1)

			Convey("unregisteration storageNode with empty should be succeed", func() {
				err := reportCollector.UnregisterLogStream(snID, lsID)
				So(err, ShouldBeNil)
				So(reportCollector.NumCommitter(), ShouldEqual, 0)

				err = reportCollector.UnregisterStorageNode(snID)
				So(err, ShouldBeNil)

				So(reportCollector.NumExecutors(), ShouldEqual, 0)
			})
		})
	})
}

func TestUnregisterLogStream(t *testing.T) {
	Convey("Register LogStream", t, func() {
		a := NewDummyReporterClientFactory(1, false)
		mr := NewDummyMetadataRepository(a)

		logger, _ := zap.NewDevelopment()
		reportCollector := NewReportCollector(mr, DefaultRPCTimeout, logger)
		reportCollector.Run()
		defer reportCollector.Close()

		snID := types.StorageNodeID(0)
		lsID := types.LogStreamID(0)

		Convey("unregisteration LogStream with not existing storageNodeID should be failed", func() {
			err := reportCollector.UnregisterLogStream(snID, lsID)
			So(err, ShouldResemble, verrors.ErrNotExist)
		})

		Convey("unregisteration LogStream with existing storageNodeID should be succeed", func() {
			sn := &varlogpb.StorageNodeDescriptor{
				StorageNodeID: snID,
			}

			err := reportCollector.RegisterStorageNode(sn, types.InvalidGLSN)
			So(err, ShouldBeNil)
			So(reportCollector.NumExecutors(), ShouldEqual, 1)

			err = reportCollector.RegisterLogStream(snID, lsID)
			So(err, ShouldBeNil)
			So(reportCollector.NumCommitter(), ShouldEqual, 1)

			err = reportCollector.UnregisterLogStream(snID, lsID)
			So(err, ShouldBeNil)

			So(reportCollector.NumCommitter(), ShouldEqual, 0)
		})
	})
}

func TestRecoverStorageNode(t *testing.T) {
	Convey("Given ReportCollector", t, func() {
		a := NewDummyReporterClientFactory(1, false)
		mr := NewDummyMetadataRepository(a)

		logger, _ := zap.NewDevelopment()
		reportCollector := NewReportCollector(mr, DefaultRPCTimeout, logger)
		reportCollector.Run()
		defer reportCollector.Close()

		nrSN := 5
		hwm := types.MinGLSN
		var SNs []*varlogpb.StorageNodeDescriptor
		var LSs []*varlogpb.LogStreamDescriptor

		for i := 0; i < nrSN; i++ {
			sn := &varlogpb.StorageNodeDescriptor{
				StorageNodeID: types.StorageNodeID(time.Now().UnixNano()),
			}

			SNs = append(SNs, sn)

			err := reportCollector.RegisterStorageNode(sn, hwm)
			So(err, ShouldBeNil)

			ls := &varlogpb.LogStreamDescriptor{
				LogStreamID: types.LogStreamID(time.Now().UnixNano()),
			}
			ls.Replicas = append(ls.Replicas, &varlogpb.ReplicaDescriptor{StorageNodeID: sn.StorageNodeID})

			LSs = append(LSs, ls)

			err = reportCollector.RegisterLogStream(sn.StorageNodeID, ls.LogStreamID)
			So(err, ShouldBeNil)
		}

		for i := 0; i < nrSN; i++ {
			reportCollector.mu.RLock()
			executor, ok := reportCollector.executors[SNs[i].StorageNodeID]
			nrCommitter := len(executor.committers)
			reportCollector.mu.RUnlock()

			So(ok, ShouldBeTrue)
			So(nrCommitter, ShouldEqual, 1)
		}

		Convey("When ReportCollector Reset", func(ctx C) {
			reportCollector.Reset()

			Convey("Then there should be no ReportCollectExecutor", func(ctx C) {
				for i := 0; i < nrSN; i++ {
					reportCollector.mu.RLock()
					_, ok := reportCollector.executors[SNs[i].StorageNodeID]
					reportCollector.mu.RUnlock()

					So(ok, ShouldBeFalse)
				}

				Convey("When ReportCollector Recover", func(ctx C) {
					reportCollector.Recover(SNs, LSs, hwm)
					Convey("Then there should be ReportCollectExecutor", func(ctx C) {
						for i := 0; i < nrSN; i++ {
							reportCollector.mu.RLock()
							executor, ok := reportCollector.executors[SNs[i].StorageNodeID]
							nrCommitter := len(executor.committers)
							reportCollector.mu.RUnlock()

							So(ok, ShouldBeTrue)
							So(nrCommitter, ShouldEqual, 1)
						}
					})
				})
			})
		})

		Convey("When ReportCollector Close", func(ctx C) {
			reportCollector.Close()

			Convey("Then there should be no ReportCollectExecutor", func(ctx C) {
				for i := 0; i < nrSN; i++ {
					reportCollector.mu.RLock()
					_, ok := reportCollector.executors[SNs[i].StorageNodeID]
					reportCollector.mu.RUnlock()

					So(ok, ShouldBeFalse)
				}

				Convey("When ReportCollector Recover", func(ctx C) {
					reportCollector.Recover(SNs, LSs, hwm)
					Convey("Then there should be no ReportCollectExecutor", func(ctx C) {
						for i := 0; i < nrSN; i++ {
							reportCollector.mu.RLock()
							_, ok := reportCollector.executors[SNs[i].StorageNodeID]
							reportCollector.mu.RUnlock()

							So(ok, ShouldBeFalse)
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
		a := NewDummyReporterClientFactory(1, false)
		mr := NewDummyMetadataRepository(a)

		logger, _ := zap.NewDevelopment()
		reportCollector := NewReportCollector(mr, DefaultRPCTimeout, logger)
		reportCollector.Run()
		defer reportCollector.Close()

		var wg sync.WaitGroup
		wg.Add(1)
		go func(nrStorage int) {
			defer wg.Done()
			m := make(map[types.StorageNodeID]int)

			after := time.After(vtesting.TimeoutUnitTimesFactor(10))

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

func TestReportDedup(t *testing.T) {
	Convey("Given ReportCollector", t, func() {
		a := NewDummyReporterClientFactory(3, true)
		mr := NewDummyMetadataRepository(a)

		logger, _ := zap.NewDevelopment()
		reportCollector := NewReportCollector(mr, DefaultRPCTimeout, logger)
		reportCollector.Run()
		defer reportCollector.Close()

		sn := &varlogpb.StorageNodeDescriptor{
			StorageNodeID: types.StorageNodeID(0),
		}

		err := reportCollector.RegisterStorageNode(sn, types.InvalidGLSN)
		So(err, ShouldBeNil)

		r := <-mr.reportC
		So(r.Len(), ShouldEqual, 3)

		Convey("When logStream[0] increase uncommitted", func() {
			reporterClient := a.lookupClient(sn.StorageNodeID)
			reporterClient.increaseUncommitted(0)

			Convey("Then report should include logStream[0]", func() {
				r = <-mr.reportC
				So(r.Len(), ShouldEqual, 1)
				So(r.UncommitReports[0].LogStreamID, ShouldEqual, types.LogStreamID(0))

				Convey("When logStream[1] increase uncommitted", func() {
					reporterClient.increaseUncommitted(1)

					Convey("Then report should include logStream[1]", func() {
						r = <-mr.reportC
						So(r.Len(), ShouldEqual, 1)
						So(r.UncommitReports[0].LogStreamID, ShouldEqual, types.LogStreamID(1))

						Convey("When logStream[2] increase uncommitted", func() {
							reporterClient.increaseUncommitted(2)

							Convey("Then report should include logStream[2]", func() {
								r = <-mr.reportC
								So(r.Len(), ShouldEqual, 1)
								So(r.UncommitReports[0].LogStreamID, ShouldEqual, types.LogStreamID(2))

								Convey("After reportAll interval, report should include all", func() {
									r = <-mr.reportC
									So(r.Len(), ShouldEqual, 3)
								})
							})
						})
					})
				})
			})
		})
	})
}

func newDummyCommitResults(prev types.GLSN, nrLogStream int) *mrpb.LogStreamCommitResults {
	cr := &mrpb.LogStreamCommitResults{
		HighWatermark:     prev + types.GLSN(nrLogStream),
		PrevHighWatermark: prev,
	}
	glsn := prev + types.GLSN(1)

	for i := 0; i < nrLogStream; i++ {
		r := &snpb.LogStreamCommitResult{
			LogStreamID:         types.LogStreamID(i),
			CommittedGLSNOffset: glsn,
			CommittedGLSNLength: 1,
		}
		glsn += 1

		cr.CommitResults = append(cr.CommitResults, r)
	}

	return cr
}

func TestCommit(t *testing.T) {
	Convey("Given ReportCollector", t, func() {
		nrStorage := 5
		nrLogStream := nrStorage
		knownHWM := types.InvalidGLSN

		a := NewDummyReporterClientFactory(1, false)
		mr := NewDummyMetadataRepository(a)

		logger, _ := zap.NewDevelopment()
		reportCollector := NewReportCollector(mr, DefaultRPCTimeout, logger)
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

			So(testutil.CompareWaitN(1, func() bool {
				return a.lookupClient(sn.StorageNodeID) != nil
			}), ShouldBeTrue)
		}

		for i := 0; i < nrLogStream; i++ {
			err := reportCollector.RegisterLogStream(types.StorageNodeID(i%nrStorage), types.LogStreamID(i))
			if err != nil {
				t.Fatal(err)
			}
		}

		Convey("ReportCollector should broadcast commit result to registered storage node", func() {
			gls := newDummyCommitResults(knownHWM, nrStorage)
			mr.appendGLS(gls)
			knownHWM = gls.HighWatermark

			reportCollector.Commit()

			a.m.Range(func(k, v interface{}) bool {
				cli := v.(*DummyReporterClient)
				So(testutil.CompareWaitN(10, func() bool {
					reportCollector.Commit()

					return cli.getKnownHighWatermark(0) == knownHWM
				}), ShouldBeTrue)
				return true
			})

			Convey("ReportCollector should send ordered commit result to registered storage node", func() {
				gls := newDummyCommitResults(knownHWM, nrStorage)
				mr.appendGLS(gls)
				knownHWM = gls.HighWatermark

				gls = newDummyCommitResults(knownHWM, nrStorage)
				mr.appendGLS(gls)
				knownHWM = gls.HighWatermark

				reportCollector.Commit()

				a.m.Range(func(k, v interface{}) bool {
					cli := v.(*DummyReporterClient)
					So(testutil.CompareWaitN(10, func() bool {
						reportCollector.Commit()

						return cli.getKnownHighWatermark(0) == knownHWM
					}), ShouldBeTrue)
					return true
				})

				trimHWM := types.MaxGLSN
				reportCollector.mu.RLock()
				for _, executor := range reportCollector.executors {
					reports := executor.reportCtx.getReport()
					for _, report := range reports.UncommitReports {
						if !report.HighWatermark.Invalid() && report.HighWatermark < trimHWM {
							trimHWM = report.HighWatermark
						}
					}
				}
				reportCollector.mu.RUnlock()

				fmt.Printf("knowHWM:%v, trim:%v\n", knownHWM, trimHWM)

				mr.trimGLS(trimHWM)

				Convey("ReportCollector should send proper commit against new StorageNode", func() {
					sn := &varlogpb.StorageNodeDescriptor{
						StorageNodeID: types.StorageNodeID(nrStorage),
					}

					err := reportCollector.RegisterStorageNode(sn, knownHWM)
					So(err, ShouldBeNil)

					nrStorage += 1

					err = reportCollector.RegisterLogStream(sn.StorageNodeID, types.LogStreamID(nrLogStream))
					So(err, ShouldBeNil)

					nrLogStream += 1

					gls := newDummyCommitResults(knownHWM, nrStorage)
					mr.appendGLS(gls)
					knownHWM = gls.HighWatermark

					So(testutil.CompareWaitN(10, func() bool {
						nrCli := 0
						a.m.Range(func(k, v interface{}) bool {
							cli := v.(*DummyReporterClient)
							So(testutil.CompareWaitN(10, func() bool {
								reportCollector.Commit()

								return cli.getKnownHighWatermark(0) == knownHWM
							}), ShouldBeTrue)
							nrCli++
							return true
						})

						return nrCli == nrStorage
					}), ShouldBeTrue)
				})
			})
		})
	})
}

func TestCommitWithDelay(t *testing.T) {
	Convey("Given ReportCollector", t, func() {
		knownHWM := types.InvalidGLSN

		a := NewDummyReporterClientFactory(1, false)
		mr := NewDummyMetadataRepository(a)

		logger, _ := zap.NewDevelopment()
		reportCollector := NewReportCollector(mr, time.Second, logger)
		reportCollector.Run()
		Reset(func() {
			reportCollector.Close()
		})

		sn := &varlogpb.StorageNodeDescriptor{
			StorageNodeID: types.StorageNodeID(0),
		}

		err := reportCollector.RegisterStorageNode(sn, types.InvalidGLSN)
		if err != nil {
			t.Fatal(err)
		}

		So(testutil.CompareWaitN(1, func() bool {
			return a.lookupClient(sn.StorageNodeID) != nil
		}), ShouldBeTrue)

		err = reportCollector.RegisterLogStream(types.StorageNodeID(0), types.LogStreamID(0))
		if err != nil {
			t.Fatal(err)
		}

		reportCollector.mu.RLock()
		executor, ok := reportCollector.executors[sn.StorageNodeID]
		reportCollector.mu.RUnlock()

		So(ok, ShouldBeTrue)

		// check report
		So(testutil.CompareWaitN(10, func() bool {
			return executor.reportCtx.getReport() != nil
		}), ShouldBeTrue)

		dummySN := a.lookupClient(sn.StorageNodeID)

		Convey("disable report to catchup using old hwm", func() {
			dummySN.DisableReport()

			gls := newDummyCommitResults(knownHWM, 1)
			mr.appendGLS(gls)
			prevHWM := knownHWM
			knownHWM = gls.HighWatermark

			gls = newDummyCommitResults(knownHWM, 1)
			mr.appendGLS(gls)
			prevHWM = knownHWM
			knownHWM = gls.HighWatermark

			gls = newDummyCommitResults(knownHWM, 1)
			mr.appendGLS(gls)
			prevHWM = knownHWM
			knownHWM = gls.HighWatermark

			reportCollector.Commit()

			So(testutil.CompareWaitN(10, func() bool {
				return dummySN.getKnownHighWatermark(0) == knownHWM
			}), ShouldBeTrue)

			So(executor.reportCtx.getReport().UncommitReports[0].HighWatermark, ShouldBeLessThan, prevHWM)

			Convey("set commit delay & enable report to trim during catchup", func() {
				dummySN.SetCommitDelay(30 * time.Millisecond)
				reportCollector.Commit()

				time.Sleep(10 * time.Millisecond)
				dummySN.EnableReport()

				So(testutil.CompareWaitN(10, func() bool {
					reports := executor.reportCtx.getReport()
					return reports.UncommitReports[0].HighWatermark == knownHWM
				}), ShouldBeTrue)

				mr.trimGLS(knownHWM)

				gls = newDummyCommitResults(knownHWM, 1)
				mr.appendGLS(gls)
				knownHWM = gls.HighWatermark

				Convey("then it should catchup", func() {
					reportCollector.Commit()

					So(testutil.CompareWaitN(10, func() bool {
						reports := executor.reportCtx.getReport()
						return reports.UncommitReports[0].HighWatermark == knownHWM
					}), ShouldBeTrue)
				})
			})
		})
	})
}

func TestRPCFail(t *testing.T) {
	Convey("Given ReportCollector", t, func(ctx C) {
		//knownHWM := types.InvalidGLSN

		clientFac := NewDummyReporterClientFactory(1, false)
		mr := NewDummyMetadataRepository(clientFac)

		logger, _ := zap.NewDevelopment()
		reportCollector := NewReportCollector(mr, DefaultRPCTimeout, logger)
		reportCollector.Run()
		Reset(func() {
			reportCollector.Close()
		})

		sn := &varlogpb.StorageNodeDescriptor{
			StorageNodeID: types.StorageNodeID(0),
		}

		err := reportCollector.RegisterStorageNode(sn, types.InvalidGLSN)
		So(err, ShouldBeNil)

		So(testutil.CompareWaitN(1, func() bool {
			return clientFac.lookupClient(sn.StorageNodeID) != nil
		}), ShouldBeTrue)

		Convey("When reporter is crashed", func(ctx C) {
			clientFac.crashRPC(sn.StorageNodeID)

			// clear reportC
			nrReport := len(mr.reportC)
			for i := 0; i < nrReport; i++ {
				<-mr.reportC
			}

			select {
			case <-mr.reportC:
			case <-time.After(vtesting.TimeoutUnitTimesFactor(1)):
			}

			Convey("reportCollector should not callback report", func(ctx C) {
				So(testutil.CompareWaitN(1, func() bool {
					select {
					case <-mr.reportC:
						return false
					case <-time.After(vtesting.TimeoutUnitTimesFactor(1)):
						return true
					}
				}), ShouldBeTrue)
			})

			Convey("When repoter recover", func(ctx C) {
				clientFac.recoverRPC(sn.StorageNodeID)

				Convey("reportCollector should callback report", func(ctx C) {
					So(testutil.CompareWaitN(1, func() bool {
						select {
						case <-mr.reportC:
							return true
						default:
							return false
						}
					}), ShouldBeTrue)
				})
			})
		})
	})
}

func TestReporterClientReconnect(t *testing.T) {
	Convey("Given Reporter Client", t, func(ctx C) {
		clientFac := NewDummyReporterClientFactory(1, false)
		mr := NewDummyMetadataRepository(clientFac)

		sn := &varlogpb.StorageNodeDescriptor{
			StorageNodeID: types.StorageNodeID(0),
		}

		logger, _ := zap.NewDevelopment()

		executor := &reportCollectExecutor{
			storageNodeID: sn.StorageNodeID,
			helper:        mr,
			snConnector:   storageNodeConnector{sn: sn},
			reportCtx:     &reportContext{},
			committers:    make(map[types.LogStreamID]*logStreamCommitter),
			runner:        runner.New("excutor", logger),
			logger:        logger,
		}

		cli := make([]storagenode.LogStreamReporterClient, 2)
		for i := 0; i < 2; i++ {
			var err error

			cli[i], err = executor.getClient(context.TODO())
			So(err, ShouldBeNil)
		}

		So(cli[0], ShouldEqual, cli[1])

		Convey("When cli[0] reconnect", func(ctx C) {
			var err error

			executor.closeClient(cli[0])
			cli[0], err = executor.getClient(context.TODO())
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

				cli[1], err = executor.getClient(context.TODO())
				So(err, ShouldBeNil)
				So(cli[0], ShouldEqual, cli[1])

				_, err = cli[0].GetReport(context.TODO())
				So(err, ShouldBeNil)
			})
		})
	})
}
