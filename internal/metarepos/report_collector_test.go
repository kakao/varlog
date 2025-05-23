package metarepos

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"

	"github.com/kakao/varlog/internal/reportcommitter"
	"github.com/kakao/varlog/internal/vtesting"
	"github.com/kakao/varlog/pkg/types"
	"github.com/kakao/varlog/pkg/util/runner"
	"github.com/kakao/varlog/pkg/util/testutil"
	"github.com/kakao/varlog/pkg/verrors"
	"github.com/kakao/varlog/proto/mrpb"
	"github.com/kakao/varlog/proto/snpb"
	"github.com/kakao/varlog/proto/varlogpb"
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

func (mr *dummyMetadataRepository) GetReporterClient(ctx context.Context, sn *varlogpb.StorageNodeDescriptor) (reportcommitter.Client, error) {
	return mr.reporterCliFac.GetReporterClient(ctx, sn)
}

func (mr *dummyMetadataRepository) ProposeReport(snID types.StorageNodeID, ur []snpb.LogStreamUncommitReport) error {
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

func (mr *dummyMetadataRepository) GetLastCommitResults() *mrpb.LogStreamCommitResults {
	mr.mt.Lock()
	defer mr.mt.Unlock()

	if len(mr.m) == 0 {
		return nil
	}

	return mr.m[len(mr.m)-1]
}

func (mr *dummyMetadataRepository) LookupNextCommitResults(ver types.Version) (*mrpb.LogStreamCommitResults, error) {
	mr.mt.Lock()
	defer mr.mt.Unlock()

	var err error
	if len(mr.m) == 0 {
		return nil, err
	}

	if mr.m[0].Version > ver+1 {
		err = fmt.Errorf("already trimmed ver:%v, oldest:%v", ver, mr.m[0].Version)
	}

	i := sort.Search(len(mr.m), func(i int) bool {
		return mr.m[i].Version >= ver+1
	})

	if i < len(mr.m) && mr.m[i].Version == ver+1 {
		return mr.m[i], err
	}

	return nil, err
}

func (mr *dummyMetadataRepository) appendGLS(gls *mrpb.LogStreamCommitResults) {
	mr.mt.Lock()
	defer mr.mt.Unlock()

	mr.m = append(mr.m, gls)
}

func (mr *dummyMetadataRepository) trimGLS(ver types.Version) {
	mr.mt.Lock()
	defer mr.mt.Unlock()

	for i, gls := range mr.m {
		if ver == gls.Version {
			if i > 0 {
				mr.m = mr.m[i-1:]
				return
			}
		}
	}
}

func TestRegisterStorageNode(t *testing.T) {
	Convey("Registering nil storage node should return an error", t, func() {
		a := NewDummyStorageNodeClientFactory(1, false)
		mr := NewDummyMetadataRepository(a)

		logger, _ := zap.NewDevelopment()
		reportCollector := NewReportCollector(mr, DefaultRPCTimeout, newNopTelmetryStub(), logger)
		reportCollector.Run() //nolint:errcheck,revive // TODO:: Handle an error returned.
		defer reportCollector.Close()

		err := reportCollector.RegisterStorageNode(nil)
		So(err, ShouldNotBeNil)
	})

	Convey("Registering dup storage node should return an error", t, func() {
		a := NewDummyStorageNodeClientFactory(1, false)
		mr := NewDummyMetadataRepository(a)

		logger, _ := zap.NewDevelopment()
		reportCollector := NewReportCollector(mr, DefaultRPCTimeout, newNopTelmetryStub(), logger)
		reportCollector.Run() //nolint:errcheck,revive // TODO:: Handle an error returned.
		defer reportCollector.Close()

		sn := &varlogpb.StorageNodeDescriptor{
			StorageNode: varlogpb.StorageNode{
				StorageNodeID: types.StorageNodeID(time.Now().UnixNano()),
			},
		}

		err := reportCollector.RegisterStorageNode(sn)
		So(err, ShouldBeNil)

		reportCollector.mu.RLock()
		ok := reportCollector.lookupExecutor(sn.StorageNodeID) != nil
		reportCollector.mu.RUnlock()

		So(ok, ShouldBeTrue)

		err = reportCollector.RegisterStorageNode(sn)
		So(err, ShouldNotBeNil)
	})
}

func TestRegisterLogStream(t *testing.T) {
	Convey("Register LogStream", t, func() {
		a := NewDummyStorageNodeClientFactory(1, false)
		mr := NewDummyMetadataRepository(a)

		logger, _ := zap.NewDevelopment()
		reportCollector := NewReportCollector(mr, DefaultRPCTimeout, newNopTelmetryStub(), logger)
		reportCollector.Run() //nolint:errcheck,revive // TODO:: Handle an error returned.
		defer reportCollector.Close()

		snID := types.MinStorageNodeID
		lsID := types.MinLogStreamID
		topicID := types.MinTopicID

		Convey("registeration LogStream with not existing storageNodeID should be failed", func() {
			err := reportCollector.RegisterLogStream(topicID, snID, lsID, types.InvalidVersion, varlogpb.LogStreamStatusRunning)
			So(err, ShouldResemble, verrors.ErrNotExist)
		})

		Convey("registeration LogStream with existing storageNodeID should be succeed", func() {
			sn := &varlogpb.StorageNodeDescriptor{
				StorageNode: varlogpb.StorageNode{
					StorageNodeID: snID,
				},
			}

			err := reportCollector.RegisterStorageNode(sn)
			So(err, ShouldBeNil)
			So(reportCollector.NumExecutors(), ShouldEqual, 1)

			err = reportCollector.RegisterLogStream(topicID, snID, lsID, types.InvalidVersion, varlogpb.LogStreamStatusRunning)
			So(err, ShouldBeNil)
			So(reportCollector.NumCommitter(), ShouldEqual, 1)

			Convey("duplicated registeration LogStream should be failed", func() {
				err = reportCollector.RegisterLogStream(topicID, snID, lsID, types.InvalidVersion, varlogpb.LogStreamStatusRunning)
				So(err, ShouldResemble, verrors.ErrExist)
			})
		})
	})
}

func TestUnregisterStorageNode(t *testing.T) {
	Convey("Unregister StorageNode", t, func() {
		a := NewDummyStorageNodeClientFactory(1, false)
		mr := NewDummyMetadataRepository(a)

		logger, _ := zap.NewDevelopment()
		reportCollector := NewReportCollector(mr, DefaultRPCTimeout, newNopTelmetryStub(), logger)
		reportCollector.Run() //nolint:errcheck,revive // TODO:: Handle an error returned.
		defer reportCollector.Close()

		snID := types.StorageNodeID(time.Now().UnixNano())
		lsID := types.MinLogStreamID
		topicID := types.MinTopicID

		sn := &varlogpb.StorageNodeDescriptor{
			StorageNode: varlogpb.StorageNode{
				StorageNodeID: snID,
			},
		}

		err := reportCollector.RegisterStorageNode(sn)
		So(err, ShouldBeNil)
		So(reportCollector.NumExecutors(), ShouldEqual, 1)

		Convey("unregisteration storageNode should be succeed", func() {
			err := reportCollector.UnregisterStorageNode(snID)
			So(err, ShouldBeNil)

			So(reportCollector.NumExecutors(), ShouldEqual, 0)
		})

		Convey("unregisteration storageNode with logstream should be failed", func() {
			err = reportCollector.RegisterLogStream(topicID, snID, lsID, types.InvalidVersion, varlogpb.LogStreamStatusRunning)
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
		a := NewDummyStorageNodeClientFactory(1, false)
		mr := NewDummyMetadataRepository(a)

		logger, _ := zap.NewDevelopment()
		reportCollector := NewReportCollector(mr, DefaultRPCTimeout, newNopTelmetryStub(), logger)
		reportCollector.Run() //nolint:errcheck,revive // TODO:: Handle an error returned.
		defer reportCollector.Close()

		snID := types.MinStorageNodeID
		lsID := types.MinLogStreamID
		topicID := types.MinTopicID

		Convey("unregisteration LogStream with not existing storageNodeID should be failed", func() {
			err := reportCollector.UnregisterLogStream(snID, lsID)
			So(err, ShouldResemble, verrors.ErrNotExist)
		})

		Convey("unregisteration LogStream with existing storageNodeID should be succeed", func() {
			sn := &varlogpb.StorageNodeDescriptor{
				StorageNode: varlogpb.StorageNode{
					StorageNodeID: snID,
				},
			}

			err := reportCollector.RegisterStorageNode(sn)
			So(err, ShouldBeNil)
			So(reportCollector.NumExecutors(), ShouldEqual, 1)

			err = reportCollector.RegisterLogStream(topicID, snID, lsID, types.InvalidVersion, varlogpb.LogStreamStatusRunning)
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
		a := NewDummyStorageNodeClientFactory(1, false)
		mr := NewDummyMetadataRepository(a)

		logger, _ := zap.NewDevelopment()
		reportCollector := NewReportCollector(mr, DefaultRPCTimeout, newNopTelmetryStub(), logger)
		reportCollector.Run() //nolint:errcheck,revive // TODO:: Handle an error returned.
		defer reportCollector.Close()

		nrSN := 5
		ver := types.MinVersion
		var SNs []*varlogpb.StorageNodeDescriptor
		var LSs []*varlogpb.LogStreamDescriptor
		var sealingLSID types.LogStreamID
		var sealedLSID types.LogStreamID
		var topicID types.TopicID

		for i := 0; i < nrSN; i++ {
			sn := &varlogpb.StorageNodeDescriptor{
				StorageNode: varlogpb.StorageNode{
					StorageNodeID: types.StorageNodeID(time.Now().UnixNano()),
				},
			}

			SNs = append(SNs, sn)

			err := reportCollector.RegisterStorageNode(sn)
			So(err, ShouldBeNil)

			ls := &varlogpb.LogStreamDescriptor{
				LogStreamID: types.LogStreamID(time.Now().UnixNano()),
			}

			if sealingLSID == types.LogStreamID(0) {
				sealingLSID = ls.LogStreamID
				ls.Status = varlogpb.LogStreamStatusSealing
			} else if sealedLSID == types.LogStreamID(0) {
				sealedLSID = ls.LogStreamID
				ls.Status = varlogpb.LogStreamStatusSealed
			}

			ls.Replicas = append(ls.Replicas, &varlogpb.ReplicaDescriptor{StorageNodeID: sn.StorageNodeID})

			LSs = append(LSs, ls)

			err = reportCollector.RegisterLogStream(topicID, sn.StorageNodeID, ls.LogStreamID, types.InvalidVersion, varlogpb.LogStreamStatusRunning)
			So(err, ShouldBeNil)
		}

		for i := 0; i < nrSN; i++ {
			reportCollector.mu.RLock()
			e := reportCollector.lookupExecutor(SNs[i].StorageNodeID)
			ok := e != nil
			nrCommitter := len(e.committers)
			reportCollector.mu.RUnlock()

			So(ok, ShouldBeTrue)
			So(nrCommitter, ShouldEqual, 1)
		}

		Convey("When ReportCollector Reset", func(ctx C) {
			reportCollector.Reset()

			Convey("Then there should be no ReportCollectExecutor", func(ctx C) {
				for i := 0; i < nrSN; i++ {
					reportCollector.mu.RLock()
					ok := reportCollector.lookupExecutor(SNs[i].StorageNodeID) != nil
					reportCollector.mu.RUnlock()

					So(ok, ShouldBeFalse)
				}

				Convey("When ReportCollector Recover", func(ctx C) {
					reportCollector.Recover(SNs, LSs, ver) //nolint:errcheck,revive // TODO:: Handle an error returned.
					Convey("Then there should be ReportCollectExecutor", func(ctx C) {
						sealing := false
						sealed := false
						for i := 0; i < nrSN; i++ {
							reportCollector.mu.RLock()
							executor := reportCollector.lookupExecutor(SNs[i].StorageNodeID)
							ok := executor != nil
							nrCommitter := len(executor.committers)

							executor.cmmu.RLock()

							if cm := executor.lookupCommitter(sealingLSID); cm != nil {
								status, _ := cm.getCommitStatus()
								sealing = status == varlogpb.LogStreamStatusRunning
							}

							if cm := executor.lookupCommitter(sealedLSID); cm != nil {
								status, _ := cm.getCommitStatus()
								sealed = status == varlogpb.LogStreamStatusSealed
							}

							executor.cmmu.RUnlock()
							reportCollector.mu.RUnlock()

							So(ok, ShouldBeTrue)
							So(nrCommitter, ShouldEqual, 1)
						}

						So(sealing, ShouldBeTrue)
						So(sealed, ShouldBeTrue)
					})
				})
			})
		})

		Convey("When ReportCollector Close", func(ctx C) {
			reportCollector.Close()

			Convey("Then there should be no ReportCollectExecutor", func(ctx C) {
				for i := 0; i < nrSN; i++ {
					reportCollector.mu.RLock()
					ok := reportCollector.lookupExecutor(SNs[i].StorageNodeID) != nil
					reportCollector.mu.RUnlock()

					So(ok, ShouldBeFalse)
				}

				Convey("When ReportCollector Recover", func(ctx C) {
					reportCollector.Recover(SNs, LSs, ver) //nolint:errcheck,revive // TODO:: Handle an error returned.
					Convey("Then there should be no ReportCollectExecutor", func(ctx C) {
						for i := 0; i < nrSN; i++ {
							reportCollector.mu.RLock()
							ok := reportCollector.lookupExecutor(SNs[i].StorageNodeID) != nil
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
		a := NewDummyStorageNodeClientFactory(1, false)
		mr := NewDummyMetadataRepository(a)

		logger, _ := zap.NewDevelopment()
		reportCollector := NewReportCollector(mr, DefaultRPCTimeout, newNopTelmetryStub(), logger)
		reportCollector.Run() //nolint:errcheck,revive // TODO:: Handle an error returned.
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
			snID := types.MinStorageNodeID + types.StorageNodeID(i)
			sn := &varlogpb.StorageNodeDescriptor{
				StorageNode: varlogpb.StorageNode{
					StorageNodeID: snID,
				},
			}

			err := reportCollector.RegisterStorageNode(sn)
			if err != nil {
				t.Fatal(err)
			}
		}

		wg.Wait()
	})
}

func TestReportIgnore(t *testing.T) {
	Convey("ReportCollector should collect report from registered storage node", t, func() {
		a := NewDummyStorageNodeClientFactory(1, true)
		mr := NewDummyMetadataRepository(a)

		logger, _ := zap.NewDevelopment()
		reportCollector := NewReportCollector(mr, DefaultRPCTimeout, newNopTelmetryStub(), logger)
		reportCollector.Run() //nolint:errcheck,revive // TODO:: Handle an error returned.
		defer reportCollector.Close()

		snID := types.MinStorageNodeID
		sn := &varlogpb.StorageNodeDescriptor{
			StorageNode: varlogpb.StorageNode{
				StorageNodeID: snID,
			},
		}

		err := reportCollector.RegisterStorageNode(sn)
		So(err, ShouldBeNil)

		<-mr.reportC

		reporterClient := a.lookupClient(sn.StorageNodeID)
		reporterClient.makeInvalid(0)

		r := <-mr.reportC
		for _, ur := range r.UncommitReports {
			So(ur.Invalid(), ShouldBeTrue)
		}

		after := time.After(DefaultReportRefreshTime / 2)

	Loop:
		for {
			select {
			case <-after:
				break Loop
			case r := <-mr.reportC:
				for _, ur := range r.UncommitReports {
					So(ur.LogStreamID.Invalid(), ShouldBeFalse)
					So(ur.Invalid(), ShouldBeFalse)
				}
			}
		}
	})
}

func TestReportDedup(t *testing.T) {
	Convey("Given ReportCollector", t, func() {
		a := NewDummyStorageNodeClientFactory(3, true)
		mr := NewDummyMetadataRepository(a)

		logger, _ := zap.NewDevelopment()
		reportCollector := NewReportCollector(mr, DefaultRPCTimeout, newNopTelmetryStub(), logger)
		reportCollector.Run() //nolint:errcheck,revive // TODO:: Handle an error returned.
		defer reportCollector.Close()

		sn := &varlogpb.StorageNodeDescriptor{
			StorageNode: varlogpb.StorageNode{
				StorageNodeID: types.MinStorageNodeID,
			},
		}

		err := reportCollector.RegisterStorageNode(sn)
		So(err, ShouldBeNil)

		r := <-mr.reportC
		So(r.Len(), ShouldEqual, 3)

		Convey("When logStream[0] increase uncommitted", func() {
			reporterClient := a.lookupClient(sn.StorageNodeID)
			reporterClient.increaseUncommitted(0)

			Convey("Then report should include logStream[1]", func() {
				r = <-mr.reportC
				So(r.Len(), ShouldEqual, 1)
				So(r.UncommitReports[0].LogStreamID, ShouldEqual, types.MinLogStreamID)

				Convey("When logStream[2] increase uncommitted", func() {
					reporterClient.increaseUncommitted(1)

					Convey("Then report should include logStream[2]", func() {
						r = <-mr.reportC
						So(r.Len(), ShouldEqual, 1)
						So(r.UncommitReports[0].LogStreamID, ShouldEqual, types.MinLogStreamID+types.LogStreamID(1))

						Convey("When logStream[3] increase uncommitted", func() {
							reporterClient.increaseUncommitted(2)

							Convey("Then report should include logStream[3]", func() {
								r = <-mr.reportC
								So(r.Len(), ShouldEqual, 1)
								So(r.UncommitReports[0].LogStreamID, ShouldEqual, types.MinLogStreamID+types.LogStreamID(2))

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

func TestReportHandleInvalidReport(t *testing.T) {
	a := NewDummyStorageNodeClientFactory(1, true)
	mr := NewDummyMetadataRepository(a)

	logger := zaptest.NewLogger(t, zaptest.Level(zap.DebugLevel))
	reportCollector := NewReportCollector(mr, DefaultRPCTimeout, newNopTelmetryStub(), logger)
	require.NoError(t, reportCollector.Run())
	defer reportCollector.Close()

	sn := &varlogpb.StorageNodeDescriptor{
		StorageNode: varlogpb.StorageNode{
			StorageNodeID: types.MinStorageNodeID,
		},
	}

	glsn := types.MinGLSN
	version := types.MinVersion

	require.NoError(t, reportCollector.RegisterStorageNode(sn))

	r := <-mr.reportC
	require.Equal(t, 1, r.Len())

	// When increase the uncommitted length of logstream,
	// report should reflect this.
	reporterClient := a.lookupClient(sn.StorageNodeID)
	reporterClient.increaseUncommitted(0)

	r = <-mr.reportC
	require.Equal(t, 1, r.Len())
	require.Equal(t, types.MinLogStreamID, r.UncommitReports[0].LogStreamID)
	require.Greater(t, r.UncommitReports[0].UncommittedLLSNLength, uint64(0))

	// report should reflect committed version.
	begin := glsn
	glsn = begin + types.GLSN(r.UncommitReports[0].UncommittedLLSNLength)
	version++

	cr := snpb.CommitBatchRequest{
		StorageNodeID: r.StorageNodeID,
		CommitResults: []snpb.LogStreamCommitResult{{
			LogStreamID:         r.UncommitReports[0].LogStreamID,
			CommittedLLSNOffset: r.UncommitReports[0].UncommittedLLSNOffset,
			CommittedGLSNOffset: begin,
			CommittedGLSNLength: r.UncommitReports[0].UncommittedLLSNLength,
			HighWatermark:       glsn,
			Version:             version,
		}},
	}

	require.NoError(t, reporterClient.CommitBatch(cr))

	r = <-mr.reportC
	require.Equal(t, 1, r.Len())
	require.Equal(t, types.MinLogStreamID, r.UncommitReports[0].LogStreamID)
	require.Equal(t, r.UncommitReports[0].Version, version)
	require.Equal(t, r.UncommitReports[0].UncommittedLLSNLength, uint64(0))

	// make report invalid and check if the report collector handles it property.
	reporterClient.setKnownVersion(0, version-1)

	r = <-mr.reportC
	require.Equal(t, 1, r.Len())
	for _, ur := range r.UncommitReports {
		require.NotEqual(t, version, ur.Version)
	}
}

func TestReportCollectorSeal(t *testing.T) {
	Convey("Given ReportCollector", t, func() {
		nrStorage := 5
		nrLogStream := nrStorage
		knownVer := types.InvalidVersion
		glsn := types.MinGLSN
		topicID := types.TopicID(0)

		a := NewDummyStorageNodeClientFactory(1, false)
		mr := NewDummyMetadataRepository(a)
		cc := newDummyCommitContext()

		logger := zap.NewNop()
		reportCollector := NewReportCollector(mr, DefaultRPCTimeout, newNopTelmetryStub(), logger)
		reportCollector.Run() //nolint:errcheck,revive // TODO:: Handle an error returned.
		Reset(func() {
			reportCollector.Close()
		})

		for i := 0; i < nrStorage; i++ {
			snID := types.MinStorageNodeID + types.StorageNodeID(i)
			sn := &varlogpb.StorageNodeDescriptor{
				StorageNode: varlogpb.StorageNode{
					StorageNodeID: snID,
				},
			}

			err := reportCollector.RegisterStorageNode(sn)
			if err != nil {
				t.Fatal(err)
			}

			So(testutil.CompareWaitN(1, func() bool {
				return a.lookupClient(sn.StorageNodeID) != nil
			}), ShouldBeTrue)
		}

		var sealedLSID types.LogStreamID

		for i := 0; i < nrLogStream; i++ {
			snID := types.MinStorageNodeID + types.StorageNodeID(i%nrStorage)
			lsID := types.MinLogStreamID + types.LogStreamID(i)
			err := reportCollector.RegisterLogStream(topicID, snID, lsID, types.InvalidVersion, varlogpb.LogStreamStatusRunning)
			if err != nil {
				t.Fatal(err)
			}

			sealedLSID = lsID
		}

		gls := cc.newDummyCommitResults(knownVer+1, glsn, nrStorage)
		mr.appendGLS(gls)
		knownVer = gls.Version
		glsn += types.GLSN(len(gls.CommitResults))

		So(testutil.CompareWaitN(10, func() bool {
			reportCollector.Commit()

			reportCollector.mu.RLock()
			defer reportCollector.mu.RUnlock()

			for _, executor := range reportCollector.executors {
				executor.cmmu.RLock()
				defer executor.cmmu.RUnlock()

				if reportedVer, ok := executor.getReportedVersion(sealedLSID); ok && reportedVer == knownVer {
					return true
				}
			}

			return false
		}), ShouldBeTrue)

		Convey("When ReportCollector Seal", func() {
			reportCollector.Seal(sealedLSID)
			cc.seal(sealedLSID)

			time.Sleep(time.Second)

			Convey("Then it should not commit", func() {
				gls = cc.newDummyCommitResults(knownVer+1, glsn, nrStorage)
				mr.appendGLS(gls)
				knownVer = gls.Version
				glsn += types.GLSN(len(gls.CommitResults))

				for i := 0; i < 10; i++ {
					reportCollector.Commit()
					time.Sleep(100 * time.Millisecond)
				}

				{
					reportCollector.mu.RLock()
					defer reportCollector.mu.RUnlock()

					for _, executor := range reportCollector.executors {
						executor.cmmu.RLock()
						defer executor.cmmu.RUnlock()

						reportedVer, ok := executor.getReportedVersion(sealedLSID)
						So(ok && reportedVer == knownVer, ShouldBeFalse)
					}
				}

				Convey("When ReportCollector Unseal", func() {
					reportCollector.Unseal(sealedLSID, knownVer)
					cc.unseal(sealedLSID)

					Convey("Then it should commit", func() {
						gls = cc.newDummyCommitResults(knownVer+1, glsn, nrStorage)
						mr.appendGLS(gls)
						knownVer = gls.Version
						glsn += types.GLSN(len(gls.CommitResults))

						a.m.Range(func(k, v interface{}) bool {
							cli := v.(*DummyStorageNodeClient)
							So(testutil.CompareWaitN(10, func() bool {
								reportCollector.Commit()

								return cli.getKnownVersion(0) == knownVer
							}), ShouldBeTrue)
							return true
						})
					})
				})
			})
		})
	})
}

type dummyCommitContext struct {
	committedLLSNBeginOffset []types.LLSN
	sealedLSID               map[types.LogStreamID]struct{}
}

func newDummyCommitContext() *dummyCommitContext {
	cc := &dummyCommitContext{}
	cc.sealedLSID = make(map[types.LogStreamID]struct{})

	return cc
}

func (cc *dummyCommitContext) seal(lsID types.LogStreamID) {
	cc.sealedLSID[lsID] = struct{}{}
}

func (cc *dummyCommitContext) unseal(lsID types.LogStreamID) {
	delete(cc.sealedLSID, lsID)
}

func (cc *dummyCommitContext) sealed(lsID types.LogStreamID) bool {
	_, ok := cc.sealedLSID[lsID]
	return ok
}

func (cc *dummyCommitContext) newDummyCommitResults(ver types.Version, baseGLSN types.GLSN, nrLogStream int) *mrpb.LogStreamCommitResults {
	cr := &mrpb.LogStreamCommitResults{
		Version: ver,
	}

	for i := len(cc.committedLLSNBeginOffset); i < nrLogStream; i++ {
		cc.committedLLSNBeginOffset = append(cc.committedLLSNBeginOffset, types.MinLLSN)
	}

	glsn := baseGLSN
	for i := 0; i < nrLogStream; i++ {
		numUncommitLen := 0
		lsID := types.MinLogStreamID + types.LogStreamID(i)
		if !cc.sealed(lsID) {
			numUncommitLen = 1
		}

		r := snpb.LogStreamCommitResult{
			LogStreamID:         lsID,
			CommittedGLSNOffset: glsn,
			CommittedLLSNOffset: cc.committedLLSNBeginOffset[i],
			CommittedGLSNLength: uint64(numUncommitLen),
		}
		cc.committedLLSNBeginOffset[i] += types.LLSN(numUncommitLen)
		glsn += types.GLSN(numUncommitLen)

		cr.CommitResults = append(cr.CommitResults, r)
	}

	return cr
}

func TestCommit(t *testing.T) {
	Convey("Given ReportCollector", t, func() {
		nrStorage := 5
		nrLogStream := nrStorage
		knownVer := types.InvalidVersion
		glsn := types.MinGLSN
		topicID := types.TopicID(0)

		a := NewDummyStorageNodeClientFactory(1, false)
		mr := NewDummyMetadataRepository(a)
		cc := newDummyCommitContext()

		logger, _ := zap.NewDevelopment()
		reportCollector := NewReportCollector(mr, DefaultRPCTimeout, newNopTelmetryStub(), logger)
		reportCollector.Run() //nolint:errcheck,revive // TODO:: Handle an error returned.
		Reset(func() {
			reportCollector.Close()
		})

		for i := 0; i < nrStorage; i++ {
			snID := types.MinStorageNodeID + types.StorageNodeID(i)
			sn := &varlogpb.StorageNodeDescriptor{
				StorageNode: varlogpb.StorageNode{
					StorageNodeID: snID,
				},
			}

			err := reportCollector.RegisterStorageNode(sn)
			if err != nil {
				t.Fatal(err)
			}

			So(testutil.CompareWaitN(1, func() bool {
				return a.lookupClient(sn.StorageNodeID) != nil
			}), ShouldBeTrue)
		}

		for i := 0; i < nrLogStream; i++ {
			snID := types.MinStorageNodeID + types.StorageNodeID(i%nrStorage)
			lsID := types.MinLogStreamID + types.LogStreamID(i)
			err := reportCollector.RegisterLogStream(topicID, snID, lsID, types.InvalidVersion, varlogpb.LogStreamStatusRunning)
			if err != nil {
				t.Fatal(err)
			}
		}

		Convey("ReportCollector should broadcast commit result to registered storage node", func() {
			gls := cc.newDummyCommitResults(knownVer+1, glsn, nrStorage)
			mr.appendGLS(gls)
			knownVer = gls.Version
			glsn += types.GLSN(len(gls.CommitResults))

			reportCollector.Commit()

			a.m.Range(func(k, v interface{}) bool {
				cli := v.(*DummyStorageNodeClient)
				So(testutil.CompareWaitN(10, func() bool {
					reportCollector.Commit()

					return cli.getKnownVersion(0) == knownVer
				}), ShouldBeTrue)
				return true
			})

			Convey("ReportCollector should send ordered commit result to registered storage node", func() {
				gls := cc.newDummyCommitResults(knownVer+1, glsn, nrStorage)
				mr.appendGLS(gls)
				knownVer = gls.Version
				glsn += types.GLSN(len(gls.CommitResults))

				gls = cc.newDummyCommitResults(knownVer+1, glsn, nrStorage)
				mr.appendGLS(gls)
				knownVer = gls.Version
				glsn += types.GLSN(len(gls.CommitResults))

				reportCollector.Commit()

				a.m.Range(func(k, v interface{}) bool {
					cli := v.(*DummyStorageNodeClient)
					So(testutil.CompareWaitN(10, func() bool {
						reportCollector.Commit()

						return cli.getKnownVersion(0) == knownVer
					}), ShouldBeTrue)
					return true
				})

				trimVer := types.MaxVersion
				reportCollector.mu.RLock()
				for _, executor := range reportCollector.executors {
					reports, _ := executor.reportCtx.getReport()
					for _, report := range reports {
						if !report.Version.Invalid() && report.Version < trimVer {
							trimVer = report.Version
						}
					}
				}
				reportCollector.mu.RUnlock()

				// wait for prev catchup job to finish
				time.Sleep(time.Second)
				mr.trimGLS(trimVer)

				Convey("ReportCollector should send proper commit against new StorageNode", func() {
					snID := types.MinStorageNodeID + types.StorageNodeID(nrStorage)
					lsID := types.MinLogStreamID + types.LogStreamID(nrLogStream)
					sn := &varlogpb.StorageNodeDescriptor{
						StorageNode: varlogpb.StorageNode{
							StorageNodeID: snID,
						},
					}

					err := reportCollector.RegisterStorageNode(sn)
					So(err, ShouldBeNil)

					nrStorage += 1

					err = reportCollector.RegisterLogStream(topicID, sn.StorageNodeID, lsID, knownVer, varlogpb.LogStreamStatusRunning)
					So(err, ShouldBeNil)

					nrLogStream += 1

					gls := cc.newDummyCommitResults(knownVer+1, glsn, nrStorage)
					mr.appendGLS(gls)
					knownVer = gls.Version
					glsn += types.GLSN(len(gls.CommitResults))

					So(testutil.CompareWaitN(10, func() bool {
						nrCli := 0
						a.m.Range(func(k, v interface{}) bool {
							cli := v.(*DummyStorageNodeClient)
							So(testutil.CompareWaitN(10, func() bool {
								reportCollector.Commit()

								return cli.getKnownVersion(0) == knownVer
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
		knownVer := types.InvalidVersion
		glsn := types.MinGLSN
		topicID := types.TopicID(0)

		a := NewDummyStorageNodeClientFactory(1, false)
		mr := NewDummyMetadataRepository(a)
		cc := newDummyCommitContext()

		logger, _ := zap.NewDevelopment()
		reportCollector := NewReportCollector(mr, time.Second, newNopTelmetryStub(), logger)
		reportCollector.Run() //nolint:errcheck,revive // TODO:: Handle an error returned.
		Reset(func() {
			reportCollector.Close()
		})

		sn := &varlogpb.StorageNodeDescriptor{
			StorageNode: varlogpb.StorageNode{
				StorageNodeID: types.MinStorageNodeID,
			},
		}

		err := reportCollector.RegisterStorageNode(sn)
		if err != nil {
			t.Fatal(err)
		}

		So(testutil.CompareWaitN(1, func() bool {
			return a.lookupClient(sn.StorageNodeID) != nil
		}), ShouldBeTrue)

		err = reportCollector.RegisterLogStream(topicID, types.MinStorageNodeID, types.MinLogStreamID, types.InvalidVersion, varlogpb.LogStreamStatusRunning)
		if err != nil {
			t.Fatal(err)
		}

		reportCollector.mu.RLock()
		executor := reportCollector.lookupExecutor(sn.StorageNodeID)
		ok := executor != nil
		reportCollector.mu.RUnlock()

		So(ok, ShouldBeTrue)

		// check report
		So(testutil.CompareWaitN(10, func() bool {
			_, ok := executor.reportCtx.getReport()
			return ok
		}), ShouldBeTrue)

		dummySN := a.lookupClient(sn.StorageNodeID)

		Convey("disable report to catchup using old hwm", func() {
			gls := cc.newDummyCommitResults(knownVer+1, glsn, 1)
			mr.appendGLS(gls)
			knownVer = gls.Version
			glsn += types.GLSN(len(gls.CommitResults))

			reportCollector.Commit()

			So(testutil.CompareWaitN(10, func() bool {
				report, ok := executor.reportCtx.getReport()
				return ok && report[0].Version == knownVer
			}), ShouldBeTrue)
			report, _ := executor.reportCtx.getReport()
			reportedVer := report[0].Version

			dummySN.DisableReport()

			time.Sleep(10 * time.Millisecond)

			for i := 0; i < 10; i++ {
				dummySN.increaseUncommitted(0)
				gls = cc.newDummyCommitResults(knownVer+1, glsn, 1)
				mr.appendGLS(gls)
				knownVer = gls.Version
				glsn += types.GLSN(len(gls.CommitResults))
			}
			reportCollector.Commit()

			So(testutil.CompareWaitN(10, func() bool {
				return dummySN.getKnownVersion(0) == knownVer
			}), ShouldBeTrue)

			time.Sleep(10 * time.Millisecond)
			report, _ = executor.reportCtx.getReport()
			So(report[0].Version, ShouldEqual, reportedVer)

			Convey("set commit delay & enable report to trim during catchup", func() {
				dummySN.SetCommitDelay(100 * time.Millisecond)
				reportCollector.Commit()

				time.Sleep(10 * time.Millisecond)
				dummySN.EnableReport()

				So(testutil.CompareWaitN(10, func() bool {
					reports, ok := executor.reportCtx.getReport()
					return ok && reports[0].Version == knownVer
				}), ShouldBeTrue)

				mr.trimGLS(knownVer)

				gls = cc.newDummyCommitResults(knownVer+1, glsn, 1)
				mr.appendGLS(gls)
				knownVer = gls.Version
				glsn += types.GLSN(len(gls.CommitResults))

				Convey("then it should catchup", func() {
					reportCollector.Commit()

					So(testutil.CompareWaitN(10, func() bool {
						reports, ok := executor.reportCtx.getReport()
						return ok && reports[0].Version == knownVer
					}), ShouldBeTrue)
				})
			})
		})
	})
}

func TestRPCFail(t *testing.T) {
	Convey("Given ReportCollector", t, func(ctx C) {
		// knownVer := types.InvalidVersion

		clientFac := NewDummyStorageNodeClientFactory(1, false)
		mr := NewDummyMetadataRepository(clientFac)

		logger, _ := zap.NewDevelopment()
		reportCollector := NewReportCollector(mr, DefaultRPCTimeout, newNopTelmetryStub(), logger)
		reportCollector.Run() //nolint:errcheck,revive // TODO:: Handle an error returned.
		Reset(func() {
			reportCollector.Close()
		})

		sn := &varlogpb.StorageNodeDescriptor{
			StorageNode: varlogpb.StorageNode{
				StorageNodeID: types.MinStorageNodeID,
			},
		}

		err := reportCollector.RegisterStorageNode(sn)
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
		clientFac := NewDummyStorageNodeClientFactory(1, false)
		mr := NewDummyMetadataRepository(clientFac)

		sn := &varlogpb.StorageNodeDescriptor{
			StorageNode: varlogpb.StorageNode{
				StorageNodeID: types.MinStorageNodeID,
			},
		}

		logger, _ := zap.NewDevelopment()

		executor := &reportCollectExecutor{
			storageNodeID: sn.StorageNodeID,
			helper:        mr,
			snConnector:   storageNodeConnector{sn: sn},
			reportCtx:     &reportContext{},
			runner:        runner.New("excutor", logger),
			logger:        logger,
		}

		cli := make([]reportcommitter.Client, 2)
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

			_, err = cli[0].GetReport()
			So(err, ShouldBeNil)

			_, err = cli[1].GetReport()
			So(err, ShouldNotBeNil)

			Convey("Then closeClient(cli[1]) should not closed the client", func(ctx C) {
				executor.closeClient(cli[1])

				_, err = cli[0].GetReport()
				So(err, ShouldBeNil)

				cli[1], err = executor.getClient(context.TODO())
				So(err, ShouldBeNil)
				So(cli[0], ShouldEqual, cli[1])

				_, err = cli[0].GetReport()
				So(err, ShouldBeNil)
			})
		})
	})
}

type testReportContextPtr struct {
	report []snpb.LogStreamUncommitReport
	mu     sync.RWMutex
}

func (rc *testReportContextPtr) saveReport(report []snpb.LogStreamUncommitReport) {
	rc.mu.Lock()
	defer rc.mu.Unlock()

	rc.report = report
}

func (rc *testReportContextPtr) getReport() []snpb.LogStreamUncommitReport {
	rc.mu.RLock()
	defer rc.mu.RUnlock()

	return rc.report
}

func BenchmarkSwapReport(b *testing.B) {
	rcPtr := &testReportContextPtr{}
	rc := &reportContext{}
	report := []snpb.LogStreamUncommitReport{
		{
			LogStreamID:           2,
			UncommittedLLSNOffset: 3,
			UncommittedLLSNLength: 4,
			Version:               5,
			HighWatermark:         6,
		},
	}

	b.ResetTimer()
	b.Run("getAndSavePtr", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			old := rcPtr.getReport()
			rcPtr.saveReport(report)
			_ = old
		}
	})

	b.ResetTimer()
	b.Run("getAndSave", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			old, ok := rc.getReport()
			rc.saveReport(report)
			_ = old
			_ = ok
		}
	})

	b.ResetTimer()
	b.Run("swap", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			old, ok := rc.swapReport(report)
			_ = old
			_ = ok
		}
	})
}
