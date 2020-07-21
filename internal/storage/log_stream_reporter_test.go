package storage

import (
	"context"
	"sync"
	"testing"
	"time"

	gomock "github.com/golang/mock/gomock"
	. "github.com/smartystreets/goconvey/convey"
	"github.com/kakao/varlog/pkg/varlog/types"
)

func TestLogStreamReporterRunAndClose(t *testing.T) {
	Convey("LogStreamReporter should be run and closed", t, func() {
		lsr := NewLogStreamReporter(types.StorageNodeID(0))
		So(func() { lsr.Run(context.TODO()) }, ShouldNotPanic)
		So(func() { lsr.Close() }, ShouldNotPanic)
	})
}

func TestLogStreamReporterRegister(t *testing.T) {
	Convey("LogStreamReporter", t, func() {
		lsr := NewLogStreamReporter(types.StorageNodeID(0))

		Convey("it should not register nil executor", func() {
			err := lsr.RegisterLogStreamExecutor(nil)
			So(err, ShouldNotBeNil)
		})

		Convey("it should not register already existing LSE", func() {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			var err error
			lse := NewMockLogStreamExecutor(ctrl)
			lse.EXPECT().LogStreamID().Return(types.LogStreamID(1)).AnyTimes()
			err = lsr.RegisterLogStreamExecutor(lse)
			So(err, ShouldBeNil)
			err = lsr.RegisterLogStreamExecutor(lse)
			So(err, ShouldNotBeNil)
		})
	})
}

func TestLogStreamReporterGetReport(t *testing.T) {
	Convey("LogStreamReporter", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		lsr := NewLogStreamReporter(types.StorageNodeID(0)).(*logStreamReporter)
		lse1 := NewMockLogStreamExecutor(ctrl)
		lse1.EXPECT().LogStreamID().Return(types.LogStreamID(1)).AnyTimes()
		lse2 := NewMockLogStreamExecutor(ctrl)
		lse2.EXPECT().LogStreamID().Return(types.LogStreamID(2)).AnyTimes()

		Convey("it should get local stream status by using GetReport", func() {
			lsr.RegisterLogStreamExecutor(lse1)
			report := UncommittedLogStreamStatus{
				LogStreamID:          lse1.LogStreamID(),
				KnownNextGLSN:        types.GLSN(0),
				UncommittedLLSNBegin: types.LLSN(0),
				UncommittedLLSNEnd:   types.LLSN(10),
			}
			lse1.EXPECT().GetReport().Return(report)
			knownNextGLSN, reports := lsr.GetReport()
			So(knownNextGLSN, ShouldEqual, types.GLSN(0))
			So(len(reports), ShouldEqual, 1)
			So(reports[0], ShouldResemble, report)
		})

		Convey("it should take a minimum of knownNextGLSN getting from LSEs", func() {
			lsr.RegisterLogStreamExecutor(lse1)
			lsr.RegisterLogStreamExecutor(lse2)
			lsr.knownNextGLSN.Store(types.GLSN(20))
			report1 := UncommittedLogStreamStatus{
				LogStreamID:          lse1.LogStreamID(),
				KnownNextGLSN:        types.GLSN(10),
				UncommittedLLSNBegin: types.LLSN(0),
				UncommittedLLSNEnd:   types.LLSN(10),
			}
			report2 := UncommittedLogStreamStatus{
				LogStreamID:          lse2.LogStreamID(),
				KnownNextGLSN:        types.GLSN(20),
				UncommittedLLSNBegin: types.LLSN(0),
				UncommittedLLSNEnd:   types.LLSN(10),
			}
			lse1.EXPECT().GetReport().Return(report1)
			lse2.EXPECT().GetReport().Return(report2)
			knownNextGLSN, reports := lsr.GetReport()
			So(knownNextGLSN, ShouldEqual, types.GLSN(10))
			So(len(reports), ShouldEqual, 2)
		})

		Convey("it should ignore knownNextGLSN of new LSE which is zero", func() {
			lsr.RegisterLogStreamExecutor(lse1)
			lsr.RegisterLogStreamExecutor(lse2)
			lsr.knownNextGLSN.Store(types.GLSN(20))
			report1 := UncommittedLogStreamStatus{
				LogStreamID:          lse1.LogStreamID(),
				KnownNextGLSN:        types.GLSN(0),
				UncommittedLLSNBegin: types.LLSN(0),
				UncommittedLLSNEnd:   types.LLSN(10),
			}
			report2 := UncommittedLogStreamStatus{
				LogStreamID:          lse2.LogStreamID(),
				KnownNextGLSN:        types.GLSN(10),
				UncommittedLLSNBegin: types.LLSN(0),
				UncommittedLLSNEnd:   types.LLSN(10),
			}
			lse1.EXPECT().GetReport().Return(report1)
			lse2.EXPECT().GetReport().Return(report2)
			knownNextGLSN, reports := lsr.GetReport()
			So(knownNextGLSN, ShouldEqual, types.GLSN(10))
			So(len(reports), ShouldEqual, 2)
		})
	})
}

func TestLogStreamReporterCommit(t *testing.T) {
	Convey("LogStreamReporter", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		lsr := NewLogStreamReporter(types.StorageNodeID(0)).(*logStreamReporter)
		lse1 := NewMockLogStreamExecutor(ctrl)
		lse1.EXPECT().LogStreamID().Return(types.LogStreamID(1)).AnyTimes()
		lse2 := NewMockLogStreamExecutor(ctrl)
		lse2.EXPECT().LogStreamID().Return(types.LogStreamID(2)).AnyTimes()

		Convey("it should reject a commit result whose prevNextGLSN is not equal to own knownNextGLSN",
			func() {
				lsr.knownNextGLSN.Store(10)
				lsr.Commit(types.GLSN(10), types.GLSN(5), nil)
				So(len(lsr.commitC), ShouldEqual, 0)

				lsr.Commit(types.GLSN(20), types.GLSN(15), nil)
				So(len(lsr.commitC), ShouldEqual, 0)
			},
		)

		Convey("it should reject an empty commit result", func() {
			lsr.knownNextGLSN.Store(10)

			lsr.Commit(types.GLSN(15), types.GLSN(10), nil)
			So(len(lsr.commitC), ShouldEqual, 0)

			lsr.Commit(types.GLSN(15), types.GLSN(10), []CommittedLogStreamStatus{})
			So(len(lsr.commitC), ShouldEqual, 0)
		})

		Convey("it should change knownNextGLSN after call LSE.Commit", func() {
			lsr.RegisterLogStreamExecutor(lse1)
			lsr.RegisterLogStreamExecutor(lse2)

			lsr.Run(context.TODO())
			defer lsr.Close()

			lsr.knownNextGLSN.Store(10)
			oldKnownNextGLSN := lsr.knownNextGLSN.Load()
			var wg sync.WaitGroup
			wg.Add(2)
			lse1.EXPECT().Commit(gomock.Any()).DoAndReturn(func(CommittedLogStreamStatus) error {
				defer wg.Done()
				return nil
			}).AnyTimes()
			lse2.EXPECT().Commit(gomock.Any()).DoAndReturn(func(CommittedLogStreamStatus) error {
				defer wg.Done()
				return nil
			}).AnyTimes()

			lsr.Commit(types.GLSN(20), types.GLSN(10), []CommittedLogStreamStatus{
				{
					LogStreamID:        lse1.LogStreamID(),
					NextGLSN:           types.GLSN(20),
					PrevNextGLSN:       types.GLSN(10),
					CommittedGLSNBegin: types.GLSN(100),
					CommittedGLSNEnd:   types.GLSN(105),
				},
				{
					LogStreamID:        lse2.LogStreamID(),
					NextGLSN:           types.GLSN(20),
					PrevNextGLSN:       types.GLSN(10),
					CommittedGLSNBegin: types.GLSN(105),
					CommittedGLSNEnd:   types.GLSN(110),
				},
			})
			wg.Wait()
			for oldKnownNextGLSN == lsr.knownNextGLSN.Load() {
				time.Sleep(time.Millisecond)
			}
			So(lsr.knownNextGLSN.Load(), ShouldEqual, types.GLSN(20))
		})
	})
}
